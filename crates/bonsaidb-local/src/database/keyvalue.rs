use std::{
    collections::{HashMap, VecDeque},
    convert::Infallible,
    sync::atomic::{AtomicBool, Ordering},
};

use async_trait::async_trait;
use bonsaidb_core::{
    keyvalue::{
        Command, KeyCheck, KeyOperation, KeyStatus, KeyValue, Numeric, Output, Timestamp, Value,
    },
    transaction::{ChangedKey, Changes},
};
use nebari::{
    io::fs::StdFile,
    tree::{KeyEvaluation, Root, Unversioned},
    AbortError, Buffer, CompareAndSwapError, ExecutingTransaction, TransactionTree,
};
use serde::{Deserialize, Serialize};

use crate::{database::Context, jobs::Job, Database, Error};

#[derive(Serialize, Deserialize)]
pub struct Entry {
    pub value: Value,
    pub expiration: Option<Timestamp>,
}

impl Entry {
    pub(crate) async fn restore(
        self,
        namespace: Option<String>,
        key: String,
        database: &Database,
    ) -> Result<(), bonsaidb_core::Error> {
        let task_context = database.data.context.clone();
        tokio::task::spawn_blocking(move || {
            KvTransaction::execute(&task_context, |tx| {
                execute_set_operation(
                    namespace,
                    key,
                    self.value,
                    self.expiration,
                    false,
                    None,
                    false,
                    tx,
                )
            })
        })
        .await
        .unwrap()
        .map(|_| {})
    }
}

#[async_trait]
impl KeyValue for Database {
    async fn execute_key_operation(
        &self,
        op: KeyOperation,
    ) -> Result<Output, bonsaidb_core::Error> {
        let task_context = self.data.context.clone();
        tokio::task::spawn_blocking(move || match op.command {
            Command::Set {
                value,
                expiration,
                keep_existing_expiration,
                check,
                return_previous_value,
            } => KvTransaction::execute(&task_context, |tx| {
                execute_set_operation(
                    op.namespace,
                    op.key,
                    value,
                    expiration,
                    keep_existing_expiration,
                    check,
                    return_previous_value,
                    tx,
                )
            }),
            Command::Get { delete } => {
                execute_get_operation(op.namespace, op.key, delete, &task_context)
            }
            Command::Delete => KvTransaction::execute(&task_context, |tx| {
                execute_delete_operation(op.namespace, op.key, tx)
            }),
            Command::Increment { amount, saturating } => {
                KvTransaction::execute(&task_context, |tx| {
                    execute_increment_operation(op.namespace, op.key, tx, &amount, saturating)
                })
            }
            Command::Decrement { amount, saturating } => {
                KvTransaction::execute(&task_context, |tx| {
                    execute_decrement_operation(op.namespace, op.key, tx, &amount, saturating)
                })
            }
        })
        .await
        .unwrap()
    }
}

impl Database {
    pub(crate) async fn all_key_value_entries(
        &self,
    ) -> Result<HashMap<(Option<String>, String), Entry>, Error> {
        let database = self.clone();
        tokio::task::spawn_blocking(move || {
            // Find all trees that start with <database>.kv.
            let mut all_entries = HashMap::new();
            database
                .roots()
                .tree(Unversioned::tree(KEY_TREE))?
                .scan::<Error, _, _, _, _>(
                    ..,
                    true,
                    |_, _, _| true,
                    |_, _| KeyEvaluation::ReadData,
                    |key, _, entry: Buffer<'static>| {
                        let entry = bincode::deserialize::<Entry>(&entry)
                            .map_err(|err| AbortError::Other(Error::from(err)))?;
                        let full_key = std::str::from_utf8(&key)
                            .map_err(|err| AbortError::Other(Error::from(err)))?;
                        if let Some(split_key) = split_key(full_key) {
                            all_entries.insert(split_key, entry);
                        }

                        Ok(())
                    },
                )?;
            Ok(all_entries)
        })
        .await?
    }
}

pub(crate) const KEY_TREE: &str = "kv";

fn full_key(namespace: Option<&str>, key: &str) -> String {
    let full_length = namespace.map_or_else(|| 0, str::len) + key.len() + 1;
    let mut full_key = String::with_capacity(full_length);
    if let Some(ns) = namespace {
        full_key.push_str(ns);
    }
    full_key.push('\0');
    full_key.push_str(key);
    full_key
}

fn split_key(full_key: &str) -> Option<(Option<String>, String)> {
    if let Some((namespace, key)) = full_key.split_once('\0') {
        let namespace = if namespace.is_empty() {
            None
        } else {
            Some(namespace.to_string())
        };
        Some((namespace, key.to_string()))
    } else {
        None
    }
}

#[allow(clippy::too_many_arguments)]
fn execute_set_operation(
    namespace: Option<String>,
    key: String,
    value: Value,
    expiration: Option<Timestamp>,
    keep_existing_expiration: bool,
    check: Option<KeyCheck>,
    return_previous_value: bool,
    tx: &mut KvTransaction<'_>,
) -> Result<Output, bonsaidb_core::Error> {
    let mut entry = Entry { value, expiration };
    let mut inserted = false;
    let mut updated = false;
    let full_key = full_key(namespace.as_deref(), &key);
    let previous_value = fetch_and_update_no_copy(tx, namespace, key, |existing_value| {
        let should_update = match check {
            Some(KeyCheck::OnlyIfPresent) => existing_value.is_some(),
            Some(KeyCheck::OnlyIfVacant) => existing_value.is_none(),
            None => true,
        };
        if should_update {
            updated = true;
            inserted = existing_value.is_none();
            if keep_existing_expiration && !inserted {
                if let Ok(previous_entry) = bincode::deserialize::<Entry>(&existing_value.unwrap())
                {
                    entry.expiration = previous_entry.expiration;
                }
            }
            let entry_vec = bincode::serialize(&entry).unwrap();
            Some(Buffer::from(entry_vec))
        } else {
            existing_value
        }
    })
    .map_err(Error::from)?;

    if updated {
        tx.context.update_key_expiration(full_key, entry.expiration);
        if return_previous_value {
            if let Some(Ok(entry)) = previous_value.map(|v| bincode::deserialize::<Entry>(&v)) {
                Ok(Output::Value(Some(entry.value)))
            } else {
                Ok(Output::Value(None))
            }
        } else if inserted {
            Ok(Output::Status(KeyStatus::Inserted))
        } else {
            Ok(Output::Status(KeyStatus::Updated))
        }
    } else {
        Ok(Output::Status(KeyStatus::NotChanged))
    }
}

fn execute_get_operation(
    namespace: Option<String>,
    key: String,
    delete: bool,
    db: &Context,
) -> Result<Output, bonsaidb_core::Error> {
    let tree = db
        .roots
        .tree(Unversioned::tree(KEY_TREE))
        .map_err(Error::from)?;
    let full_key = full_key(namespace.as_deref(), &key);
    let entry = if delete {
        let entry = KvTransaction::execute(db, |tx| {
            if let Some(removed_entry) =
                tx.tree().remove(full_key.as_bytes()).map_err(Error::from)?
            {
                tx.push(ChangedKey {
                    namespace,
                    key,
                    deleted: true,
                });
                Ok(Some(removed_entry))
            } else {
                Ok(None)
            }
        })?;
        if entry.is_some() {
            db.update_key_expiration(full_key, None);
        }
        entry
    } else {
        tree.get(full_key.as_bytes()).map_err(Error::from)?
    };

    let entry = entry
        .map(|e| bincode::deserialize::<Entry>(&e))
        .transpose()
        .map_err(Error::from)
        .unwrap()
        .map(|e| e.value);
    Ok(Output::Value(entry))
}

fn execute_delete_operation(
    namespace: Option<String>,
    key: String,
    tx: &mut KvTransaction<'_>,
) -> Result<Output, bonsaidb_core::Error> {
    let full_key = full_key(namespace.as_deref(), &key);
    let value = tx.tree().remove(full_key.as_bytes()).map_err(Error::from)?;
    if value.is_some() {
        tx.push(ChangedKey {
            namespace,
            key,
            deleted: true,
        });
        tx.context.update_key_expiration(full_key, None);

        Ok(Output::Status(KeyStatus::Deleted))
    } else {
        Ok(Output::Status(KeyStatus::NotChanged))
    }
}

fn execute_increment_operation(
    namespace: Option<String>,
    key: String,
    tx: &mut KvTransaction<'_>,
    amount: &Numeric,
    saturating: bool,
) -> Result<Output, bonsaidb_core::Error> {
    execute_numeric_operation(namespace, key, tx, amount, saturating, increment)
}

fn execute_decrement_operation(
    namespace: Option<String>,
    key: String,
    tx: &mut KvTransaction<'_>,
    amount: &Numeric,
    saturating: bool,
) -> Result<Output, bonsaidb_core::Error> {
    execute_numeric_operation(namespace, key, tx, amount, saturating, decrement)
}

fn execute_numeric_operation<F: Fn(&Numeric, &Numeric, bool) -> Numeric>(
    namespace: Option<String>,
    key: String,
    tx: &mut KvTransaction<'_>,
    amount: &Numeric,
    saturating: bool,
    op: F,
) -> Result<Output, bonsaidb_core::Error> {
    let full_key = full_key(namespace.as_deref(), &key);
    let mut current = tx.tree().get(full_key.as_bytes()).map_err(Error::from)?;
    loop {
        let mut entry = current
            .as_ref()
            .map(|current| bincode::deserialize::<Entry>(current))
            .transpose()
            .map_err(Error::from)?
            .unwrap_or(Entry {
                value: Value::Numeric(Numeric::UnsignedInteger(0)),
                expiration: None,
            });

        match entry.value {
            Value::Numeric(existing) => {
                let value = Value::Numeric(op(&existing, amount, saturating));
                entry.value = value.clone();

                let result_bytes = Buffer::from(bincode::serialize(&entry).unwrap());
                match tx.tree().compare_and_swap(
                    full_key.as_bytes(),
                    current.as_ref(),
                    Some(result_bytes),
                ) {
                    Ok(_) => {
                        tx.push(ChangedKey {
                            namespace,
                            key,
                            deleted: false,
                        });
                        return Ok(Output::Value(Some(value)));
                    }
                    Err(CompareAndSwapError::Conflict(cur)) => {
                        current = cur;
                    }
                    Err(CompareAndSwapError::Error(other)) => {
                        // TODO should roots errors be able to be put in core?
                        return Err(bonsaidb_core::Error::Database(other.to_string()));
                    }
                }
            }
            Value::Bytes(_) => {
                return Err(bonsaidb_core::Error::Database(String::from(
                    "type of stored `Value` is not `Numeric`",
                )))
            }
        }
    }
}

fn increment(existing: &Numeric, amount: &Numeric, saturating: bool) -> Numeric {
    match amount {
        Numeric::Integer(amount) => {
            let existing_value = existing.as_i64_lossy(saturating);
            let new_value = if saturating {
                existing_value.saturating_add(*amount)
            } else {
                existing_value.wrapping_add(*amount)
            };
            Numeric::Integer(new_value)
        }
        Numeric::UnsignedInteger(amount) => {
            let existing_value = existing.as_u64_lossy(saturating);
            let new_value = if saturating {
                existing_value.saturating_add(*amount)
            } else {
                existing_value.wrapping_add(*amount)
            };
            Numeric::UnsignedInteger(new_value)
        }
        Numeric::Float(amount) => {
            let existing_value = existing.as_f64_lossy();
            let new_value = existing_value + *amount;
            Numeric::Float(new_value)
        }
    }
}

fn decrement(existing: &Numeric, amount: &Numeric, saturating: bool) -> Numeric {
    match amount {
        Numeric::Integer(amount) => {
            let existing_value = existing.as_i64_lossy(saturating);
            let new_value = if saturating {
                existing_value.saturating_sub(*amount)
            } else {
                existing_value.wrapping_sub(*amount)
            };
            Numeric::Integer(new_value)
        }
        Numeric::UnsignedInteger(amount) => {
            let existing_value = existing.as_u64_lossy(saturating);
            let new_value = if saturating {
                existing_value.saturating_sub(*amount)
            } else {
                existing_value.wrapping_sub(*amount)
            };
            Numeric::UnsignedInteger(new_value)
        }
        Numeric::Float(amount) => {
            let existing_value = existing.as_f64_lossy();
            let new_value = existing_value - *amount;
            Numeric::Float(new_value)
        }
    }
}

fn fetch_and_update_no_copy<F>(
    tx: &mut KvTransaction<'_>,
    namespace: Option<String>,
    key: String,
    mut f: F,
) -> Result<Option<Buffer<'static>>, nebari::Error>
where
    F: FnMut(Option<Buffer<'static>>) -> Option<Buffer<'static>>,
{
    let full_key = full_key(namespace.as_deref(), &key);
    let mut current = tx.tree().get(full_key.as_bytes())?;

    loop {
        let next = f(current.clone());
        match tx
            .tree()
            .compare_and_swap(full_key.as_bytes(), current.as_ref(), next)
        {
            Ok(()) => {
                tx.push(ChangedKey {
                    namespace,
                    key,
                    deleted: false,
                });
                return Ok(current);
            }
            Err(CompareAndSwapError::Conflict(cur)) => {
                current = cur;
            }
            Err(CompareAndSwapError::Error(other)) => return Err(other),
        }
    }
}

#[derive(Debug)]
pub struct ExpirationUpdate {
    pub tree_key: String,
    pub expiration: Option<Timestamp>,
    completion_sender: flume::Sender<()>,
}

impl ExpirationUpdate {
    pub fn new(tree_key: String, expiration: Option<Timestamp>) -> (Self, flume::Receiver<()>) {
        let (completion_sender, completion_receiver) = flume::bounded(1);
        (
            Self {
                tree_key,
                expiration,
                completion_sender,
            },
            completion_receiver,
        )
    }
}

impl Drop for ExpirationUpdate {
    fn drop(&mut self) {
        let _ = self.completion_sender.send(());
    }
}

async fn remove_expired_keys(
    context: &Context,
    now: Timestamp,
    tracked_keys: &mut HashMap<String, Timestamp>,
    expiration_order: &mut VecDeque<String>,
) -> Result<(), Error> {
    let mut keys_to_remove = Vec::new();
    while !expiration_order.is_empty() && tracked_keys.get(&expiration_order[0]).unwrap() <= &now {
        let key = expiration_order.pop_front().unwrap();
        tracked_keys.remove(&key);
        keys_to_remove.push(key);
    }
    let task_context = context.clone();
    tokio::task::spawn_blocking(move || {
        KvTransaction::execute(&task_context, |tx| {
            for full_key in keys_to_remove {
                if let Some((namespace, key)) = split_key(&full_key) {
                    tx.tree().remove(full_key.as_bytes()).map_err(Error::from)?;

                    tx.push(ChangedKey {
                        namespace,
                        key,
                        deleted: true,
                    });
                }
            }
            Ok(())
        })?;

        Result::<(), Error>::Ok(())
    })
    .await
    .unwrap()?;
    Ok(())
}

pub(crate) async fn expiration_task(
    context: Context,
    updates: flume::Receiver<ExpirationUpdate>,
) -> Result<(), Error> {
    // expiring_keys will be maintained such that the soonest expiration is at the front and furthest in the future is at the back
    let mut tracked_keys = HashMap::new();
    let mut expiration_order = VecDeque::new();
    loop {
        let update = if expiration_order.is_empty() {
            match updates.recv_async().await {
                Ok(update) => update,
                Err(_) => break,
            }
        } else {
            // Check to see if we have any remaining time before a key expires
            let timeout = tracked_keys.get(&expiration_order[0]).unwrap();
            let now = Timestamp::now();
            let remaining_time = *timeout - now;
            let received_update = if let Some(remaining_time) = remaining_time {
                // Allow flume to receive updates for the remaining time.
                match tokio::time::timeout(remaining_time, updates.recv_async()).await {
                    Ok(Ok(update)) => Some(update),
                    Ok(Err(flume::RecvError::Disconnected)) => break,
                    Err(_elapsed) => None,
                }
            } else {
                updates.try_recv().ok()
            };

            // If we've received an update, we bubble it up to process
            if let Some(update) = received_update {
                update
            } else {
                // Reaching this block means that we didn't receive an update to
                // process, and we have at least one key that is ready to be
                // removed.
                remove_expired_keys(&context, now, &mut tracked_keys, &mut expiration_order)
                    .await?;
                continue;
            }
        };

        if let Some(expiration) = update.expiration {
            let key = if tracked_keys.contains_key(&update.tree_key) {
                // Update the existing entry.
                let existing_entry_index = expiration_order
                    .iter()
                    .enumerate()
                    .find_map(|(index, key)| {
                        if &update.tree_key == key {
                            Some(index)
                        } else {
                            None
                        }
                    })
                    .unwrap();
                expiration_order.remove(existing_entry_index).unwrap()
            } else {
                update.tree_key.clone()
            };

            // Insert the key into the expiration_order queue
            let mut insert_at = None;
            for (index, expiring_key) in expiration_order.iter().enumerate() {
                if tracked_keys.get(expiring_key).unwrap() > &expiration {
                    insert_at = Some(index);
                    break;
                }
            }
            if let Some(insert_at) = insert_at {
                expiration_order.insert(insert_at, key.clone());
            } else {
                expiration_order.push_back(key.clone());
            }
            tracked_keys.insert(key, expiration);
        } else if tracked_keys.remove(&update.tree_key).is_some() {
            let index = expiration_order
                .iter()
                .enumerate()
                .find_map(|(index, key)| {
                    if &update.tree_key == key {
                        Some(index)
                    } else {
                        None
                    }
                })
                .unwrap();
            expiration_order.remove(index);
        }
    }

    Ok(())
}

struct KvTransaction<'context> {
    context: &'context Context,
    transaction: ExecutingTransaction<StdFile>,
    changed_keys: Vec<ChangedKey>,
}

impl<'context> KvTransaction<'context> {
    pub fn execute<T, F: FnOnce(&mut Self) -> Result<T, bonsaidb_core::Error>>(
        context: &'context Context,
        tx_body: F,
    ) -> Result<T, bonsaidb_core::Error> {
        let transaction = context
            .roots
            .transaction(&[Unversioned::tree(KEY_TREE)])
            .map_err(Error::from)?;
        let mut tx = Self {
            context,
            transaction,
            changed_keys: Vec::new(),
        };

        match tx_body(&mut tx) {
            Ok(result) => {
                let Self {
                    mut transaction,
                    changed_keys,
                    ..
                } = tx;
                if !changed_keys.is_empty() {
                    transaction
                        .entry_mut()
                        .set_data(pot::to_vec(&Changes::Keys(changed_keys))?)
                        .map_err(Error::from)?;
                    transaction.commit().map_err(Error::from)?;
                }
                Ok(result)
            }
            Err(err) => Err(bonsaidb_core::Error::from(Error::from(err))),
        }
    }

    pub fn push(&mut self, key: ChangedKey) {
        self.changed_keys.push(key);
    }

    pub fn tree(&mut self) -> &mut TransactionTree<Unversioned, StdFile> {
        self.transaction.tree(0).unwrap()
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use bonsaidb_core::test_util::{TestDirectory, TimingTest};
    use futures::Future;
    use nebari::io::fs::StdFile;

    use super::*;
    use crate::database::Context;

    async fn run_test<
        F: FnOnce(Context, nebari::Roots<StdFile>) -> R + Send,
        R: Future<Output = anyhow::Result<()>> + Send,
    >(
        name: &str,
        test_contents: F,
    ) -> anyhow::Result<()> {
        let dir = TestDirectory::new(name);
        let sled = nebari::Config::new(&dir).open()?;

        let context = Context::new(sled.clone());

        test_contents(context, sled).await?;

        Ok(())
    }

    #[tokio::test]
    async fn basic_expiration() -> anyhow::Result<()> {
        run_test("kv-basic-expiration", |sender, sled| async move {
            loop {
                sled.delete_tree(KEY_TREE)?;
                let tree = sled.tree(Unversioned::tree(KEY_TREE))?;
                tree.set(b"atree\0akey", b"somevalue")?;
                let timing = TimingTest::new(Duration::from_millis(100));
                sender
                    .update_key_expiration_async(
                        full_key(Some("atree"), "akey"),
                        Some(Timestamp::now() + Duration::from_millis(100)),
                    )
                    .await;
                if !timing.wait_until(Duration::from_secs(1)).await {
                    println!("basic_expiration restarting due to timing discrepency");
                    continue;
                }
                assert!(tree.get(b"akey")?.is_none());
                break;
            }

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn updating_expiration() -> anyhow::Result<()> {
        run_test("kv-updating-expiration", |sender, sled| async move {
            loop {
                sled.delete_tree(KEY_TREE)?;
                let tree = sled.tree(Unversioned::tree(KEY_TREE))?;
                tree.set(b"atree\0akey", b"somevalue")?;
                let timing = TimingTest::new(Duration::from_millis(100));
                sender
                    .update_key_expiration_async(
                        full_key(Some("atree"), "akey"),
                        Some(Timestamp::now() + Duration::from_millis(100)),
                    )
                    .await;
                sender
                    .update_key_expiration_async(
                        full_key(Some("atree"), "akey"),
                        Some(Timestamp::now() + Duration::from_secs(1)),
                    )
                    .await;
                if timing.elapsed() > Duration::from_millis(100)
                    || !timing.wait_until(Duration::from_millis(500)).await
                {
                    continue;
                }
                assert!(tree.get(b"atree\0akey")?.is_some());

                timing.wait_until(Duration::from_secs_f32(1.5)).await;
                assert_eq!(tree.get(b"atree\0akey")?, None);
                break;
            }

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn multiple_keys_expiration() -> anyhow::Result<()> {
        run_test("kv-multiple-keys-expiration", |sender, sled| async move {
            loop {
                sled.delete_tree(KEY_TREE)?;
                let tree = sled.tree(Unversioned::tree(KEY_TREE))?;
                tree.set(b"atree\0akey", b"somevalue")?;
                tree.set(b"atree\0bkey", b"somevalue")?;

                let timing = TimingTest::new(Duration::from_millis(100));
                sender
                    .update_key_expiration_async(
                        full_key(Some("atree"), "akey"),
                        Some(Timestamp::now() + Duration::from_millis(100)),
                    )
                    .await;
                sender
                    .update_key_expiration_async(
                        full_key(Some("atree"), "bkey"),
                        Some(Timestamp::now() + Duration::from_secs(1)),
                    )
                    .await;

                if !timing.wait_until(Duration::from_millis(200)).await {
                    continue;
                }

                assert!(tree.get(b"atree\0akey")?.is_none());
                assert!(tree.get(b"atree\0bkey")?.is_some());
                timing.wait_until(Duration::from_millis(1100)).await;
                assert!(tree.get(b"atree\0bkey")?.is_none());

                break;
            }

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn clearing_expiration() -> anyhow::Result<()> {
        run_test("kv-clearing-expiration", |sender, sled| async move {
            loop {
                sled.delete_tree(KEY_TREE)?;
                let tree = sled.tree(Unversioned::tree(KEY_TREE))?;
                tree.set(b"atree\0akey", b"somevalue")?;
                let timing = TimingTest::new(Duration::from_millis(100));
                sender
                    .update_key_expiration_async(
                        full_key(Some("atree"), "akey"),
                        Some(Timestamp::now() + Duration::from_millis(100)),
                    )
                    .await;
                sender
                    .update_key_expiration_async(full_key(Some("atree"), "akey"), None)
                    .await;
                if timing.elapsed() > Duration::from_millis(100) {
                    // Restart, took too long.
                    continue;
                }
                timing.wait_until(Duration::from_millis(150)).await;
                assert!(tree.get(b"atree\0akey")?.is_some());
                break;
            }

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn out_of_order_expiration() -> anyhow::Result<()> {
        run_test("kv-out-of-order-expiration", |sender, sled| async move {
            let tree = sled.tree(Unversioned::tree(KEY_TREE))?;
            tree.set(b"atree\0akey", b"somevalue")?;
            tree.set(b"atree\0bkey", b"somevalue")?;
            tree.set(b"atree\0ckey", b"somevalue")?;
            sender
                .update_key_expiration_async(
                    full_key(Some("atree"), "akey"),
                    Some(Timestamp::now() + Duration::from_secs(3)),
                )
                .await;
            sender
                .update_key_expiration_async(
                    full_key(Some("atree"), "ckey"),
                    Some(Timestamp::now() + Duration::from_secs(1)),
                )
                .await;
            sender
                .update_key_expiration_async(
                    full_key(Some("atree"), "bkey"),
                    Some(Timestamp::now() + Duration::from_secs(2)),
                )
                .await;
            tokio::time::sleep(Duration::from_millis(1200)).await;
            assert!(tree.get(b"atree\0akey")?.is_some());
            assert!(tree.get(b"atree\0bkey")?.is_some());
            assert!(tree.get(b"atree\0ckey")?.is_none());
            tokio::time::sleep(Duration::from_secs(1)).await;
            assert!(tree.get(b"atree\0akey")?.is_some());
            assert!(tree.get(b"atree\0bkey")?.is_none());
            tokio::time::sleep(Duration::from_secs(1)).await;
            assert!(tree.get(b"atree\0akey")?.is_none());

            Ok(())
        })
        .await
    }
}

#[derive(Debug)]
pub struct ExpirationLoader {
    pub database: Database,
}

#[async_trait]
impl Job for ExpirationLoader {
    type Output = ();
    type Error = Error;

    #[cfg_attr(feature = "tracing", tracing::instrument)]
    async fn execute(&mut self) -> Result<Self::Output, Self::Error> {
        let database = self.database.clone();
        let (sender, receiver) = flume::unbounded();

        tokio::task::spawn_blocking(move || {
            // Find all trees that start with <database>.kv.
            let keep_scanning = AtomicBool::new(true);
            database
                .roots()
                .tree(Unversioned::tree(KEY_TREE))?
                .scan::<Infallible, _, _, _, _>(
                    ..,
                    true,
                    |_, _, _| true,
                    |_, _| {
                        if keep_scanning.load(Ordering::SeqCst) {
                            KeyEvaluation::ReadData
                        } else {
                            KeyEvaluation::Stop
                        }
                    },
                    |key, _, entry: Buffer<'static>| {
                        if let Ok(entry) = bincode::deserialize::<Entry>(&entry) {
                            if entry.expiration.is_some()
                                && sender.send((key, entry.expiration)).is_err()
                            {
                                keep_scanning.store(false, Ordering::SeqCst);
                            }
                        }

                        Ok(())
                    },
                )?;

            Result::<(), Error>::Ok(())
        });

        while let Ok((key, expiration)) = receiver.recv_async().await {
            self.database
                .update_key_expiration_async(String::from_utf8(key.to_vec())?, expiration)
                .await;
        }

        Ok(())
    }
}
