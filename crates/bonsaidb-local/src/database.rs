use std::{
    borrow::{Borrow, Cow},
    collections::{BTreeMap, HashMap, HashSet},
    convert::Infallible,
    ops::{self, Deref},
    sync::Arc,
    u8,
};

use async_lock::Mutex;
use async_trait::async_trait;
#[cfg(any(feature = "encryption", feature = "compression"))]
use bonsaidb_core::document::KeyId;
use bonsaidb_core::{
    arc_bytes::{
        serde::{Bytes, CowBytes},
        ArcBytes,
    },
    connection::{self, AccessPolicy, Connection, QueryKey, Range, Sort, StorageConnection},
    document::{AnyDocumentId, BorrowedDocument, DocumentId, Header, OwnedDocument, Revision},
    key::Key,
    keyvalue::{KeyOperation, Output, Timestamp},
    limits::{LIST_TRANSACTIONS_DEFAULT_RESULT_COUNT, LIST_TRANSACTIONS_MAX_RESULTS},
    permissions::Permissions,
    schema::{
        self,
        view::{
            self,
            map::{MappedDocuments, MappedSerializedValue},
        },
        Collection, CollectionName, Map, MappedValue, Schema, Schematic, ViewName,
    },
    transaction::{
        self, ChangedDocument, Changes, Command, DocumentChanges, Operation, OperationResult,
        Transaction,
    },
};
use bonsaidb_utils::fast_async_lock;
use itertools::Itertools;
use nebari::{
    io::any::AnyFile,
    tree::{
        AnyTreeRoot, BorrowByteRange, BorrowedRange, CompareSwap, KeyEvaluation, Root, TreeRoot,
        Unversioned, Versioned,
    },
    AbortError, ExecutingTransaction, Roots, Tree,
};
use serde::{Deserialize, Serialize};
use tokio::sync::watch;

#[cfg(feature = "encryption")]
use crate::storage::TreeVault;
use crate::{
    config::{Builder, KeyValuePersistence, StorageConfiguration},
    database::keyvalue::BackgroundWorkerProcessTarget,
    error::Error,
    open_trees::OpenTrees,
    views::{
        mapper::{self},
        view_document_map_tree_name, view_entries_tree_name, view_invalidated_docs_tree_name,
        ViewEntry,
    },
    Storage,
};

pub mod keyvalue;

pub(crate) mod compat;
pub mod pubsub;

/// A local, file-based database.
///
/// ## Using `Database` to create a single database
///
/// `Database`provides an easy mechanism to open and access a single database:
///
/// ```rust
/// // `bonsaidb_core` is re-exported to `bonsaidb::core` or `bonsaidb_local::core`.
/// use bonsaidb_core::schema::Collection;
/// // `bonsaidb_local` is re-exported to `bonsaidb::local` if using the omnibus crate.
/// use bonsaidb_local::{
///     config::{Builder, StorageConfiguration},
///     Database,
/// };
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Debug, Serialize, Deserialize, Collection)]
/// #[collection(name = "blog-posts")]
/// # #[collection(core = bonsaidb_core)]
/// struct BlogPost {
///     pub title: String,
///     pub contents: String,
/// }
///
/// # async fn test_fn() -> Result<(), bonsaidb_core::Error> {
/// let db = Database::open::<BlogPost>(StorageConfiguration::new("my-db.bonsaidb")).await?;
/// #     Ok(())
/// # }
/// ```
///
/// Under the hood, this initializes a [`Storage`] instance pointing at
/// "./my-db.bonsaidb". It then returns (or creates) a database named "default"
/// with the schema `BlogPost`.
///
/// In this example, `BlogPost` implements the [`Collection`] trait, and all
/// collections can be used as a [`Schema`].
#[derive(Debug)]
pub struct Database {
    pub(crate) data: Arc<Data>,
}

#[derive(Debug)]
pub struct Data {
    pub name: Arc<Cow<'static, str>>,
    context: Context,
    pub(crate) storage: Storage,
    pub(crate) schema: Arc<Schematic>,
    #[allow(dead_code)] // This code was previously used, it works, but is currently unused.
    pub(crate) effective_permissions: Option<Permissions>,
}
impl Clone for Database {
    fn clone(&self) -> Self {
        Self {
            data: self.data.clone(),
        }
    }
}

impl Database {
    /// Opens a local file as a bonsaidb.
    pub(crate) async fn new<DB: Schema, S: Into<Cow<'static, str>> + Send>(
        name: S,
        context: Context,
        storage: Storage,
    ) -> Result<Self, Error> {
        let name = name.into();
        let schema = Arc::new(DB::schematic()?);
        let db = Self {
            data: Arc::new(Data {
                name: Arc::new(name),
                context,
                storage: storage.clone(),
                schema,
                effective_permissions: None,
            }),
        };

        if db.data.storage.check_view_integrity_on_database_open() {
            for view in db.data.schema.views() {
                db.data
                    .storage
                    .tasks()
                    .spawn_integrity_check(view, &db)
                    .await?;
            }
        }

        storage.tasks().spawn_key_value_expiration_loader(&db).await;

        Ok(db)
    }

    /// Returns a clone with `effective_permissions`. Replaces any previously applied permissions.
    ///
    /// # Unstable
    ///
    /// See [this issue](https://github.com/khonsulabs/bonsaidb/issues/68).
    #[doc(hidden)]
    #[must_use]
    pub fn with_effective_permissions(&self, effective_permissions: Permissions) -> Self {
        Self {
            data: Arc::new(Data {
                name: self.data.name.clone(),
                context: self.data.context.clone(),
                storage: self.data.storage.clone(),
                schema: self.data.schema.clone(),
                effective_permissions: Some(effective_permissions),
            }),
        }
    }

    /// Returns the name of the database.
    #[must_use]
    pub fn name(&self) -> &str {
        self.data.name.as_ref()
    }

    /// Creates a `Storage` with a single-database named "default" with its data stored at `path`.
    pub async fn open<DB: Schema>(configuration: StorageConfiguration) -> Result<Self, Error> {
        let storage = Storage::open(configuration.with_schema::<DB>()?).await?;

        storage.create_database::<DB>("default", true).await?;

        Ok(storage.database::<DB>("default").await?)
    }

    /// Returns the [`Storage`] that this database belongs to.
    #[must_use]
    pub fn storage(&self) -> &'_ Storage {
        &self.data.storage
    }

    /// Returns the [`Schematic`] for the schema for this database.
    #[must_use]
    pub fn schematic(&self) -> &'_ Schematic {
        &self.data.schema
    }

    pub(crate) fn roots(&self) -> &'_ nebari::Roots<AnyFile> {
        &self.data.context.roots
    }

    async fn for_each_in_view<
        F: FnMut(ViewEntry) -> Result<(), bonsaidb_core::Error> + Send + Sync,
    >(
        &self,
        view: &dyn view::Serialized,
        key: Option<QueryKey<Bytes>>,
        order: Sort,
        limit: Option<usize>,
        access_policy: AccessPolicy,
        mut callback: F,
    ) -> Result<(), bonsaidb_core::Error> {
        if matches!(access_policy, AccessPolicy::UpdateBefore) {
            self.data
                .storage
                .tasks()
                .update_view_if_needed(view, self)
                .await?;
        }

        let view_entries = self
            .roots()
            .tree(self.collection_tree(
                &view.collection(),
                view_entries_tree_name(&view.view_name()),
            )?)
            .map_err(Error::from)?;

        {
            for entry in Self::create_view_iterator(&view_entries, key, order, limit)? {
                callback(entry)?;
            }
        }

        if matches!(access_policy, AccessPolicy::UpdateAfter) {
            let db = self.clone();
            let view_name = view.view_name();
            tokio::task::spawn(async move {
                let view = db
                    .data
                    .schema
                    .view_by_name(&view_name)
                    .expect("query made with view that isn't registered with this database");
                db.data
                    .storage
                    .tasks()
                    .update_view_if_needed(view, &db)
                    .await
            });
        }

        Ok(())
    }

    async fn for_each_view_entry<
        V: schema::View,
        F: FnMut(ViewEntry) -> Result<(), bonsaidb_core::Error> + Send + Sync,
    >(
        &self,
        key: Option<QueryKey<V::Key>>,
        order: Sort,
        limit: Option<usize>,
        access_policy: AccessPolicy,
        callback: F,
    ) -> Result<(), bonsaidb_core::Error> {
        let view = self
            .data
            .schema
            .view::<V>()
            .expect("query made with view that isn't registered with this database");

        self.for_each_in_view(
            view,
            key.map(|key| key.serialized()).transpose()?,
            order,
            limit,
            access_policy,
            callback,
        )
        .await
    }

    #[cfg(feature = "internal-apis")]
    #[doc(hidden)]
    pub async fn internal_get_from_collection_id(
        &self,
        id: DocumentId,
        collection: &CollectionName,
    ) -> Result<Option<OwnedDocument>, bonsaidb_core::Error> {
        self.get_from_collection_id(id, collection).await
    }

    #[cfg(feature = "internal-apis")]
    #[doc(hidden)]
    pub async fn list_from_collection(
        &self,
        ids: Range<DocumentId>,
        order: Sort,
        limit: Option<usize>,
        collection: &CollectionName,
    ) -> Result<Vec<OwnedDocument>, bonsaidb_core::Error> {
        self.list(ids, order, limit, collection).await
    }

    #[cfg(feature = "internal-apis")]
    #[doc(hidden)]
    pub async fn count_from_collection(
        &self,
        ids: Range<DocumentId>,
        collection: &CollectionName,
    ) -> Result<u64, bonsaidb_core::Error> {
        self.count(ids, collection).await
    }

    #[cfg(feature = "internal-apis")]
    #[doc(hidden)]
    pub async fn internal_get_multiple_from_collection_id(
        &self,
        ids: &[DocumentId],
        collection: &CollectionName,
    ) -> Result<Vec<OwnedDocument>, bonsaidb_core::Error> {
        self.get_multiple_from_collection_id(ids, collection).await
    }

    #[cfg(feature = "internal-apis")]
    #[doc(hidden)]
    pub async fn compact_collection_by_name(
        &self,
        collection: CollectionName,
    ) -> Result<(), bonsaidb_core::Error> {
        self.storage()
            .tasks()
            .compact_collection(self.clone(), collection)
            .await?;
        Ok(())
    }

    #[cfg(feature = "internal-apis")]
    #[doc(hidden)]
    pub async fn query_by_name(
        &self,
        view: &ViewName,
        key: Option<QueryKey<Bytes>>,
        order: Sort,
        limit: Option<usize>,
        access_policy: AccessPolicy,
    ) -> Result<Vec<bonsaidb_core::schema::view::map::Serialized>, bonsaidb_core::Error> {
        if let Some(view) = self.schematic().view_by_name(view) {
            let mut results = Vec::new();
            self.for_each_in_view(view, key, order, limit, access_policy, |entry| {
                for mapping in entry.mappings {
                    results.push(bonsaidb_core::schema::view::map::Serialized {
                        source: mapping.source,
                        key: entry.key.clone(),
                        value: mapping.value,
                    });
                }
                Ok(())
            })
            .await?;

            Ok(results)
        } else {
            Err(bonsaidb_core::Error::CollectionNotFound)
        }
    }

    #[cfg(feature = "internal-apis")]
    #[doc(hidden)]
    pub async fn query_by_name_with_docs(
        &self,
        view: &ViewName,
        key: Option<QueryKey<Bytes>>,
        order: Sort,
        limit: Option<usize>,
        access_policy: AccessPolicy,
    ) -> Result<bonsaidb_core::schema::view::map::MappedSerializedDocuments, bonsaidb_core::Error>
    {
        let results = self
            .query_by_name(view, key, order, limit, access_policy)
            .await?;
        let view = self.schematic().view_by_name(view).unwrap(); // query() will fail if it's not present

        let documents = self
            .get_multiple_from_collection_id(
                &results.iter().map(|m| m.source.id).collect::<Vec<_>>(),
                &view.collection(),
            )
            .await?
            .into_iter()
            .map(|doc| (doc.header.id, doc))
            .collect::<BTreeMap<_, _>>();

        Ok(
            bonsaidb_core::schema::view::map::MappedSerializedDocuments {
                mappings: results,
                documents,
            },
        )
    }

    #[cfg(feature = "internal-apis")]
    #[doc(hidden)]
    pub async fn reduce_by_name(
        &self,
        view: &ViewName,
        key: Option<QueryKey<Bytes>>,
        access_policy: AccessPolicy,
    ) -> Result<Vec<u8>, bonsaidb_core::Error> {
        self.reduce_in_view(view, key, access_policy).await
    }

    #[cfg(feature = "internal-apis")]
    #[doc(hidden)]
    pub async fn reduce_grouped_by_name(
        &self,
        view: &ViewName,
        key: Option<QueryKey<Bytes>>,
        access_policy: AccessPolicy,
    ) -> Result<Vec<MappedSerializedValue>, bonsaidb_core::Error> {
        self.grouped_reduce_in_view(view, key, access_policy).await
    }

    #[cfg(feature = "internal-apis")]
    #[doc(hidden)]
    pub async fn delete_docs_by_name(
        &self,
        view: &ViewName,
        key: Option<QueryKey<Bytes>>,
        access_policy: AccessPolicy,
    ) -> Result<u64, bonsaidb_core::Error> {
        let view = self
            .data
            .schema
            .view_by_name(view)
            .ok_or(bonsaidb_core::Error::CollectionNotFound)?;
        let collection = view.collection();
        let mut transaction = Transaction::default();
        self.for_each_in_view(view, key, Sort::Ascending, None, access_policy, |entry| {
            for mapping in entry.mappings {
                transaction.push(Operation::delete(collection.clone(), mapping.source));
            }

            Ok(())
        })
        .await?;

        let results = Connection::apply_transaction(self, transaction).await?;

        Ok(results.len() as u64)
    }

    async fn get_from_collection_id(
        &self,
        id: DocumentId,
        collection: &CollectionName,
    ) -> Result<Option<OwnedDocument>, bonsaidb_core::Error> {
        let task_self = self.clone();
        let collection = collection.clone();
        tokio::task::spawn_blocking(move || {
            let tree = task_self
                .data
                .context
                .roots
                .tree(task_self.collection_tree::<Versioned, _>(
                    &collection,
                    document_tree_name(&collection),
                )?)
                .map_err(Error::from)?;
            if let Some(vec) = tree.get(id.as_ref()).map_err(Error::from)? {
                Ok(Some(deserialize_document(&vec)?.into_owned()))
            } else {
                Ok(None)
            }
        })
        .await
        .unwrap()
    }

    async fn get_multiple_from_collection_id(
        &self,
        ids: &[DocumentId],
        collection: &CollectionName,
    ) -> Result<Vec<OwnedDocument>, bonsaidb_core::Error> {
        let task_self = self.clone();
        let mut ids = ids.to_vec();
        let collection = collection.clone();
        tokio::task::spawn_blocking(move || {
            let tree = task_self
                .data
                .context
                .roots
                .tree(task_self.collection_tree::<Versioned, _>(
                    &collection,
                    document_tree_name(&collection),
                )?)
                .map_err(Error::from)?;
            ids.sort();
            let keys_and_values = tree
                .get_multiple(ids.iter().map(|id| id.as_ref()))
                .map_err(Error::from)?;

            keys_and_values
                .into_iter()
                .map(|(_, value)| deserialize_document(&value).map(BorrowedDocument::into_owned))
                .collect::<Result<Vec<_>, Error>>()
        })
        .await
        .unwrap()
        .map_err(bonsaidb_core::Error::from)
    }

    pub(crate) async fn list(
        &self,
        ids: Range<DocumentId>,
        sort: Sort,
        limit: Option<usize>,
        collection: &CollectionName,
    ) -> Result<Vec<OwnedDocument>, bonsaidb_core::Error> {
        let task_self = self.clone();
        let collection = collection.clone();
        tokio::task::spawn_blocking(move || {
            let tree = task_self
                .data
                .context
                .roots
                .tree(task_self.collection_tree::<Versioned, _>(
                    &collection,
                    document_tree_name(&collection),
                )?)
                .map_err(Error::from)?;
            let mut found_docs = Vec::new();
            let mut keys_read = 0;
            let ids = DocumentIdRange(ids);
            tree.scan(
                &ids.borrow_as_bytes(),
                match sort {
                    Sort::Ascending => true,
                    Sort::Descending => false,
                },
                |_, _, _| true,
                |_, _| {
                    if let Some(limit) = limit {
                        if keys_read >= limit {
                            return KeyEvaluation::Stop;
                        }

                        keys_read += 1;
                    }
                    KeyEvaluation::ReadData
                },
                |_, _, doc| {
                    found_docs.push(
                        deserialize_document(&doc)
                            .map(BorrowedDocument::into_owned)
                            .map_err(AbortError::Other)?,
                    );
                    Ok(())
                },
            )
            .map_err(|err| match err {
                AbortError::Other(err) => err,
                AbortError::Nebari(err) => crate::Error::from(err),
            })
            .unwrap();

            Ok(found_docs)
        })
        .await
        .unwrap()
    }

    pub(crate) async fn count(
        &self,
        ids: Range<DocumentId>,
        collection: &CollectionName,
    ) -> Result<u64, bonsaidb_core::Error> {
        let task_self = self.clone();
        let collection = collection.clone();
        // TODO this should be able to use a reduce operation from Nebari https://github.com/khonsulabs/nebari/issues/23
        tokio::task::spawn_blocking(move || {
            let tree = task_self
                .data
                .context
                .roots
                .tree(task_self.collection_tree::<Versioned, _>(
                    &collection,
                    document_tree_name(&collection),
                )?)
                .map_err(Error::from)?;
            let mut keys_found = 0;
            let ids = DocumentIdRange(ids);
            tree.scan(
                &ids.borrow_as_bytes(),
                true,
                |_, _, _| true,
                |_, _| {
                    keys_found += 1;
                    KeyEvaluation::Skip
                },
                |_, _, _| unreachable!(),
            )
            .map_err(|err| match err {
                AbortError::Other(err) => err,
                AbortError::Nebari(err) => crate::Error::from(err),
            })
            .unwrap();

            Ok(keys_found)
        })
        .await
        .unwrap()
    }

    async fn reduce_in_view(
        &self,
        view_name: &ViewName,
        key: Option<QueryKey<Bytes>>,
        access_policy: AccessPolicy,
    ) -> Result<Vec<u8>, bonsaidb_core::Error> {
        let view = self
            .data
            .schema
            .view_by_name(view_name)
            .ok_or(bonsaidb_core::Error::CollectionNotFound)?;
        let mut mappings = self
            .grouped_reduce_in_view(view_name, key, access_policy)
            .await?;

        let result = if mappings.len() == 1 {
            mappings.pop().unwrap().value.into_vec()
        } else {
            view.reduce(
                &mappings
                    .iter()
                    .map(|map| (map.key.as_ref(), map.value.as_ref()))
                    .collect::<Vec<_>>(),
                true,
            )
            .map_err(Error::from)?
        };

        Ok(result)
    }

    async fn grouped_reduce_in_view(
        &self,
        view_name: &ViewName,
        key: Option<QueryKey<Bytes>>,
        access_policy: AccessPolicy,
    ) -> Result<Vec<MappedSerializedValue>, bonsaidb_core::Error> {
        let view = self
            .data
            .schema
            .view_by_name(view_name)
            .ok_or(bonsaidb_core::Error::CollectionNotFound)?;
        let mut mappings = Vec::new();
        self.for_each_in_view(view, key, Sort::Ascending, None, access_policy, |entry| {
            mappings.push(MappedSerializedValue {
                key: entry.key,
                value: entry.reduced_value,
            });
            Ok(())
        })
        .await?;

        Ok(mappings)
    }

    fn apply_transaction_to_roots(
        &self,
        transaction: &Transaction,
    ) -> Result<Vec<OperationResult>, Error> {
        let mut open_trees = OpenTrees::default();
        for op in &transaction.operations {
            if !self.data.schema.contains_collection_id(&op.collection) {
                return Err(Error::Core(bonsaidb_core::Error::CollectionNotFound));
            }

            #[cfg(any(feature = "encryption", feature = "compression"))]
            let vault = if let Some(encryption_key) =
                self.collection_encryption_key(&op.collection).cloned()
            {
                #[cfg(feature = "encryption")]
                if let Some(mut vault) = self.storage().tree_vault().cloned() {
                    vault.key = Some(encryption_key);
                    Some(vault)
                } else {
                    TreeVault::new_if_needed(
                        Some(encryption_key),
                        self.storage().vault(),
                        #[cfg(feature = "compression")]
                        None,
                    )
                }

                #[cfg(not(feature = "encryption"))]
                {
                    drop(encryption_key);
                    return Err(Error::EncryptionDisabled);
                }
            } else {
                self.storage().tree_vault().cloned()
            };

            open_trees.open_trees_for_document_change(
                &op.collection,
                &self.data.schema,
                #[cfg(any(feature = "encryption", feature = "compression"))]
                vault,
            );
        }

        let mut roots_transaction = self
            .data
            .context
            .roots
            .transaction::<_, dyn AnyTreeRoot<AnyFile>>(&open_trees.trees)?;

        let mut results = Vec::new();
        let mut changed_documents = Vec::new();
        let mut collection_indexes = HashMap::new();
        let mut collections = Vec::new();
        for op in &transaction.operations {
            let result = self.execute_operation(
                op,
                &mut roots_transaction,
                &open_trees.trees_index_by_name,
            )?;

            if let Some((collection, id, deleted)) = match &result {
                OperationResult::DocumentUpdated { header, collection } => {
                    Some((collection, header.id, false))
                }
                OperationResult::DocumentDeleted { id, collection } => {
                    Some((collection, *id, true))
                }
                OperationResult::Success => None,
            } {
                let collection = match collection_indexes.get(collection) {
                    Some(index) => *index,
                    None => {
                        if let Ok(id) = u16::try_from(collections.len()) {
                            collection_indexes.insert(collection.clone(), id);
                            collections.push(collection.clone());
                            id
                        } else {
                            return Err(Error::TransactionTooLarge);
                        }
                    }
                };
                changed_documents.push(ChangedDocument {
                    collection,
                    id,
                    deleted,
                });
            }
            results.push(result);
        }

        self.invalidate_changed_documents(
            &mut roots_transaction,
            &open_trees,
            &collections,
            &changed_documents,
        )?;

        roots_transaction
            .entry_mut()
            .set_data(compat::serialize_executed_transaction_changes(
                &Changes::Documents(DocumentChanges {
                    collections,
                    documents: changed_documents,
                }),
            )?)?;

        roots_transaction.commit()?;

        Ok(results)
    }

    fn invalidate_changed_documents(
        &self,
        roots_transaction: &mut ExecutingTransaction<AnyFile>,
        open_trees: &OpenTrees,
        collections: &[CollectionName],
        changed_documents: &[ChangedDocument],
    ) -> Result<(), Error> {
        for (collection, changed_documents) in &changed_documents
            .iter()
            .group_by(|doc| &collections[usize::from(doc.collection)])
        {
            if let Some(views) = self.data.schema.views_in_collection(collection) {
                let changed_documents = changed_documents.collect::<Vec<_>>();
                for view in views.into_iter().filter(|view| !view.unique()) {
                    let view_name = view.view_name();
                    let tree_name = view_invalidated_docs_tree_name(&view_name);
                    for changed_document in &changed_documents {
                        let mut invalidated_docs = roots_transaction
                            .tree::<Unversioned>(open_trees.trees_index_by_name[&tree_name])
                            .unwrap();
                        invalidated_docs.set(changed_document.id.as_ref().to_vec(), b"")?;
                    }
                }
            }
        }
        Ok(())
    }

    fn execute_operation(
        &self,
        operation: &Operation,
        transaction: &mut ExecutingTransaction<AnyFile>,
        tree_index_map: &HashMap<String, usize>,
    ) -> Result<OperationResult, Error> {
        match &operation.command {
            Command::Insert { id, contents } => {
                self.execute_insert(operation, transaction, tree_index_map, *id, contents)
            }
            Command::Update { header, contents } => self.execute_update(
                operation,
                transaction,
                tree_index_map,
                header.id,
                Some(&header.revision),
                contents,
            ),
            Command::Overwrite { id, contents } => {
                self.execute_update(operation, transaction, tree_index_map, *id, None, contents)
            }
            Command::Delete { header } => {
                self.execute_delete(operation, transaction, tree_index_map, header)
            }
        }
    }

    fn execute_update(
        &self,
        operation: &Operation,
        transaction: &mut ExecutingTransaction<AnyFile>,
        tree_index_map: &HashMap<String, usize>,
        id: DocumentId,
        check_revision: Option<&Revision>,
        contents: &[u8],
    ) -> Result<OperationResult, crate::Error> {
        let mut documents = transaction
            .tree::<Versioned>(tree_index_map[&document_tree_name(&operation.collection)])
            .unwrap();
        let document_id = ArcBytes::from(id.to_vec());
        let mut result = None;
        let mut updated = false;
        documents.modify(
            vec![document_id.clone()],
            nebari::tree::Operation::CompareSwap(CompareSwap::new(&mut |_key,
                                                                        value: Option<
                ArcBytes<'_>,
            >| {
                if let Some(old) = value {
                    let doc = match deserialize_document(&old) {
                        Ok(doc) => doc,
                        Err(err) => {
                            result = Some(Err(err));
                            return nebari::tree::KeyOperation::Skip;
                        }
                    };
                    if check_revision.is_none() || Some(&doc.header.revision) == check_revision {
                        if let Some(updated_revision) = doc.header.revision.next_revision(contents)
                        {
                            let updated_header = Header {
                                id,
                                revision: updated_revision,
                            };
                            let serialized_doc = match serialize_document(&BorrowedDocument {
                                header: updated_header.clone(),
                                contents: CowBytes::from(contents),
                            }) {
                                Ok(bytes) => bytes,
                                Err(err) => {
                                    result = Some(Err(Error::from(err)));
                                    return nebari::tree::KeyOperation::Skip;
                                }
                            };
                            result = Some(Ok(OperationResult::DocumentUpdated {
                                collection: operation.collection.clone(),
                                header: updated_header,
                            }));
                            updated = true;
                            return nebari::tree::KeyOperation::Set(ArcBytes::from(serialized_doc));
                        }

                        // If no new revision was made, it means an attempt to
                        // save a document with the same contents was made.
                        // We'll return a success but not actually give a new
                        // version
                        result = Some(Ok(OperationResult::DocumentUpdated {
                            collection: operation.collection.clone(),
                            header: doc.header,
                        }));
                    } else {
                        result = Some(Err(Error::Core(bonsaidb_core::Error::DocumentConflict(
                            operation.collection.clone(),
                            Box::new(doc.header),
                        ))));
                    }
                } else if check_revision.is_none() {
                    let doc = BorrowedDocument::new(id, contents);
                    match serialize_document(&doc).map(|bytes| (doc, bytes)) {
                        Ok((doc, serialized)) => {
                            result = Some(Ok(OperationResult::DocumentUpdated {
                                collection: operation.collection.clone(),
                                header: doc.header,
                            }));
                            return nebari::tree::KeyOperation::Set(ArcBytes::from(serialized));
                        }
                        Err(err) => {
                            result = Some(Err(Error::from(err)));
                        }
                    }
                } else {
                    result = Some(Err(Error::Core(bonsaidb_core::Error::DocumentNotFound(
                        operation.collection.clone(),
                        Box::new(id),
                    ))));
                }
                nebari::tree::KeyOperation::Skip
            })),
        )?;
        drop(documents);

        if updated {
            self.update_unique_views(&document_id, operation, transaction, tree_index_map)?;
        }

        result.expect("nebari should invoke the callback even when the key isn't found")
    }

    fn execute_insert(
        &self,
        operation: &Operation,
        transaction: &mut ExecutingTransaction<AnyFile>,
        tree_index_map: &HashMap<String, usize>,
        id: Option<DocumentId>,
        contents: &[u8],
    ) -> Result<OperationResult, Error> {
        let mut documents = transaction
            .tree::<Versioned>(tree_index_map[&document_tree_name(&operation.collection)])
            .unwrap();
        let id = if let Some(id) = id {
            id
        } else if let Some(last_key) = documents.last_key()? {
            let id = DocumentId::try_from(last_key.as_slice())?;
            self.data
                .schema
                .next_id_for_collection(&operation.collection, Some(id))?
        } else {
            self.data
                .schema
                .next_id_for_collection(&operation.collection, None)?
        };

        let doc = BorrowedDocument::new(id, contents);
        let serialized: Vec<u8> = serialize_document(&doc)?;
        let document_id = ArcBytes::from(doc.header.id.as_ref().to_vec());
        if let Some(document) = documents.replace(document_id.clone(), serialized)? {
            let doc = deserialize_document(&document)?;
            Err(Error::Core(bonsaidb_core::Error::DocumentConflict(
                operation.collection.clone(),
                Box::new(doc.header),
            )))
        } else {
            drop(documents);
            self.update_unique_views(&document_id, operation, transaction, tree_index_map)?;

            Ok(OperationResult::DocumentUpdated {
                collection: operation.collection.clone(),
                header: doc.header,
            })
        }
    }

    fn execute_delete(
        &self,
        operation: &Operation,
        transaction: &mut ExecutingTransaction<AnyFile>,
        tree_index_map: &HashMap<String, usize>,
        header: &Header,
    ) -> Result<OperationResult, Error> {
        let mut documents = transaction
            .tree::<Versioned>(tree_index_map[&document_tree_name(&operation.collection)])
            .unwrap();
        if let Some(vec) = documents.remove(header.id.as_ref())? {
            drop(documents);
            let doc = deserialize_document(&vec)?;
            if &doc.header == header {
                self.update_unique_views(
                    &ArcBytes::from(doc.header.id.to_vec()),
                    operation,
                    transaction,
                    tree_index_map,
                )?;

                Ok(OperationResult::DocumentDeleted {
                    collection: operation.collection.clone(),
                    id: header.id,
                })
            } else {
                Err(Error::Core(bonsaidb_core::Error::DocumentConflict(
                    operation.collection.clone(),
                    Box::new(header.clone()),
                )))
            }
        } else {
            Err(Error::Core(bonsaidb_core::Error::DocumentNotFound(
                operation.collection.clone(),
                Box::new(header.id),
            )))
        }
    }

    fn update_unique_views(
        &self,
        document_id: &ArcBytes<'static>,
        operation: &Operation,
        transaction: &mut ExecutingTransaction<AnyFile>,
        tree_index_map: &HashMap<String, usize>,
    ) -> Result<(), Error> {
        if let Some(unique_views) = self
            .data
            .schema
            .unique_views_in_collection(&operation.collection)
        {
            let documents = transaction
                .unlocked_tree(tree_index_map[&document_tree_name(&operation.collection)])
                .unwrap();
            for view in unique_views {
                let name = view.view_name();
                let document_map = transaction
                    .unlocked_tree(tree_index_map[&view_document_map_tree_name(&name)])
                    .unwrap();
                let view_entries = transaction
                    .unlocked_tree(tree_index_map[&view_entries_tree_name(&name)])
                    .unwrap();
                mapper::DocumentRequest {
                    database: self,
                    document_ids: vec![document_id.clone()],
                    map_request: &mapper::Map {
                        database: self.data.name.clone(),
                        collection: operation.collection.clone(),
                        view_name: name.clone(),
                    },
                    document_map,
                    documents,
                    view_entries,
                    view,
                }
                .map()?;
            }
        }

        Ok(())
    }

    fn create_view_iterator<'a, K: for<'k> Key<'k> + 'a>(
        view_entries: &'a Tree<Unversioned, AnyFile>,
        key: Option<QueryKey<K>>,
        order: Sort,
        limit: Option<usize>,
    ) -> Result<Vec<ViewEntry>, Error> {
        let mut values = Vec::new();
        let forwards = match order {
            Sort::Ascending => true,
            Sort::Descending => false,
        };
        let mut values_read = 0;
        if let Some(key) = key {
            match key {
                QueryKey::Range(range) => {
                    let range = range
                        .as_ord_bytes()
                        .map_err(view::Error::key_serialization)?;
                    view_entries.scan::<Infallible, _, _, _, _>(
                        &range.map_ref(|bytes| &bytes[..]),
                        forwards,
                        |_, _, _| true,
                        |_, _| {
                            if let Some(limit) = limit {
                                if values_read >= limit {
                                    return KeyEvaluation::Stop;
                                }
                                values_read += 1;
                            }
                            KeyEvaluation::ReadData
                        },
                        |_key, _index, value| {
                            values.push(value);
                            Ok(())
                        },
                    )?;
                }
                QueryKey::Matches(key) => {
                    let key = key
                        .as_ord_bytes()
                        .map_err(view::Error::key_serialization)?
                        .to_vec();

                    values.extend(view_entries.get(&key)?);
                }
                QueryKey::Multiple(list) => {
                    let mut list = list
                        .into_iter()
                        .map(|key| {
                            key.as_ord_bytes()
                                .map(|bytes| bytes.to_vec())
                                .map_err(view::Error::key_serialization)
                        })
                        .collect::<Result<Vec<_>, _>>()?;

                    list.sort();

                    values.extend(
                        view_entries
                            .get_multiple(list.iter().map(Vec::as_slice))?
                            .into_iter()
                            .map(|(_, value)| value),
                    );
                }
            }
        } else {
            view_entries.scan::<Infallible, _, _, _, _>(
                &(..),
                forwards,
                |_, _, _| true,
                |_, _| {
                    if let Some(limit) = limit {
                        if values_read >= limit {
                            return KeyEvaluation::Stop;
                        }
                        values_read += 1;
                    }
                    KeyEvaluation::ReadData
                },
                |_, _, value| {
                    values.push(value);
                    Ok(())
                },
            )?;
        }

        values
            .into_iter()
            .map(|value| bincode::deserialize(&value).map_err(Error::from))
            .collect::<Result<Vec<_>, Error>>()
    }

    #[cfg(any(feature = "encryption", feature = "compression"))]
    pub(crate) fn collection_encryption_key(&self, collection: &CollectionName) -> Option<&KeyId> {
        self.schematic()
            .encryption_key_for_collection(collection)
            .or_else(|| self.storage().default_encryption_key())
    }

    #[cfg_attr(
        not(feature = "encryption"),
        allow(
            unused_mut,
            unused_variables,
            clippy::unused_self,
            clippy::let_and_return
        )
    )]
    #[allow(clippy::unnecessary_wraps)]
    pub(crate) fn collection_tree<R: Root, S: Into<Cow<'static, str>>>(
        &self,
        collection: &CollectionName,
        name: S,
    ) -> Result<TreeRoot<R, AnyFile>, Error> {
        let mut tree = R::tree(name);

        #[cfg(any(feature = "encryption", feature = "compression"))]
        match (
            self.collection_encryption_key(collection),
            self.storage().tree_vault().cloned(),
        ) {
            (Some(override_key), Some(mut vault)) => {
                #[cfg(feature = "encryption")]
                {
                    vault.key = Some(override_key.clone());
                    tree = tree.with_vault(vault);
                }

                #[cfg(not(feature = "encryption"))]
                {
                    return Err(Error::EncryptionDisabled);
                }
            }
            (None, Some(vault)) => {
                tree = tree.with_vault(vault);
            }
            (key, None) => {
                #[cfg(feature = "encryption")]
                if let Some(vault) = TreeVault::new_if_needed(
                    key.cloned(),
                    self.storage().vault(),
                    #[cfg(feature = "compression")]
                    None,
                ) {
                    tree = tree.with_vault(vault);
                }

                #[cfg(not(feature = "encryption"))]
                if key.is_some() {
                    return Err(Error::EncryptionDisabled);
                }
            }
        }

        Ok(tree)
    }

    pub(crate) async fn update_key_expiration_async<'key>(
        &self,
        tree_key: impl Into<Cow<'key, str>>,
        expiration: Option<Timestamp>,
    ) {
        self.data
            .context
            .update_key_expiration_async(tree_key, expiration)
            .await;
    }
}
#[derive(Serialize, Deserialize)]
struct LegacyHeader {
    id: u64,
    revision: Revision,
}
#[derive(Serialize, Deserialize)]
struct LegacyDocument<'a> {
    header: LegacyHeader,
    #[serde(borrow)]
    contents: &'a [u8],
}

pub(crate) fn deserialize_document(bytes: &[u8]) -> Result<BorrowedDocument<'_>, Error> {
    match pot::from_slice::<BorrowedDocument<'_>>(bytes) {
        Ok(document) => Ok(document),
        Err(err) => match bincode::deserialize::<LegacyDocument<'_>>(bytes) {
            Ok(legacy_doc) => Ok(BorrowedDocument {
                header: Header {
                    id: DocumentId::from_u64(legacy_doc.header.id),
                    revision: legacy_doc.header.revision,
                },
                contents: CowBytes::from(legacy_doc.contents),
            }),
            Err(_) => Err(Error::from(err)),
        },
    }
}

fn serialize_document(document: &BorrowedDocument<'_>) -> Result<Vec<u8>, bonsaidb_core::Error> {
    pot::to_vec(document)
        .map_err(Error::from)
        .map_err(bonsaidb_core::Error::from)
}

#[async_trait]
impl Connection for Database {
    #[cfg_attr(feature = "tracing", tracing::instrument(skip(transaction)))]
    async fn apply_transaction(
        &self,
        transaction: Transaction,
    ) -> Result<Vec<OperationResult>, bonsaidb_core::Error> {
        let task_self = self.clone();
        let mut unique_view_tasks = Vec::new();
        for collection_name in transaction
            .operations
            .iter()
            .map(|op| &op.collection)
            .collect::<HashSet<_>>()
        {
            if let Some(views) = self.data.schema.views_in_collection(collection_name) {
                for view in views {
                    if view.unique() {
                        if let Some(task) = self
                            .data
                            .storage
                            .tasks()
                            .spawn_integrity_check(view, self)
                            .await?
                        {
                            unique_view_tasks.push(task);
                        }
                    }
                }
            }
        }

        let mut unique_view_mapping_tasks = Vec::new();
        for task in unique_view_tasks {
            if let Some(spawned_task) = task
                .receive()
                .await
                .map_err(Error::from)?
                .map_err(Error::from)?
            {
                unique_view_mapping_tasks.push(spawned_task);
            }
        }
        for task in unique_view_mapping_tasks {
            let mut task = fast_async_lock!(task);
            if let Some(task) = task.take() {
                task.receive()
                    .await
                    .map_err(Error::from)?
                    .map_err(Error::from)?;
            }
        }

        tokio::task::spawn_blocking(move || task_self.apply_transaction_to_roots(&transaction))
            .await
            .map_err(|err| bonsaidb_core::Error::Database(err.to_string()))?
            .map_err(bonsaidb_core::Error::from)
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip(id)))]
    async fn get<C, PrimaryKey>(
        &self,
        id: PrimaryKey,
    ) -> Result<Option<OwnedDocument>, bonsaidb_core::Error>
    where
        C: schema::Collection,
        PrimaryKey: Into<AnyDocumentId<C::PrimaryKey>> + Send,
    {
        self.get_from_collection_id(id.into().to_document_id()?, &C::collection_name())
            .await
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip(ids)))]
    async fn get_multiple<C, PrimaryKey, DocumentIds, I>(
        &self,
        ids: DocumentIds,
    ) -> Result<Vec<OwnedDocument>, bonsaidb_core::Error>
    where
        C: schema::Collection,
        DocumentIds: IntoIterator<Item = PrimaryKey, IntoIter = I> + Send + Sync,
        I: Iterator<Item = PrimaryKey> + Send + Sync,
        PrimaryKey: Into<AnyDocumentId<C::PrimaryKey>> + Send + Sync,
    {
        self.get_multiple_from_collection_id(
            &ids.into_iter()
                .map(|id| id.into().to_document_id())
                .collect::<Result<Vec<_>, _>>()?,
            &C::collection_name(),
        )
        .await
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip(ids, order, limit)))]
    async fn list<C, R, PrimaryKey>(
        &self,
        ids: R,
        order: Sort,
        limit: Option<usize>,
    ) -> Result<Vec<OwnedDocument>, bonsaidb_core::Error>
    where
        C: schema::Collection,
        R: Into<Range<PrimaryKey>> + Send,
        PrimaryKey: Into<AnyDocumentId<C::PrimaryKey>> + Send,
    {
        self.list(
            ids.into().map_result(|id| id.into().to_document_id())?,
            order,
            limit,
            &C::collection_name(),
        )
        .await
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip(ids)))]
    async fn count<C, R, PrimaryKey>(&self, ids: R) -> Result<u64, bonsaidb_core::Error>
    where
        C: schema::Collection,
        R: Into<Range<PrimaryKey>> + Send,
        PrimaryKey: Into<AnyDocumentId<C::PrimaryKey>> + Send,
    {
        self.count(
            ids.into().map_result(|id| id.into().to_document_id())?,
            &C::collection_name(),
        )
        .await
    }

    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(skip(starting_id, result_limit))
    )]
    async fn list_executed_transactions(
        &self,
        starting_id: Option<u64>,
        result_limit: Option<usize>,
    ) -> Result<Vec<transaction::Executed>, bonsaidb_core::Error> {
        let result_limit = result_limit
            .unwrap_or(LIST_TRANSACTIONS_DEFAULT_RESULT_COUNT)
            .min(LIST_TRANSACTIONS_MAX_RESULTS);
        if result_limit > 0 {
            let task_self = self.clone();
            tokio::task::spawn_blocking::<_, Result<Vec<transaction::Executed>, Error>>(move || {
                let range = if let Some(starting_id) = starting_id {
                    Range::from(starting_id..)
                } else {
                    Range::from(..)
                };

                let mut entries = Vec::new();
                task_self.roots().transactions().scan(range, |entry| {
                    entries.push(entry);
                    entries.len() < result_limit
                })?;

                entries
                    .into_iter()
                    .map(|entry| {
                        if let Some(data) = entry.data() {
                            let changes = compat::deserialize_executed_transaction_changes(data)?;
                            Ok(Some(transaction::Executed {
                                id: entry.id,
                                changes,
                            }))
                        } else {
                            Ok(None)
                        }
                    })
                    .filter_map(Result::transpose)
                    .collect::<Result<Vec<_>, Error>>()
            })
            .await
            .unwrap()
            .map_err(bonsaidb_core::Error::from)
        } else {
            // A request was made to return an empty result? This should probably be
            // an error, but technically this is a correct response.
            Ok(Vec::default())
        }
    }

    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(skip(key, order, limit, access_policy))
    )]
    #[must_use]
    async fn query<V: schema::SerializedView>(
        &self,
        key: Option<QueryKey<V::Key>>,
        order: Sort,
        limit: Option<usize>,
        access_policy: AccessPolicy,
    ) -> Result<Vec<Map<V::Key, V::Value>>, bonsaidb_core::Error>
    where
        Self: Sized,
    {
        let mut results = Vec::new();
        self.for_each_view_entry::<V, _>(key, order, limit, access_policy, |entry| {
            let key = <V::Key as Key>::from_ord_bytes(&entry.key)
                .map_err(view::Error::key_serialization)
                .map_err(Error::from)?;
            for entry in entry.mappings {
                results.push(Map::new(
                    entry.source,
                    key.clone(),
                    V::deserialize(&entry.value)?,
                ));
            }
            Ok(())
        })
        .await?;

        Ok(results)
    }

    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(skip(key, order, limit, access_policy))
    )]
    async fn query_with_docs<V: schema::SerializedView>(
        &self,
        key: Option<QueryKey<V::Key>>,
        order: Sort,
        limit: Option<usize>,
        access_policy: AccessPolicy,
    ) -> Result<MappedDocuments<OwnedDocument, V>, bonsaidb_core::Error>
    where
        Self: Sized,
    {
        let results = Connection::query::<V>(self, key, order, limit, access_policy).await?;

        let documents = self
            .get_multiple::<V::Collection, _, _, _>(results.iter().map(|m| m.source.id))
            .await?
            .into_iter()
            .map(|doc| (doc.header.id, doc))
            .collect::<BTreeMap<_, _>>();

        Ok(MappedDocuments {
            mappings: results,
            documents,
        })
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip(key, access_policy)))]
    async fn reduce<V: schema::SerializedView>(
        &self,
        key: Option<QueryKey<V::Key>>,
        access_policy: AccessPolicy,
    ) -> Result<V::Value, bonsaidb_core::Error>
    where
        Self: Sized,
    {
        let view = self
            .data
            .schema
            .view::<V>()
            .expect("query made with view that isn't registered with this database");

        let result = self
            .reduce_in_view(
                &view.view_name(),
                key.map(|key| key.serialized()).transpose()?,
                access_policy,
            )
            .await?;
        let value = V::deserialize(&result)?;

        Ok(value)
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip(key, access_policy)))]
    async fn reduce_grouped<V: schema::SerializedView>(
        &self,
        key: Option<QueryKey<V::Key>>,
        access_policy: AccessPolicy,
    ) -> Result<Vec<MappedValue<V::Key, V::Value>>, bonsaidb_core::Error>
    where
        Self: Sized,
    {
        let view = self
            .data
            .schema
            .view::<V>()
            .expect("query made with view that isn't registered with this database");

        let results = self
            .grouped_reduce_in_view(
                &view.view_name(),
                key.map(|key| key.serialized()).transpose()?,
                access_policy,
            )
            .await?;
        results
            .into_iter()
            .map(|map| {
                Ok(MappedValue::new(
                    V::Key::from_ord_bytes(&map.key).map_err(view::Error::key_serialization)?,
                    V::deserialize(&map.value)?,
                ))
            })
            .collect::<Result<Vec<_>, bonsaidb_core::Error>>()
    }

    async fn delete_docs<V: schema::SerializedView>(
        &self,
        key: Option<QueryKey<V::Key>>,
        access_policy: AccessPolicy,
    ) -> Result<u64, bonsaidb_core::Error>
    where
        Self: Sized,
    {
        let collection = <V::Collection as Collection>::collection_name();
        let mut transaction = Transaction::default();
        self.for_each_view_entry::<V, _>(key, Sort::Ascending, None, access_policy, |entry| {
            for mapping in entry.mappings {
                transaction.push(Operation::delete(collection.clone(), mapping.source));
            }

            Ok(())
        })
        .await?;

        let results = Connection::apply_transaction(self, transaction).await?;

        Ok(results.len() as u64)
    }

    #[cfg_attr(feature = "tracing", tracing::instrument)]
    async fn last_transaction_id(&self) -> Result<Option<u64>, bonsaidb_core::Error> {
        Ok(self.roots().transactions().current_transaction_id())
    }

    #[cfg_attr(feature = "tracing", tracing::instrument)]
    async fn compact_collection<C: schema::Collection>(&self) -> Result<(), bonsaidb_core::Error> {
        self.storage()
            .tasks()
            .compact_collection(self.clone(), C::collection_name())
            .await?;
        Ok(())
    }

    #[cfg_attr(feature = "tracing", tracing::instrument)]
    async fn compact(&self) -> Result<(), bonsaidb_core::Error> {
        self.storage()
            .tasks()
            .compact_database(self.clone())
            .await?;
        Ok(())
    }

    #[cfg_attr(feature = "tracing", tracing::instrument)]
    async fn compact_key_value_store(&self) -> Result<(), bonsaidb_core::Error> {
        self.storage()
            .tasks()
            .compact_key_value_store(self.clone())
            .await?;
        Ok(())
    }
}

type ViewIterator<'a> =
    Box<dyn Iterator<Item = Result<(ArcBytes<'static>, ArcBytes<'static>), Error>> + 'a>;

struct ViewEntryCollectionIterator<'a> {
    iterator: ViewIterator<'a>,
}

impl<'a> Iterator for ViewEntryCollectionIterator<'a> {
    type Item = Result<ViewEntry, crate::Error>;

    fn next(&mut self) -> Option<Self::Item> {
        self.iterator.next().map(|item| {
            item.map_err(crate::Error::from)
                .and_then(|(_, value)| bincode::deserialize(&value).map_err(Error::from))
        })
    }
}

#[derive(Debug, Clone)]
pub(crate) struct Context {
    data: Arc<ContextData>,
}

impl Deref for Context {
    type Target = ContextData;

    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

#[derive(Debug)]
pub(crate) struct ContextData {
    pub(crate) roots: Roots<AnyFile>,
    key_value_state: Arc<Mutex<keyvalue::KeyValueState>>,
    runtime: tokio::runtime::Handle,
}

impl Borrow<Roots<AnyFile>> for Context {
    fn borrow(&self) -> &Roots<AnyFile> {
        &self.data.roots
    }
}

impl Context {
    pub(crate) fn new(roots: Roots<AnyFile>, key_value_persistence: KeyValuePersistence) -> Self {
        let (background_sender, background_receiver) =
            watch::channel(BackgroundWorkerProcessTarget::Never);
        let key_value_state = Arc::new(Mutex::new(keyvalue::KeyValueState::new(
            key_value_persistence,
            roots.clone(),
            background_sender,
        )));
        let context = Self {
            data: Arc::new(ContextData {
                roots,
                key_value_state: key_value_state.clone(),
                runtime: tokio::runtime::Handle::current(),
            }),
        };
        tokio::task::spawn(keyvalue::background_worker(
            key_value_state,
            background_receiver,
        ));
        context
    }

    pub(crate) async fn perform_kv_operation(
        &self,
        op: KeyOperation,
    ) -> Result<Output, bonsaidb_core::Error> {
        let mut state = fast_async_lock!(self.data.key_value_state);
        state
            .perform_kv_operation(op, &self.data.key_value_state)
            .await
    }

    pub(crate) async fn update_key_expiration_async<'key>(
        &self,
        tree_key: impl Into<Cow<'key, str>>,
        expiration: Option<Timestamp>,
    ) {
        let mut state = fast_async_lock!(self.data.key_value_state);
        state.update_key_expiration(tree_key, expiration);
    }
}

impl Drop for ContextData {
    fn drop(&mut self) {
        let key_value_state = self.key_value_state.clone();
        self.runtime.spawn(async move {
            let mut state = fast_async_lock!(key_value_state);
            state.shutdown(&key_value_state).await
        });
    }
}

pub fn document_tree_name(collection: &CollectionName) -> String {
    format!("collection.{:#}", collection)
}

pub struct DocumentIdRange(Range<DocumentId>);

impl<'a> BorrowByteRange<'a> for DocumentIdRange {
    fn borrow_as_bytes(&'a self) -> BorrowedRange<'a> {
        BorrowedRange {
            start: match &self.0.start {
                connection::Bound::Unbounded => ops::Bound::Unbounded,
                connection::Bound::Included(docid) => ops::Bound::Included(docid.as_ref()),
                connection::Bound::Excluded(docid) => ops::Bound::Excluded(docid.as_ref()),
            },
            end: match &self.0.end {
                connection::Bound::Unbounded => ops::Bound::Unbounded,
                connection::Bound::Included(docid) => ops::Bound::Included(docid.as_ref()),
                connection::Bound::Excluded(docid) => ops::Bound::Excluded(docid.as_ref()),
            },
        }
    }
}
