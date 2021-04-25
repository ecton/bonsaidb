use serde::{Deserialize, Serialize};

mod timestamp;

pub use self::timestamp::Timestamp;

#[cfg(feature = "keyvalue")]
mod implementation {
    use async_trait::async_trait;
    use futures::future::BoxFuture;
    use serde::{Deserialize, Serialize};

    use crate::{
        kv::{Command, KeyCheck, KeyOperation, KeyStatus, Output, Timestamp},
        Error,
    };

    /// Types for executing get operations.
    pub mod get;
    /// Types for handling key namespaces.
    pub mod namespaced;
    /// Types for executing set operations.
    pub mod set;

    use namespaced::Namespaced;

    /// Key-Value store methods. The Key-Value store is designed to be a
    /// high-performance, lightweight storage mechanism.
    ///
    /// When compared to Collections, the Key-Value store does not offer
    /// ACID-compliant transactions. Instead, the Key-Value store is made more
    /// efficient by periodically flushing the store to disk rather than during each
    /// operation. As such, the Key-Value store is intended to be used as a
    /// lightweight caching layer. However, because each of the operations it
    /// supports are executed atomically, the Key-Value store can also be utilized
    /// for synchronized locking.
    #[async_trait]
    pub trait Kv: Send + Sync {
        /// Executes a single [`KeyOperation`].
        async fn execute_key_operation(&self, op: KeyOperation) -> Result<Output, Error>;

        /// Sets `key` to `value`. This function returns a builder that is also a
        /// Future. Awaiting the builder will execute [`Command::Set`] with the options
        /// given.
        fn set_key<'a, S: Into<String>, V: Serialize>(
            &'a self,
            key: S,
            value: &'a V,
        ) -> set::Builder<'a, Self, V>
        where
            Self: Sized,
        {
            set::Builder::new(
                self,
                self.key_namespace().map(Into::into),
                key.into(),
                value,
            )
        }

        /// Gets the value stored at `key`. This function returns a builder that is also a
        /// Future. Awaiting the builder will execute [`Command::Get`] with the options
        /// given.
        fn get_key<'de, V: Deserialize<'de>, S: Into<String>>(
            &'_ self,
            key: S,
        ) -> get::Builder<'_, Self, V>
        where
            Self: Sized,
        {
            get::Builder::new(self, self.key_namespace().map(Into::into), key.into())
        }

        /// Deletes the value stored at `key`.
        async fn delete_key<S: Into<String> + Send>(&'_ self, key: S) -> Result<KeyStatus, Error>
        where
            Self: Sized,
        {
            match self
                .execute_key_operation(KeyOperation {
                    namespace: self.key_namespace().map(ToOwned::to_owned),
                    key: key.into(),
                    command: Command::Delete,
                })
                .await?
            {
                Output::Status(status) => Ok(status),
                Output::Value(_) => unreachable!("invalid output from delete operation"),
            }
        }

        /// The current namespace.
        fn key_namespace(&self) -> Option<&'_ str> {
            None
        }

        /// Access this Key-Value store within a namespace. When using the returned
        /// [`Namespaced`] instance, all keys specified will be separated into their
        /// own storage designated by `namespace`.
        fn with_key_namespace(&'_ self, namespace: &str) -> Namespaced<'_, Self>
        where
            Self: Sized,
        {
            Namespaced::new(namespace.to_string(), self)
        }
    }

    enum BuilderState<'a, T, R> {
        Pending(Option<T>),
        Executing(BoxFuture<'a, R>),
    }
}

#[cfg(feature = "keyvalue")]
pub use implementation::*;

/// Checks for existing keys.
#[derive(Serialize, Deserialize, Copy, Clone, Debug)]
pub enum KeyCheck {
    /// Only allow the operation if an existing key is present.
    OnlyIfPresent,
    /// Only allow the opeartion if the key isn't present.
    OnlyIfVacant,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
/// An operation performed on a key.
pub struct KeyOperation {
    /// The namespace for the key.
    pub namespace: Option<String>,
    /// The key to operate on.
    pub key: String,
    /// The command to execute.
    pub command: Command,
}

/// Commands for a key-value store.
#[derive(Clone, Serialize, Deserialize, Debug)]
pub enum Command {
    /// Set a key/value pair.
    Set {
        /// The value.
        value: Vec<u8>,
        /// If set, the key will be set to expire automatically.
        expiration: Option<Timestamp>,
        /// If true and the key already exists, the expiration will not be
        /// updated. If false and an expiration is provided, the expiration will
        /// be set.
        keep_existing_expiration: bool,
        /// Conditional checks for whether the key is already present or not.
        check: Option<KeyCheck>,
        /// If true and the key already exists, the existing key will be returned if overwritten.
        return_previous_value: bool,
    },
    /// Get the value from a key.
    Get {
        /// Remove the key after retrieving the value.
        delete: bool,
    },
    /// Delete a key.
    Delete,
}

/// The result of a [`KeyOperation`].
#[derive(Clone, Serialize, Deserialize, Debug)]
pub enum Output {
    /// A status was returned.
    Status(KeyStatus),
    /// A value was returned.
    Value(Option<Vec<u8>>),
}
/// The status of an operation on a Key.
#[derive(Copy, Clone, Serialize, Deserialize, Debug, PartialEq)]
pub enum KeyStatus {
    /// A new key was inserted.
    Inserted,
    /// An existing key was updated with a new value.
    Updated,
    /// A key was deleted.
    Deleted,
    /// No changes were made.
    NotChanged,
}
