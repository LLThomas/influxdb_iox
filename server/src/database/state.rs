//! Database initialization states
use crate::ApplicationState;
use data_types::{server_id::ServerId, DatabaseName};
use internal_types::freezable::Freezable;
use iox_object_store::IoxObjectStore;
use parking_lot::RwLock;
use std::sync::Arc;
use tokio::sync::Notify;
use uuid::Uuid;

use super::init::DatabaseState;

#[derive(Debug, Clone)]
/// Information about where a database is located on object store,
/// and how to perform startup activities.
pub struct DatabaseConfig {
    pub name: DatabaseName<'static>,
    pub server_id: ServerId,
    pub database_uuid: Uuid,
    pub wipe_catalog_on_error: bool,
    pub skip_replay: bool,
}

/// State shared with the `Database` background worker
#[derive(Debug)]
pub(crate) struct DatabaseShared {
    /// Configuration provided to the database at startup
    pub(crate) config: RwLock<DatabaseConfig>,

    /// Application-global state
    pub(crate) application: Arc<ApplicationState>,

    /// Database object store
    pub(crate) iox_object_store: Arc<IoxObjectStore>,

    /// The initialization state of the `Database`, wrapped in a
    /// `Freezable` to ensure there is only one task with an
    /// outstanding intent to write at any time.
    pub(crate) state: RwLock<Freezable<DatabaseState>>,

    /// Notify that the database state has changed
    pub(crate) state_notify: Notify,
}
