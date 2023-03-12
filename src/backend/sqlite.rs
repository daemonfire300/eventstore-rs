use std::fmt::Debug;

use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::params;
use tracing::{debug, info, instrument};

pub struct SqliteBackend {
    pool: Pool<SqliteConnectionManager>,
}

pub struct Error;

impl Debug for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Error").finish()
    }
}

static CREATE_TABLE_STMT: &'static str = "CREATE TABLE eventstore(
                aggregateId INTEGER,
                data BLOB,
                version INTEGER
            )";

impl Debug for SqliteBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqliteBackend")
            .field("pool", &self.pool.state())
            .finish()
    }
}

impl SqliteBackend {
    pub fn new() -> Self {
        let manager = r2d2_sqlite::SqliteConnectionManager::memory();
        let pool = r2d2::Pool::new(manager).unwrap();
        let backend = Self { pool };
        backend.init_tables().unwrap();
        return backend;
    }

    #[instrument]
    fn init_tables(&self) -> Result<(), Error> {
        let _span = tracing::debug_span!("creating tables").entered();
        let res = self
            .pool
            .get()
            .expect("failed to get connection")
            .execute(CREATE_TABLE_STMT, params![]);
        debug!(executed_query = CREATE_TABLE_STMT, "executed query");
        match res {
            Ok(_) => Ok(()),
            Err(_) => Err(Error),
        }
    }
}
