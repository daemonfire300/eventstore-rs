use std::fmt::Debug;

use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::{params, Statement};
use tracing::{debug, instrument, warn};
use uuid::Uuid;

use crate::backend::model::Event;

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
                aggregate_id INTEGER,
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
        backend.init_indices().unwrap();
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

    #[instrument]
    fn init_indices(&self) -> Result<(), Error> {
        let res = self.pool.get().expect("failed to get connection").execute(
            "CREATE INDEX IF NOT EXISTS eventstore_agg_id_idx ON eventstore (aggregate_id)",
            params![],
        );
        match res {
            Ok(_) => Ok(()),
            Err(_) => Err(Error),
        }
    }

    #[instrument]
    pub fn get_agg_max_version(&self, agg_id_str: &str) -> Result<u32, Error> {
        let conn = self.pool.get().expect("failed to get connection");
        let mut stmt = conn
            .prepare("SELECT COALESCE(MAX(version), 0) as max_version FROM eventstore WHERE aggregate_id = ?")
            .unwrap();
        let version = match stmt.query_row(params![agg_id_str], |row| match row.get::<_, u32>(0) {
            Ok(val) => Ok(val),
            Err(err) => {
                warn!(sqlite_error = err.to_string());
                Err(err)
            }
        }) {
            Ok(res) => res,
            Err(_) => {
                return Err(Error);
            }
        };
        debug!(current_event_version = version);
        Ok(version)
    }

    #[instrument]
    pub fn append_event(&self, event: &Event) -> Result<(), Error> {
        let version = match self.get_agg_max_version(&event.id.to_string()) {
            Ok(version) => version,
            Err(err) => {
                return Err(err);
            }
        };
        let expected_version = version + 1;
        if event.version != expected_version {
            warn!("version mismtach {} != {}", event.version, expected_version);
            return Err(Error);
        }
        let conn = self.pool.get().expect("failed to get connection");
        let res = conn.execute(
            "INSERT INTO eventstore(aggregate_id, version, data) VALUES(?,?,?)",
            params![&event.id.to_string(), event.version, event.data],
        );
        match res {
            Ok(_) => Ok(()),
            Err(err) => {
                warn!(sqlite_error = err.to_string());
                Err(Error)
            }
        }
    }

    #[instrument]
    fn result_from_stmt(stmt: &mut Statement, agg_id_str: &str) -> Result<Vec<Event>, Error> {
        let mut events: Vec<_> = Vec::new();

        let query_res = stmt.query_map(params![agg_id_str], |r| {
            let tmp: String = r.get(0).unwrap();
            let res = uuid::Uuid::parse_str(tmp.as_str());
            let id = match res {
                Ok(id) => id,
                // return nil uuid for now
                Err(_) => uuid::Uuid::parse_str("00000000-0000-0000-0000-000000000000").unwrap(),
            };
            Ok(Event {
                id,
                data: r.get(1).unwrap(),
                version: r.get(2).unwrap(),
            })
        });
        match query_res {
            Ok(iter) => {
                iter.filter_map(|e| match e {
                    Ok(val) => Some(val),
                    Err(_) => None,
                })
                .fold(&mut events, |acc, e| {
                    acc.push(e);
                    acc
                });
                Ok(events)
            }
            Err(err) => {
                warn!(sqlite_error = err.to_string());
                Err(Error)
            }
        }
    }

    #[instrument]
    pub fn get_aggretate(&self, aggregate_id: Uuid) -> Result<Vec<Event>, Error> {
        let agg_id_str: String = aggregate_id.to_string();
        let conn = self.pool.get().expect("failed to get connection");
        let mut stmt = conn
            .prepare("SELECT * FROM eventstore WHERE aggregate_id = ? ORDER BY version ASC")
            .unwrap();
        SqliteBackend::result_from_stmt(&mut stmt, &agg_id_str)
    }
}
