use std::fmt::{Debug, Display};

use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::{params, Statement, Transaction};
use tracing::{debug, instrument, warn};
use uuid::Uuid;

use crate::backend::model::Event;

pub struct SqliteBackend {
    pool: Pool<SqliteConnectionManager>,
}

pub enum Error {
    WithMsg(String),
    InvalidUUID,
    Sqlite(rusqlite::Error),
    R2D2Sqlite(r2d2::Error),
}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::InvalidUUID => f.write_fmt(format_args!("could not parse uuid")),
            Error::Sqlite(err) => f.write_fmt(format_args!("{}", err)),
            Error::R2D2Sqlite(err) => f.write_fmt(format_args!("{}", err)),
            Error::WithMsg(msg) => f.write_fmt(format_args!("{}", msg)),
        }
    }
}

impl Debug for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Error").finish()
    }
}

impl From<rusqlite::Error> for Error {
    fn from(value: rusqlite::Error) -> Self {
        Error::Sqlite(value)
    }
}

impl From<r2d2::Error> for Error {
    fn from(value: r2d2::Error) -> Self {
        Error::R2D2Sqlite(value)
    }
}

static CREATE_AGGREGATE_OVERVIEW_TABLE_STMT: &'static str = "CREATE TABLE aggregate_index(
                aggregate_id TEXT PRIMARY KEY,
                type_name TEXT,
                version INTEGER
            )";

static CREATE_AGGREGATE_TABLE_STMT: &'static str = "CREATE TABLE eventstore(
                aggregate_id TEXT,
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
        for qry in vec![
            CREATE_AGGREGATE_TABLE_STMT,
            CREATE_AGGREGATE_OVERVIEW_TABLE_STMT,
        ] {
            self.pool.get()?.execute(qry, params![])?;
        }
        return Ok(());
    }

    #[instrument]
    fn init_indices(&self) -> Result<(), Error> {
        self.pool.get()?.execute(
            "CREATE INDEX IF NOT EXISTS eventstore_agg_id_idx ON eventstore (aggregate_id)",
            params![],
        )?;
        return Ok(());
    }

    #[instrument]
    pub fn get_agg_max_version(&self, tx: &Transaction, agg_id_str: &str) -> Result<u32, Error> {
        let mut stmt = tx
            .prepare("SELECT COALESCE(MAX(version), 0) as max_version FROM aggregate_index WHERE aggregate_id = ?")?;
        let version = stmt.query_row(params![agg_id_str], |row| match row.get(0) {
            Ok(val) => Ok(val),
            Err(err) => {
                warn!(sqlite_error = err.to_string());
                Err(err)
            }
        })?;
        debug!(current_event_version = version);
        Ok(version)
    }

    #[instrument]
    pub fn append_event(&self, event: &Event) -> Result<(), Error> {
        let mut conn = self.pool.get()?;
        let tx = match conn.transaction() {
            Ok(tx) => tx,
            Err(err) => {
                warn!(sqlite_error = err.to_string());
                return Err(Error::Sqlite(err));
            }
        };
        let version = match self.get_agg_max_version(&tx, &event.id.to_string()) {
            Ok(version) => version,
            Err(err) => {
                return Err(err);
            }
        };
        let expected_version = version + 1;
        if event.version != expected_version {
            warn!("version mismtach {} != {}", event.version, expected_version);
            return Err(Error::WithMsg("version mismtach".to_string()));
        }
        let res = tx.execute(
            "INSERT INTO eventstore(aggregate_id, version, data) VALUES(?,?,?)",
            params![&event.id.to_string(), event.version, event.data],
        );
        if let Err(err) = res {
            warn!(sqlite_error = err.to_string());
            return Err(Error::Sqlite(err));
        }
        let res = tx.execute(
            "INSERT INTO aggregate_index(version, aggregate_id, type_name) VALUES(?,?, 'todo_implement_type_name')
                ON CONFLICT(aggregate_id) DO UPDATE SET version = ?",
            params![event.version, &event.id.to_string(), event.version],
        );
        match res {
            Ok(_) => match tx.commit() {
                Ok(_) => Ok(()),
                Err(err) => {
                    warn!(sqlite_error = err.to_string());
                    Err(Error::Sqlite(err))
                }
            },
            Err(err) => {
                warn!(sqlite_error = err.to_string());
                Err(Error::Sqlite(err))
            }
        }
    }

    #[instrument]
    fn result_from_stmt(stmt: &mut Statement, agg_id_str: &str) -> Result<Vec<Event>, Error> {
        let mut events: Vec<_> = Vec::new();
        let query_res = stmt.query_and_then(params![agg_id_str], |r| {
            let id = if let Ok(tmp) = r.get::<_, String>(0) {
                match uuid::Uuid::parse_str(tmp.as_str()) {
                    Ok(id) => id,
                    Err(_) => return Err(Error::InvalidUUID),
                }
            } else {
                return Err(Error::WithMsg("could not read uuid from row".to_string()));
            };
            Ok(Event {
                id,
                data: r.get(1)?,
                version: r.get(2)?,
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
                Err(Error::Sqlite(err))
            }
        }
    }

    #[instrument]
    pub fn get_aggretate(&self, aggregate_id: Uuid) -> Result<Vec<Event>, Error> {
        let agg_id_str: String = aggregate_id.to_string();
        let conn = self.pool.get()?;
        let mut stmt =
            conn.prepare("SELECT * FROM eventstore WHERE aggregate_id = ? ORDER BY version ASC")?;
        SqliteBackend::result_from_stmt(&mut stmt, &agg_id_str)
    }
}
