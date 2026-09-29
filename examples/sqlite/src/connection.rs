//! The live SQLite file is a view of committed history, never recovery authority.
use std::path::Path;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use rusqlite::{
    Connection, OpenFlags,
    hooks::{AuthAction, AuthContext, Authorization},
};

pub(crate) type Statement = (String, Vec<rusqlite::types::Value>);
pub(crate) type QueryRows = (Vec<String>, Vec<Vec<rusqlite::types::Value>>);

#[derive(Default)]
pub(crate) struct SqliteConnection {
    connection: Option<Connection>,
    internal_transaction: Arc<AtomicBool>,
    pub visible_lsn: i64,
}

impl SqliteConnection {
    pub fn open(&mut self, path: &Path, vfs: &str, committed_lsn: i64) -> rusqlite::Result<()> {
        self.close();
        let connection = Connection::open_with_flags_and_vfs(
            path,
            OpenFlags::SQLITE_OPEN_READ_WRITE
                | OpenFlags::SQLITE_OPEN_CREATE
                | OpenFlags::SQLITE_OPEN_NO_MUTEX,
            vfs,
        )?;
        connection.execute_batch("PRAGMA locking_mode=EXCLUSIVE; PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA wal_autocheckpoint=0;")?;
        let internal = self.internal_transaction.clone();
        // Clients cannot bypass the WAL barrier with another database, journal
        // mode, temporary tables, or a transaction spanning multiple RPCs.
        connection.authorizer(Some(move |context: AuthContext<'_>| {
            if context.database_name.is_some_and(|name| name != "main") {
                return Authorization::Deny;
            }
            match context.action {
                AuthAction::Attach { .. }
                | AuthAction::Detach { .. }
                | AuthAction::CreateVtable { .. }
                | AuthAction::DropVtable { .. } => Authorization::Deny,
                AuthAction::Transaction { .. } | AuthAction::Savepoint { .. } => {
                    if internal.load(Ordering::SeqCst) {
                        Authorization::Allow
                    } else {
                        Authorization::Deny
                    }
                }
                AuthAction::Pragma {
                    pragma_name,
                    pragma_value,
                } => {
                    if matches!(
                        pragma_name.to_ascii_lowercase().as_str(),
                        "table_info"
                            | "table_xinfo"
                            | "index_info"
                            | "foreign_key_list"
                            | "integrity_check"
                            | "quick_check"
                    ) || (pragma_value.is_none()
                        && matches!(
                            pragma_name.to_ascii_lowercase().as_str(),
                            "page_count" | "page_size" | "freelist_count"
                        ))
                    {
                        Authorization::Allow
                    } else {
                        Authorization::Deny
                    }
                }
                _ => Authorization::Allow,
            }
        }))?;
        self.connection = Some(connection);
        self.visible_lsn = committed_lsn;
        Ok(())
    }

    pub fn is_open(&self) -> bool {
        self.connection.is_some()
    }

    pub fn close(&mut self) {
        self.connection.take();
    }

    pub fn execute(&mut self, statements: &[Statement]) -> rusqlite::Result<(Vec<usize>, i64)> {
        let connection = self
            .connection
            .as_mut()
            .ok_or(rusqlite::Error::InvalidQuery)?;
        self.internal_transaction.store(true, Ordering::SeqCst);
        let transaction = connection.transaction();
        self.internal_transaction.store(false, Ordering::SeqCst);
        let transaction = transaction?;
        let results = statements
            .iter()
            .map(|(sql, params)| transaction.execute(sql, rusqlite::params_from_iter(params)))
            .collect::<rusqlite::Result<Vec<_>>>();
        let rowid = transaction.last_insert_rowid();
        self.internal_transaction.store(true, Ordering::SeqCst);
        let result = match results {
            Ok(results) => transaction.commit().map(|()| (results, rowid)),
            Err(error) => {
                let _ = transaction.rollback();
                Err(error)
            }
        };
        self.internal_transaction.store(false, Ordering::SeqCst);
        result
    }

    pub fn query(
        &self,
        sql: &str,
        params: &[rusqlite::types::Value],
    ) -> rusqlite::Result<QueryRows> {
        let connection = self
            .connection
            .as_ref()
            .ok_or(rusqlite::Error::InvalidQuery)?;
        let mut statement = connection.prepare(sql)?;
        if !statement.readonly() {
            return Err(rusqlite::Error::InvalidQuery);
        }
        let columns = statement
            .column_names()
            .iter()
            .map(|name| (*name).to_owned())
            .collect();
        let count = statement.column_count();
        let rows = statement
            .query_map(rusqlite::params_from_iter(params), |row| {
                (0..count)
                    .map(|index| row.get(index))
                    .collect::<rusqlite::Result<Vec<_>>>()
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok((columns, rows))
    }
}
