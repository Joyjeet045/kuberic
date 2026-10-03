use crate::config::PgConfig;
use crate::instance::{PgError, PgInstanceManager};
use std::sync::atomic::Ordering;
use std::time::Duration;

pub const APPLICATION_ROLE: &str = "kuberic_app";
pub const APPLICATION_DATABASE: &str = "kuberic";
pub(crate) const CLOSED: u8 = 0;
const GRANTING: u8 = 1;
const GRANTED: u8 = 2;

pub struct PgAccessController<'a> {
    instance: &'a PgInstanceManager,
}

impl<'a> PgAccessController<'a> {
    pub fn new(instance: &'a PgInstanceManager) -> Self {
        Self { instance }
    }

    pub async fn close_external(&self) -> Result<(), PgError> {
        let _access = self.instance.access_lock.lock().await;
        if self.instance.access_state.load(Ordering::Acquire) != CLOSED {
            self.instance.advance_access_generation()?;
            // A pre-authentication backend can retain the HBA rules inherited at
            // fork and is not yet terminable via pg_stat_activity. Fast shutdown
            // drains every socket; the owned restart installs closed HBA rules
            // before accepting anything, including replication/admin reconnects.
            self.instance.restart_access_closed().await?;
        }
        Ok(())
    }

    pub async fn grant_role_access(&self) -> Result<(), PgError> {
        let _access = self.instance.access_lock.lock().await;
        if !self.instance.is_running().await {
            return Err(PgError::Process(
                "cannot grant access to a stopped PostgreSQL run".into(),
            ));
        }
        if self.instance.access_state.load(Ordering::Acquire) == GRANTED {
            return Ok(());
        }
        self.instance.advance_access_generation()?;
        // A cancelled or failed grant still requires a full fence on the next
        // close, even if the reload acknowledgement has not completed.
        self.instance
            .access_state
            .store(GRANTING, Ordering::Release);
        let granted = self.grant_and_confirm().await;
        match granted {
            Ok(()) => {
                self.instance.access_state.store(GRANTED, Ordering::Release);
                Ok(())
            }
            Err(error) => Err(error.with_cleanup(self.instance.restart_access_closed().await)),
        }
    }

    async fn grant_and_confirm(&self) -> Result<(), PgError> {
        PgConfig::write_access_rules(
            self.instance.data_dir(),
            true,
            APPLICATION_ROLE,
            APPLICATION_DATABASE,
        )
        .await?;
        let (client, _connection) = self.instance.connect().await?;
        client
            .query_one("SELECT pg_reload_conf()", &[])
            .await
            .map_err(|error| PgError::Query(format!("reload granted access rules: {error}")))?;
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if let Ok((probe, connection)) = self.instance.connect_application().await {
                    drop(probe);
                    connection.await.map_err(|error| {
                        PgError::Connection(format!("join access probe: {error}"))
                    })?;
                    return Ok::<_, PgError>(());
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .map_err(|_| {
            PgError::Connection("application access reload was not acknowledged".into())
        })??;
        Ok(())
    }
}

#[cfg(all(test, feature = "testing"))]
mod tests {
    use super::*;
    use crate::testing::{ProcessProbe, TestDataDir, allocate_port, find_pg_bin};
    use std::future::{Future, poll_fn};
    use std::sync::{Arc, Mutex, atomic::AtomicBool, mpsc};
    use std::task::Poll;

    #[test]
    fn cancelled_hba_grant_cannot_overwrite_successor_closed_generation() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let grant_runtime = tokio::runtime::Builder::new_current_thread()
            .max_blocking_threads(1)
            .enable_all()
            .build()
            .unwrap();
        let directory = TestDataDir::new("cancel-hba-write");
        let instance = PgInstanceManager::new(
            directory.path().join("pgdata"),
            find_pg_bin(),
            runtime.block_on(allocate_port()),
        );
        runtime.block_on(async {
            instance.init_db().await.unwrap();
            let (faults, _faults) = tokio::sync::mpsc::channel(8);
            instance.start_native(faults).await.unwrap();
            initialize_application_role(&instance).await.unwrap();
        });
        let access = PgAccessController::new(&instance);
        let old = ProcessProbe::postgres(instance.data_dir());
        let hba = instance.data_dir().join("pg_hba.conf");
        let (release, released) = mpsc::channel();
        let released = Mutex::new(Some(released));
        let writing = Arc::new(AtomicBool::new(false));
        let before_write = {
            let writing = writing.clone();
            Box::new(move || {
                let released = released.lock().unwrap().take().unwrap();
                let (entered, entered_rx) = mpsc::channel();
                tokio::task::spawn_blocking(move || {
                    entered.send(()).unwrap();
                    let _ = released.recv_timeout(Duration::from_secs(60));
                });
                entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
                writing.store(true, Ordering::Release);
            }) as Box<dyn Fn() + Send + Sync>
        };

        // Hold the sole filesystem worker only after the real HBA read. Poll
        // through submission of the write, then drop the grant, not just its
        // GRANTING flag. An async writer would remain queued behind this gate;
        // the synchronous replacement instead completes in the same poll.
        grant_runtime.block_on(async {
            let grant = crate::config::BEFORE_ACCESS_RULES_WRITE
                .scope(before_write, access.grant_role_access());
            tokio::pin!(grant);
            tokio::time::timeout(
                Duration::from_secs(5),
                poll_fn(|context| {
                    assert!(grant.as_mut().poll(context).is_pending());
                    if writing.load(Ordering::Acquire) {
                        Poll::Ready(())
                    } else {
                        Poll::Pending
                    }
                }),
            )
            .await
            .unwrap();
        });
        assert_eq!(instance.access_state.load(Ordering::Acquire), GRANTING);
        runtime.block_on(async {
            tokio::time::timeout(Duration::from_secs(15), access.close_external())
                .await
                .unwrap()
                .unwrap();
            old.assert_reaped();
            assert_eq!(instance.access_state.load(Ordering::Acquire), CLOSED);
            assert_application_hba_rejected(&instance).await;
        });
        let closed = std::fs::read_to_string(&hba).unwrap();
        assert!(!closed.contains(APPLICATION_ROLE));

        release.send(()).unwrap();
        // Runtime destruction joins all blocking jobs, including an abandoned
        // tokio::fs write. Check the successor only after that stale work drains.
        drop(grant_runtime);
        assert_eq!(
            std::fs::read_to_string(&hba).unwrap(),
            closed,
            "cancelled grant rewrote the successor's closed HBA"
        );
        assert!(!hba.with_extension("conf.pending").exists());
        runtime.block_on(async {
            let replacement = ProcessProbe::postgres(instance.data_dir());
            let (client, connection) = instance.connect().await.unwrap();
            let loaded: String = client
                .query_one("SELECT pg_conf_load_time()::text", &[])
                .await
                .unwrap()
                .get(0);
            assert!(
                client
                    .query_one("SELECT pg_reload_conf()", &[])
                    .await
                    .unwrap()
                    .get::<_, bool>(0)
            );
            tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    let current: String = client
                        .query_one("SELECT pg_conf_load_time()::text", &[])
                        .await
                        .unwrap()
                        .get(0);
                    if current != loaded {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            assert_application_hba_rejected(&instance).await;
            drop(client);
            connection.await.unwrap();
            instance.stop().await.unwrap();
            replacement.assert_reaped();
        });
    }

    async fn assert_application_hba_rejected(instance: &PgInstanceManager) {
        let error = tokio_postgres::connect(
            &instance.application_connection_string(),
            tokio_postgres::NoTls,
        )
        .await
        .err()
        .expect("ordinary SQL must remain closed");
        let database = error
            .as_db_error()
            .expect("server authentication rejection");
        assert_eq!(
            database.code(),
            &tokio_postgres::error::SqlState::INVALID_AUTHORIZATION_SPECIFICATION
        );
        assert!(
            database
                .message()
                .contains("pg_hba.conf rejects connection")
        );
    }

    #[tokio::test]
    async fn cancelled_grant_remains_fence_required() {
        let directory = TestDataDir::new("cancel-grant");
        let instance = PgInstanceManager::new(
            directory.path().join("pgdata"),
            find_pg_bin(),
            allocate_port().await,
        );
        instance.init_db().await.unwrap();
        let (faults, _faults) = tokio::sync::mpsc::channel(8);
        instance.start_native(faults).await.unwrap();
        initialize_application_role(&instance).await.unwrap();
        let access = PgAccessController::new(&instance);
        for _ in 0..4 {
            let old = ProcessProbe::postgres(instance.data_dir());
            {
                let grant = access.grant_role_access();
                tokio::pin!(grant);
                tokio::select! {
                    biased;
                    result = &mut grant => panic!("grant completed before cancellation: {result:?}"),
                    _ = async {
                        while instance.access_state.load(Ordering::Acquire) != GRANTING {
                            tokio::task::yield_now().await;
                        }
                    } => {}
                }
            }
            assert_eq!(instance.access_state.load(Ordering::Acquire), GRANTING);
            access.close_external().await.unwrap();
            old.assert_reaped();
            assert_eq!(instance.access_state.load(Ordering::Acquire), CLOSED);
            assert!(instance.connect_application().await.is_err());
        }
        instance.stop().await.unwrap();
    }
}

pub async fn initialize_application_role(instance: &PgInstanceManager) -> Result<(), PgError> {
    let (client, _connection) = instance.connect().await?;
    client
        .batch_execute(&format!(
            "DO $$ BEGIN \
                 IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '{role}') THEN \
                   CREATE ROLE {role} LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION; \
                 END IF; \
                 IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'kuberic_rewind') THEN \
                   CREATE ROLE kuberic_rewind LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION; \
                 END IF; \
               END $$; \
               GRANT pg_read_server_files TO kuberic_rewind; \
               GRANT EXECUTE ON FUNCTION pg_catalog.pg_read_binary_file(text) TO kuberic_rewind; \
               GRANT EXECUTE ON FUNCTION pg_catalog.pg_read_binary_file(text,bigint,bigint,boolean) TO kuberic_rewind; \
               GRANT EXECUTE ON FUNCTION pg_catalog.pg_stat_file(text,boolean) TO kuberic_rewind; \
               GRANT EXECUTE ON FUNCTION pg_catalog.pg_ls_dir(text,boolean,boolean) TO kuberic_rewind; \
               ALTER ROLE {role} SET synchronous_commit = 'remote_apply';",
            role = APPLICATION_ROLE,
        ))
        .await
        .map_err(|error| PgError::Query(format!("create application role: {error}")))?;
    let exists: bool = client
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = $1)",
            &[&APPLICATION_DATABASE],
        )
        .await
        .map_err(|error| PgError::Query(format!("query application database: {error}")))?
        .get(0);
    if !exists {
        client
            .batch_execute(&format!(
                "CREATE DATABASE {database} OWNER {role}",
                database = APPLICATION_DATABASE,
                role = APPLICATION_ROLE,
            ))
            .await
            .map_err(|error| PgError::Query(format!("create application database: {error}")))?;
    }
    Ok(())
}

pub async fn application_role_uses_remote_apply(
    instance: &PgInstanceManager,
) -> Result<bool, PgError> {
    let (client, _connection) = instance.connect().await?;
    let setting: Option<Vec<String>> = client
        .query_one(
            "SELECT rolconfig FROM pg_roles WHERE rolname = $1",
            &[&APPLICATION_ROLE],
        )
        .await
        .map_err(|error| PgError::Query(format!("query application role: {error}")))?
        .get(0);
    Ok(setting.is_some_and(|settings| {
        settings
            .iter()
            .any(|setting| setting == "synchronous_commit=remote_apply")
    }))
}

pub async fn stop_fence(instance: &PgInstanceManager) -> Result<(), PgError> {
    instance.stop().await?;
    if instance.is_running().await {
        return Err(PgError::Process(
            "PostgreSQL remained running after completed stop fence".into(),
        ));
    }
    Ok(())
}
