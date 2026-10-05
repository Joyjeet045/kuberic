use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::{Mutex, OnceLock};

use crate::{PgService, PgServiceConfig};
use kuberic_agent::{
    hosting::PodRuntime,
    runtime_adapter::RuntimeAdapter,
    service::AgentService,
    sqlite_store::SqliteStore,
    state::{AgentState, SCHEMA_VERSION, StorageIdentity},
    store::AgentStore,
};
use kuberic_protocol::types::{
    AccessStatus, AgentGeneration, BuildAuthority, ConfigurationDescriptor, ConfigurationMember,
    EffectivePolicy, Epoch, InitializationId, OperationId, PodUid, ProcessSessionId, PvcUid,
    ReplicaId, ReplicaIdentity, ReplicaInstanceId, ReplicaRole, ResourceUid,
};
use kuberic_runtime::StatefulServiceReplica;
use kuberic_runtime::application::OpenMode;
use kuberic_runtime::replicator::ReplicaInformation;
use kuberic_runtime::replicator::copy::BuildConfiguration;
use kuberic_runtime_internal::authority::AdmittedAuthority;
use kuberic_runtime_internal::effects::{RuntimeEffect, RuntimeEffectAction};
use std::sync::Arc;

const NATIVE_TOKEN: &str = "host-local-native-build";

#[path = "testing/group.rs"]
mod group;
#[cfg(test)]
pub(crate) mod layout;
pub use group::{
    AdmissionCut, PgGroup, PgSqlSession, RestartPart, definitive_fence_error, run_pg_test,
};

pub fn native_identity(id: i64, incarnation: &str) -> ReplicaIdentity {
    ReplicaIdentity {
        replica_id: ReplicaId::new(id),
        instance_id: ReplicaInstanceId::new(incarnation),
        agent_generation: AgentGeneration::new(format!("generation-{incarnation}")),
    }
}

pub fn native_configuration(
    members: &[ReplicaIdentity],
    primary: usize,
    epoch: i64,
) -> ConfigurationDescriptor {
    ConfigurationDescriptor::new(
        Epoch::new(0, epoch),
        members[primary].replica_id,
        members
            .iter()
            .enumerate()
            .map(|(index, identity)| ConfigurationMember {
                identity: identity.clone(),
                role: if index == primary {
                    ReplicaRole::Primary
                } else {
                    ReplicaRole::ActiveSecondary
                },
            })
            .collect(),
        members.len() as u32 / 2 + 1,
    )
}

pub struct PgPod {
    pub runtime: Arc<PodRuntime>,
    pub application: Arc<PgService>,
    pub store: Arc<SqliteStore>,
    pub identity: ReplicaIdentity,
    pub session: ProcessSessionId,
    agent: AgentService<SqliteStore, PodRuntime>,
    control_server: Option<tokio::task::JoinHandle<kuberic_agent::Result<()>>>,
    pub root: PathBuf,
    pub endpoint: String,
    server: tokio::task::JoinHandle<()>,
}

impl PgPod {
    pub async fn new(root: PathBuf, identity: ReplicaIdentity) -> Self {
        Self::with_bin(root, identity, find_pg_bin()).await
    }

    pub async fn with_bin(root: PathBuf, identity: ReplicaIdentity, pg_bin: PathBuf) -> Self {
        Self::create(root, identity, pg_bin, true).await
    }

    async fn create(
        root: PathBuf,
        identity: ReplicaIdentity,
        pg_bin: PathBuf,
        fixture_reconstruction: bool,
    ) -> Self {
        let path = SqliteStore::metadata_database_path(&root);
        let existing = path.exists();
        let store = Arc::new(if existing {
            SqliteStore::open_existing(path, None).unwrap()
        } else {
            SqliteStore::create_authorized(
                path,
                AgentState::new(StorageIdentity {
                    schema_version: SCHEMA_VERSION,
                    resource_uid: ResourceUid::new("postgres-native-test"),
                    local_identity: identity.clone(),
                    pod_uid: PodUid::new(identity.instance_id.as_str()),
                    pvc_uid: PvcUid::new(format!("pvc-{}", identity.instance_id)),
                    initialization_id: InitializationId::new(format!(
                        "init-{}",
                        identity.instance_id
                    )),
                    effective_policy: EffectivePolicy::fixed(1, 30).unwrap(),
                }),
            )
            .unwrap()
        });
        assert_eq!(
            store.load_state().await.unwrap().identity.local_identity,
            identity
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let application = Arc::new(
            PgService::deferred(PgServiceConfig {
                resource_uid: ResourceUid::new("postgres-native-test"),
                application_root: root.join("application"),
                pg_data: root.join("pgdata"),
                pg_bin,
                pg_port: allocate_port().await,
                replication_address: endpoint.clone(),
            })
            .with_coordination_token(NATIVE_TOKEN.into()),
        );
        let runtime = Arc::new(PodRuntime::new(
            identity.clone(),
            application.clone(),
            store.clone(),
        ));
        let agent = AgentService::new(
            store.clone(),
            runtime.clone(),
            runtime.clone(),
            NATIVE_TOKEN,
        )
        .unwrap();
        let session = agent.sessions().local_session().clone();
        runtime
            .bind_replica_session(ResourceUid::new("postgres-native-test"), session.clone())
            .unwrap();
        let service =
            crate::data_service::PgDataServiceImpl::new(application.clone(), NATIVE_TOKEN.into());
        let server = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(service.into_server())
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                .await
                .unwrap();
        });
        let pod = Self {
            runtime,
            application,
            store,
            identity,
            session,
            agent,
            control_server: None,
            root,
            endpoint,
            server,
        };
        if !fixture_reconstruction {
            return pod;
        } else if existing {
            let state = pod.store.load_state().await.unwrap();
            pod.runtime
                .reconstruct(
                    OpenMode::Existing,
                    state.role,
                    AccessStatus::ReconfigurationPending,
                    AccessStatus::ReconfigurationPending,
                    None,
                )
                .await
                .unwrap();
        } else {
            pod.effect(RuntimeEffectAction::Open(OpenMode::New))
                .await
                .unwrap();
        }
        pod
    }

    pub async fn start_control(&mut self) -> std::net::SocketAddr {
        assert!(self.control_server.is_none());
        let control = format!("127.0.0.1:{}", allocate_port().await)
            .parse()
            .unwrap();
        let replication = format!("127.0.0.1:{}", allocate_port().await)
            .parse()
            .unwrap();
        let (ready, mut ready_rx) = tokio::sync::watch::channel(false);
        let (shutdown, stopped) = tokio::sync::watch::channel(false);
        let agent = self.agent.clone();
        self.control_server = Some(tokio::spawn(async move {
            let _shutdown = shutdown;
            agent.serve(control, replication, ready, stopped).await
        }));
        tokio::time::timeout(
            std::time::Duration::from_secs(10),
            ready_rx.wait_for(|ready| *ready),
        )
        .await
        .unwrap()
        .unwrap();
        control
    }

    pub async fn dispatch_native(&self, target: &Self, control: std::net::SocketAddr, id: &str) {
        use kuberic_agent::transport::{
            GrpcOutboundDispatcher, OutboundDispatcher, QueuedOutbound, ReliableTransport,
        };
        let resolver = Arc::new(NativeRoute {
            identity: target.identity.clone(),
            control,
        });
        let dispatcher = GrpcOutboundDispatcher::new(
            self.runtime.clone(),
            Arc::new(tokio::sync::Mutex::new(
                ReliableTransport::new(self.session.clone(), 16).unwrap(),
            )),
            resolver,
            "postgres-native-test",
            NATIVE_TOKEN,
            std::time::Duration::from_secs(10),
        )
        .unwrap();
        self.refresh().await;
        let build = self.effect(RuntimeEffectAction::BuildReplica {
            build_id: OperationId::new(id),
            target: target.identity.clone(),
            replication_address: String::new(),
        });
        tokio::pin!(build);
        let data_plane = self.runtime.data_plane();
        let outbound = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            tokio::select! {
                result = &mut build => panic!("build completed before dispatch: {result:?}"),
                outbound = data_plane.next_outbound() => outbound,
            }
        })
        .await
        .unwrap()
        .unwrap();
        let kuberic_agent::hosting::OutboundReplication::Build(endpoint) = outbound else {
            panic!("expected custom build");
        };
        tokio::time::timeout(
            std::time::Duration::from_secs(70),
            dispatcher.dispatch(QueuedOutbound::Build(endpoint)),
        )
        .await
        .unwrap()
        .unwrap();
        build.await.unwrap();
    }

    pub async fn effect(&self, action: RuntimeEffectAction) -> kuberic_agent::Result<()> {
        let sequence = self.store.load_state().await?.next_effect_sequence;
        self.effect_as(
            OperationId::new(format!("native-effect-{sequence}")),
            action,
        )
        .await
    }

    pub async fn effect_as(
        &self,
        operation_id: OperationId,
        action: RuntimeEffectAction,
    ) -> kuberic_agent::Result<()> {
        let sequence = self.store.load_state().await?.next_effect_sequence;
        RuntimeAdapter::new(self.store.clone(), self.runtime.clone())
            .execute(RuntimeEffect {
                operation_id,
                sequence,
                action,
            })
            .await?;
        Ok(())
    }

    pub async fn shutdown(&mut self) -> kuberic_runtime::Result<()> {
        self.runtime.abort();
        let result = self.application.abort_and_wait();
        self.server.abort();
        let mut server_errors = Vec::new();
        if let Err(error) = (&mut self.server).await
            && !error.is_cancelled()
        {
            server_errors.push(error.to_string());
        }
        if let Some(server) = self.control_server.take() {
            server.abort();
            match server.await {
                Ok(Ok(())) => {}
                Ok(Err(error)) => server_errors.push(error.to_string()),
                Err(error) if error.is_cancelled() => {}
                Err(error) => server_errors.push(error.to_string()),
            }
        }
        if server_errors.is_empty() {
            result
        } else {
            Err(kuberic_runtime::RuntimeError::Application(format!(
                "fixture server cleanup: {server_errors:?}; process cleanup: {result:?}"
            )))
        }
    }

    pub async fn restart_application(&self) -> Result<(), crate::instance::PgError> {
        self.application.instance().restart_access_closed().await
    }

    pub async fn disconnect_receiver(&self) -> Result<(), crate::instance::PgError> {
        self.application.instance().stop().await?;
        self.application
            .instance()
            .config()
            .disconnect_receiver(self.application.instance().data_dir())
            .await?;
        self.application.instance().restart_access_closed().await
    }

    pub async fn singleton(&self) {
        self.admit(native_configuration(
            std::slice::from_ref(&self.identity),
            0,
            1,
        ))
        .await;
        self.effect(RuntimeEffectAction::ChangeRole(ReplicaRole::Primary))
            .await
            .unwrap();
        self.effect(RuntimeEffectAction::SetAccessStatus {
            read: AccessStatus::Granted,
            write: AccessStatus::Granted,
        })
        .await
        .unwrap();
    }

    pub async fn admit(&self, configuration: ConfigurationDescriptor) {
        self.effect(RuntimeEffectAction::AdmitAuthority(Box::new(
            AdmittedAuthority {
                local_identity: self.identity.clone(),
                current_configuration: configuration,
                previous_configuration: None,
                transition_kind: None,
                switchover_handoff: None,
                secondary_removal: None,
                scale_up: None,
            },
        )))
        .await
        .unwrap();
    }

    pub async fn refresh(&self) {
        self.effect(RuntimeEffectAction::RefreshApplicationProgress)
            .await
            .unwrap();
    }

    pub async fn peer(&self, other: &Self) {
        self.effect(RuntimeEffectAction::RegisterPeerSession {
            identity: other.identity.clone(),
            session: other.session.clone(),
        })
        .await
        .unwrap();
        let mut description = ReplicaInformation::new(
            OperationId::default(),
            other.identity.clone(),
            other.endpoint.clone(),
        );
        description.process_session_id = other.session.clone();
        kuberic_agent::testing::describe_peer(&self.runtime, description)
            .await
            .unwrap();
    }

    pub async fn authorize(&self, target: &Self, id: &str) -> BuildAuthority {
        self.refresh().await;
        let authority = self
            .runtime
            .authorize_build(
                OperationId::new(id),
                target.identity.clone(),
                BuildConfiguration::Current,
            )
            .await
            .unwrap();
        self.peer(target).await;
        target.peer(self).await;
        if target.runtime.snapshot().await.role != ReplicaRole::IdleSecondary {
            target
                .effect(RuntimeEffectAction::ChangeRole(ReplicaRole::IdleSecondary))
                .await
                .unwrap();
        }
        target
            .store
            .journal_build(&kuberic_protocol::command::EnsureReplicaBuild {
                operation_id: authority.build_id.clone(),
                local_replica_id: target.identity.replica_id,
                expected_instance_id: target.identity.instance_id.clone(),
                expected_agent_generation: target.identity.agent_generation.clone(),
                target: target.identity.clone(),
                authority: Some(authority.clone()),
                source_session_id: Some(self.session.clone()),
                retire: false,
            })
            .await
            .unwrap();
        target
            .effect(RuntimeEffectAction::AdmitBuildAuthority(Box::new(
                authority.clone(),
            )))
            .await
            .unwrap();
        authority
    }

    pub async fn build(
        &self,
        target: &Self,
        authority: &BuildAuthority,
    ) -> kuberic_runtime::Result<()> {
        tokio::time::timeout(
            std::time::Duration::from_secs(70),
            kuberic_agent::testing::execute_build(
                &self.runtime,
                ReplicaInformation::new(
                    authority.build_id.clone(),
                    target.identity.clone(),
                    target.endpoint.clone(),
                ),
            ),
        )
        .await
        .expect("native build deadline")?;
        Ok(())
    }

    pub async fn reopen(self) -> Self {
        let identity = self.identity.clone();
        let root = self.root.clone();
        let old = self.session.clone();
        self.runtime.abort();
        drop(self);
        let pod = Self::new(root, identity).await;
        assert_ne!(pod.session, old);
        pod
    }

    pub async fn reopen_with_agent(self) -> (Self, std::net::SocketAddr) {
        let identity = self.identity.clone();
        let root = self.root.clone();
        let old = self.session.clone();
        self.runtime.abort();
        drop(self);
        let mut pod = Self::create(root, identity, find_pg_bin(), false).await;
        assert_ne!(pod.session, old);
        let address = pod.start_control().await;
        (pod, address)
    }

    pub async fn status(&self) -> kuberic_agent::Result<kuberic_wire::proto::AgentStatusReport> {
        kuberic_agent::report::AgentReporter::new(self.store.clone())
            .report(&self.runtime)
            .await
    }

    /// Models pre-existing promoted storage; promotion orchestration is deliberately
    /// not part of the Phase 4 driver.
    pub async fn promoted_fixture(self) -> Self {
        self.application.instance().promote().await.unwrap();
        let (sql, _) = self.application.instance().connect().await.unwrap();
        sql.batch_execute("CHECKPOINT").await.unwrap();
        drop(sql);
        let snapshot = crate::native::PgNativeObserver::new(self.application.instance().clone())
            .snapshot()
            .await
            .unwrap();
        self.application.instance().stop().await.unwrap();
        let identity = self.identity.clone();
        let root = self.root.clone();
        drop(self);
        let durable = crate::durable::PgDurableStore::open(
            root.join("application"),
            crate::durable::PgDurableIdentity {
                resource_uid: ResourceUid::new("postgres-native-test"),
                replica: identity.clone(),
            },
            crate::durable::StorageMode::Established,
        )
        .await
        .unwrap();
        let evidence = snapshot.evidence.unwrap();
        let digest = crate::native::timeline_history_digest(&root.join("pgdata"), &evidence)
            .await
            .unwrap();
        durable
            .update(|state| {
                state.native_build = None;
                state.accepted_build = None;
                state.has_accepted_authority = true;
                state.recovery_state = crate::durable::PgRecoveryState::Rebuilding;
                state.role = crate::durable::PgDurableRole::Primary;
                state.system_identifier = Some(evidence.system_identifier.clone());
                state.timeline_id = Some(evidence.timeline_id);
                state.timeline_history_digest = Some(digest);
                state.current_lsn = snapshot.current_lsn;
                state.flush_lsn = evidence.flush_lsn;
                state.received_lsn = None;
                state.replay_lsn = None;
                state.policy_certified_lsn = evidence.flush_lsn;
                state.synchronous = None;
                state.postgres_stopped = true;
                state.external_access_closed = true;
                Ok(())
            })
            .await
            .unwrap();
        durable
            .update(|state| {
                state.recovery_state = crate::durable::PgRecoveryState::Ready;
                Ok(())
            })
            .await
            .unwrap();
        drop(durable);
        Self::new(root, identity).await
    }

    pub async fn inject(
        &self,
        request: &crate::build::PgBuildRequest,
    ) -> Result<crate::build::PgBuildProgress, tonic::Status> {
        let mut client = crate::proto::pg_data_service_client::PgDataServiceClient::connect(
            self.endpoint.clone(),
        )
        .await
        .unwrap();
        let mut message = tonic::Request::new(crate::proto::NativeBuildRequest {
            envelope_json: crate::build::encode(request).unwrap(),
        });
        message.metadata_mut().insert(
            "authorization",
            format!("Bearer {NATIVE_TOKEN}").parse().unwrap(),
        );
        let response =
            tokio::time::timeout(std::time::Duration::from_secs(70), client.build(message))
                .await
                .unwrap()?;
        Ok(crate::build::decode(&response.into_inner().progress_json).unwrap())
    }
}

struct NativeRoute {
    identity: ReplicaIdentity,
    control: std::net::SocketAddr,
}

impl kuberic_agent::transport::ReplicaEndpointResolver for NativeRoute {
    fn control_endpoint(&self, identity: &ReplicaIdentity) -> String {
        if identity == &self.identity {
            format!("http://{}", self.control)
        } else {
            String::new()
        }
    }
    fn replication_endpoint(&self, _: &ReplicaIdentity) -> String {
        panic!("native builds must not use the operation-stream endpoint");
    }
}

impl Drop for PgPod {
    fn drop(&mut self) {
        if let Some(server) = &self.control_server {
            server.abort();
        }
        self.runtime.abort();
        self.application.abort();
        self.server.abort();
    }
}

/// Keeps test process identities independent of PGDATA and cleans up on assertion failure.
pub struct ProcessProbe(Vec<(u32, std::os::fd::OwnedFd)>);

impl ProcessProbe {
    pub fn process(pid: u32) -> Self {
        use rustix::process::{Pid, PidfdFlags, pidfd_open};
        Self(vec![(
            pid,
            pidfd_open(Pid::from_raw(pid as i32).unwrap(), PidfdFlags::empty()).unwrap(),
        )])
    }

    pub fn postgres(data_dir: &std::path::Path) -> Self {
        use rustix::process::{Pid, PidfdFlags, pidfd_open};
        let pid = std::fs::read_to_string(data_dir.join("postmaster.pid"))
            .unwrap()
            .lines()
            .next()
            .unwrap()
            .parse::<u32>()
            .unwrap();
        let mut probe = Self::descendants(pid);
        probe.0.push((
            pid,
            pidfd_open(Pid::from_raw(pid as i32).unwrap(), PidfdFlags::empty()).unwrap(),
        ));
        probe
    }

    pub fn descendants(parent: u32) -> Self {
        use rustix::process::{Pid, PidfdFlags, pidfd_open};
        let mut processes = Vec::new();
        let mut parents = vec![parent];
        while let Some(parent) = parents.pop() {
            let Ok(tasks) = std::fs::read_dir(format!("/proc/{parent}/task")) else {
                continue;
            };
            for task in tasks.flatten() {
                let Ok(children) = std::fs::read_to_string(task.path().join("children")) else {
                    continue;
                };
                for child in children.split_whitespace() {
                    let pid = child.parse::<u32>().unwrap();
                    if processes.iter().any(|(known, _)| *known == pid) {
                        continue;
                    }
                    if let Ok(fd) =
                        pidfd_open(Pid::from_raw(pid as i32).unwrap(), PidfdFlags::empty())
                    {
                        processes.push((pid, fd));
                        parents.push(pid);
                    }
                }
            }
        }
        Self(processes)
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn signal(&self, signal: rustix::process::Signal) {
        for (_, fd) in &self.0 {
            match rustix::process::pidfd_send_signal(fd, signal) {
                Ok(()) | Err(rustix::io::Errno::SRCH) => {}
                Err(error) => panic!("signal owned test process: {error}"),
            }
        }
    }

    /// Only for an isolated subprocess test that deliberately adopts its orphans.
    pub fn reap_adopted(&self) -> std::io::Result<()> {
        use std::os::fd::AsFd;

        use rustix::event::{PollFd, PollFlags, Timespec, poll};
        use rustix::process::{Signal, WaitId, WaitIdOptions, pidfd_send_signal, waitid};
        for signal in [Signal::QUIT, Signal::KILL] {
            for (_, fd) in &self.0 {
                match pidfd_send_signal(fd, signal) {
                    Ok(()) | Err(rustix::io::Errno::SRCH) => {}
                    Err(error) => return Err(error.into()),
                }
            }
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(3);
            loop {
                let mut pending = false;
                for (_, fd) in &self.0 {
                    match waitid(
                        WaitId::PidFd(fd.as_fd()),
                        WaitIdOptions::EXITED | WaitIdOptions::NOHANG,
                    ) {
                        Ok(_) | Err(rustix::io::Errno::CHILD | rustix::io::Errno::INTR) => {}
                        Err(error) => return Err(error.into()),
                    }
                    let mut fds = [PollFd::new(fd, PollFlags::IN)];
                    poll(
                        &mut fds,
                        Some(&Timespec {
                            tv_sec: 0,
                            tv_nsec: 0,
                        }),
                    )?;
                    pending |= !fds[0].revents().contains(PollFlags::HUP);
                }
                if !pending {
                    return Ok(());
                }
                if std::time::Instant::now() >= deadline {
                    break;
                }
                std::thread::sleep(std::time::Duration::from_millis(20));
            }
        }
        Err(std::io::Error::other(
            "adopted test descendants were not reaped",
        ))
    }

    pub async fn assert_gone(&self) {
        self.wait_for_exit(true).await;
    }

    pub fn assert_reaped(&self) {
        use rustix::event::{PollFd, PollFlags, Timespec, poll};
        for (pid, fd) in &self.0 {
            let mut fds = [PollFd::new(fd, PollFlags::IN)];
            poll(
                &mut fds,
                Some(&Timespec {
                    tv_sec: 0,
                    tv_nsec: 0,
                }),
            )
            .unwrap();
            assert!(
                fds[0].revents().contains(PollFlags::HUP),
                "owned descendant {pid} was not reaped before successful completion"
            );
        }
    }

    pub async fn assert_exited(&self) {
        self.wait_for_exit(false).await;
    }

    async fn wait_for_exit(&self, reaped: bool) {
        use rustix::event::{PollFd, PollFlags, Timespec, poll};
        tokio::time::timeout(std::time::Duration::from_secs(3), async {
            loop {
                if self.0.iter().all(|(_, fd)| {
                    let mut fds = [PollFd::new(fd, PollFlags::IN)];
                    poll(
                        &mut fds,
                        Some(&Timespec {
                            tv_sec: 0,
                            tv_nsec: 0,
                        }),
                    )
                    .unwrap();
                    fds[0].revents().contains(PollFlags::IN)
                        && (!reaped || fds[0].revents().contains(PollFlags::HUP))
                }) {
                    return;
                }
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap_or_else(|_| {
            panic!(
                "owned descendants survived: {:?}",
                self.0.iter().map(|p| p.0).collect::<Vec<_>>()
            )
        });
    }
}

impl Drop for ProcessProbe {
    fn drop(&mut self) {
        use rustix::process::{Signal, pidfd_send_signal};
        for (_, fd) in &self.0 {
            let _ = pidfd_send_signal(fd, Signal::QUIT);
            let _ = pidfd_send_signal(fd, Signal::CONT);
        }
    }
}

pub fn wrapped_pg_bin(root: &std::path::Path, command: &str, script: &str) -> PathBuf {
    use std::os::unix::fs::{PermissionsExt, symlink};
    let wrappers = root.join("bin");
    std::fs::create_dir(&wrappers).unwrap();
    for entry in std::fs::read_dir(find_pg_bin()).unwrap() {
        let entry = entry.unwrap();
        symlink(entry.path(), wrappers.join(entry.file_name())).unwrap();
    }
    let wrapper = wrappers.join(command);
    std::fs::remove_file(&wrapper).unwrap();
    std::fs::write(&wrapper, script).unwrap();
    std::fs::set_permissions(&wrapper, std::fs::Permissions::from_mode(0o700)).unwrap();
    wrappers
}

/// Find the pg_bin directory from the system.
pub fn find_pg_bin() -> PathBuf {
    let candidates = [
        "/usr/lib/postgresql/16/bin",
        "/usr/lib/postgresql/17/bin",
        "/usr/lib/postgresql/15/bin",
        "/usr/pgsql-16/bin",
        "/usr/local/pgsql/bin",
    ];

    for path in &candidates {
        let p = PathBuf::from(path);
        if p.join("initdb").exists() {
            return p;
        }
    }

    // Fallback: try PATH
    if let Some(bindir) = std::process::Command::new("pg_config")
        .arg("--bindir")
        .output()
        .ok()
        .filter(|o| o.status.success())
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
        .filter(|s| !s.is_empty())
    {
        return PathBuf::from(bindir);
    }

    panic!("PostgreSQL binaries not found. Install postgresql or set --pg-bin.");
}

/// Create a temporary data directory for testing.
pub fn temp_data_dir(name: &str) -> PathBuf {
    static DIRECTORY_COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("target")
        .join("postgresql-v2-tests");
    std::fs::create_dir_all(&root).expect("create PostgreSQL test root");
    let root = std::fs::canonicalize(root).expect("resolve PostgreSQL test root");
    loop {
        let counter = DIRECTORY_COUNTER
            .fetch_update(
                std::sync::atomic::Ordering::Relaxed,
                std::sync::atomic::Ordering::Relaxed,
                |counter| (counter < 4096).then_some(counter + 1),
            )
            .expect("PostgreSQL fixture directory IDs exhausted");
        let dir = root.join(fixture_directory_id(std::process::id(), counter));
        match std::fs::create_dir(&dir) {
            Ok(()) => {
                tracing::debug!(name, path = ?dir, "created PostgreSQL fixture");
                return dir;
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => panic!("create PostgreSQL fixture {name}: {error}"),
        }
    }
}

fn fixture_directory_id(pid: u32, counter: u64) -> String {
    // Seven base-36 bytes leave room for the compact fixture layouts below.
    // Linux PIDs use at most 22 bits; reserve 12 disjoint bits for fixture IDs.
    assert!(pid < 1 << 22 && (1..4096).contains(&counter));
    let mut value = (u64::from(pid) << 12) | counter;
    let mut name = Vec::new();
    while value != 0 {
        name.push(b"0123456789abcdefghijklmnopqrstuvwxyz"[(value % 36) as usize] as char);
        value /= 36;
    }
    name.into_iter().rev().collect()
}

#[test]
fn compact_fixture_ids_are_unique_and_fit_hosted_ci_socket_paths() {
    let mut names = HashSet::new();
    for pid in [1, 1234, 1235, 1234567, 1234568, (1 << 22) - 1] {
        for counter in 1..4096 {
            let name = fixture_directory_id(pid, counter);
            let root =
                PathBuf::from("/home/runner/work/kuberic/kuberic/target/postgresql-v2-tests")
                    .join(&name);
            // Standalone instances, in-process hosts, SF library/executable hosts,
            // native-build peers and group incarnations all use these layouts.
            for data in [
                root.join("pgdata"),
                root.join("primary"),
                root.join("standby"),
                root.join(layout::SINGLE_REPLICA_DIRECTORY).join("pgdata"),
                root.join("p/pgdata"),
                root.join("s/pgdata"),
                root.join("t/pgdata"),
                root.join("f/pgdata"),
                root.join("o/pgdata"),
                root.join("old/pgdata"),
                root.join("new/pgdata"),
                root.join("r1/pgdata"),
                root.join("r3x2/pgdata"),
            ] {
                let config = crate::config::PgConfig::new(65535, &data);
                let socket = PathBuf::from(config.socket_dir).join(".s.PGSQL.65535");
                assert!(socket.as_os_str().len() <= 107, "{}", socket.display());
            }
            assert!(names.insert(name));
        }
    }
}

pub struct TestDataDir {
    path: PathBuf,
}

impl TestDataDir {
    pub fn new(name: &str) -> Self {
        Self {
            path: temp_data_dir(name),
        }
    }

    pub fn path(&self) -> &std::path::Path {
        &self.path
    }
}

impl Drop for TestDataDir {
    fn drop(&mut self) {
        cleanup_data_dir(&self.path);
    }
}

/// Clean up a test data directory.
pub fn cleanup_data_dir(dir: &std::path::Path) {
    let _ = std::fs::remove_dir_all(dir);
}

/// Allocate a free port by binding to port 0 and reading the OS-assigned port.
/// Same approach as kvstore's data plane port allocation.
pub async fn allocate_port() -> u16 {
    static ALLOCATED_PORTS: OnceLock<Mutex<HashSet<u16>>> = OnceLock::new();

    loop {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind to port 0");
        let port = listener.local_addr().unwrap().port();
        let is_new = ALLOCATED_PORTS
            .get_or_init(|| Mutex::new(HashSet::new()))
            .lock()
            .unwrap()
            .insert(port);
        drop(listener);

        if is_new {
            return port;
        }
    }
}
pub use crate::adapter::recovery::{RecoveryGate, RecoveryStage};
