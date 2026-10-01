use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::time::Duration;

use kuberic_agent::store::AgentStore;
use kuberic_protocol::types::*;
use kuberic_runtime_internal::authority::{AdmittedAuthority, BuildAuthority, RetiredAuthority};
use kuberic_runtime_internal::effects::RuntimeEffectAction;
use tokio_postgres::{Client, NoTls, error::SqlState};

use super::{PgPod, ProcessProbe, TestDataDir, native_configuration, native_identity};

pub fn run_pg_test<F, T>(test: F)
where
    F: FnOnce() -> T + Send + 'static,
    T: Future<Output = ()>,
{
    std::thread::Builder::new()
        .stack_size(16 * 1024 * 1024)
        .spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(test());
        })
        .unwrap()
        .join()
        .unwrap();
}

pub fn definitive_fence_error(error: &tokio_postgres::Error) -> bool {
    error.is_closed()
        || error.code().is_some_and(|code| {
            matches!(
                *code,
                SqlState::ADMIN_SHUTDOWN
                    | SqlState::CRASH_SHUTDOWN
                    | SqlState::READ_ONLY_SQL_TRANSACTION
            )
        })
}

pub struct PgSqlSession {
    client: Client,
    task: tokio::task::JoinHandle<()>,
}

impl PgSqlSession {
    pub fn client(&self) -> &Client {
        &self.client
    }

    pub async fn rejected(&self, id: i64) {
        let error = tokio::time::timeout(
            Duration::from_secs(5),
            self.client.execute(
                "INSERT INTO phase6_rows(id, value) VALUES($1, 'forbidden')",
                &[&id],
            ),
        )
        .await
        .expect("a timeout is not a fencing proof")
        .expect_err("stale write succeeded");
        assert!(
            definitive_fence_error(&error),
            "not a definitive fence: {error:?}"
        );
    }

    pub async fn disconnected(&self) {
        let error =
            tokio::time::timeout(Duration::from_secs(5), self.client.simple_query("SELECT 1"))
                .await
                .expect("disconnect oracle cannot time out")
                .expect_err("fenced SQL session remained connected");
        assert!(
            error.is_closed()
                || error.code().is_some_and(|code| matches!(
                    *code,
                    SqlState::ADMIN_SHUTDOWN | SqlState::CRASH_SHUTDOWN
                )),
            "{error:?}"
        );
    }
}

impl Drop for PgSqlSession {
    fn drop(&mut self) {
        self.task.abort();
    }
}

pub struct PgGroup {
    pub pods: BTreeMap<i64, PgPod>,
    pub configuration: ConfigurationDescriptor,
    pub expected: BTreeMap<i64, String>,
    retired: Vec<PgPod>,
    incarnation: u64,
    row: i64,
    addresses: BTreeSet<std::net::SocketAddr>,
    root: TestDataDir,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdmissionCut {
    Built,
    PreviousCurrent,
    CurrentOnly,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RestartPart {
    ApplicationProcess,
    AgentHost,
}

impl PgGroup {
    pub async fn singleton() -> Self {
        let root = TestDataDir::new("pg-group");
        let identity = native_identity(
            1,
            &format!("{}-1-0", root.path().file_name().unwrap().to_string_lossy()),
        );
        let pod = PgPod::new(root.path().join("r1"), identity.clone()).await;
        pod.singleton().await;
        let addresses = Self::addresses(&pod).into_iter().collect();
        let mut group = Self {
            pods: BTreeMap::from([(1, pod)]),
            configuration: native_configuration(&[identity], 0, 1),
            expected: BTreeMap::new(),
            retired: Vec::new(),
            incarnation: 0,
            row: 0,
            addresses,
            root,
        };
        let session = group.session(1, false).await;
        session
            .client
            .batch_execute("CREATE TABLE phase6_rows(id bigint PRIMARY KEY, value text NOT NULL)")
            .await
            .unwrap();
        group.write("bootstrap").await;
        group
    }

    pub fn primary_id(&self) -> i64 {
        self.configuration.primary_id.value()
    }
    fn addresses(pod: &PgPod) -> [std::net::SocketAddr; 2] {
        [
            format!("127.0.0.1:{}", pod.application.instance().port())
                .parse()
                .unwrap(),
            pod.endpoint
                .strip_prefix("http://")
                .unwrap()
                .parse()
                .unwrap(),
        ]
    }
    pub fn pod(&self, id: i64) -> &PgPod {
        self.pods.get(&id).expect("exact group member")
    }
    pub fn next_probe(&mut self) -> i64 {
        self.row = self.row.checked_add(1).unwrap();
        self.row
    }

    pub async fn session(&self, id: i64, administrative: bool) -> PgSqlSession {
        Self::connect(self.pod(id), administrative).await
    }

    async fn connect(pod: &PgPod, administrative: bool) -> PgSqlSession {
        let connection = if administrative {
            pod.application
                .instance()
                .connection_string()
                .replace("dbname=postgres", "dbname=kuberic")
        } else {
            pod.application.instance().application_connection_string()
        };
        let (client, connection) = tokio::time::timeout(
            Duration::from_secs(5),
            tokio_postgres::connect(&connection, NoTls),
        )
        .await
        .unwrap()
        .unwrap();
        let task = tokio::spawn(async move {
            if let Err(error) = connection.await {
                tracing::debug!(%error, "fixture SQL connection closed");
            }
        });
        PgSqlSession { client, task }
    }

    pub async fn write(&mut self, value: &str) -> i64 {
        let id = self.next_probe();
        let session = self.session(self.primary_id(), false).await;
        tokio::time::timeout(
            Duration::from_secs(10),
            session
                .client
                .execute("INSERT INTO phase6_rows VALUES($1,$2)", &[&id, &value]),
        )
        .await
        .expect("supported synchronous write deadline")
        .unwrap();
        self.expected.insert(id, value.into());
        id
    }

    pub async fn contents(&self, id: i64) -> BTreeMap<i64, String> {
        let session = self.session(id, true).await;
        session
            .client
            .query("SELECT id, value FROM phase6_rows ORDER BY id", &[])
            .await
            .unwrap()
            .into_iter()
            .map(|row| (row.get(0), row.get(1)))
            .collect()
    }

    pub async fn assert_contents(&self) {
        let primary = self
            .pod(self.primary_id())
            .application
            .native_driver()
            .durable_state()
            .await;
        for (&id, pod) in &self.pods {
            if !self
                .configuration
                .members
                .iter()
                .any(|member| member.identity == pod.identity)
            {
                continue;
            }
            tokio::time::timeout(Duration::from_secs(10), async {
                loop {
                    let actual = self.contents(id).await;
                    for (id, value) in &actual {
                        assert_eq!(self.expected.get(id), Some(value), "unexpected SQL row");
                    }
                    if actual == self.expected {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            })
            .await
            .unwrap_or_else(|_| {
                panic!(
                    "replica {id} did not replay expected rows {:?}",
                    self.expected
                )
            });
            pod.refresh().await;
            let durable = pod.application.native_driver().durable_state().await;
            let snapshot = pod.runtime.snapshot().await;
            assert_eq!(durable.identity.replica, pod.identity);
            assert_eq!(
                durable.system_identifier, primary.system_identifier,
                "replica {id} belongs to another PostgreSQL system"
            );
            assert_eq!(
                durable.timeline_id, primary.timeline_id,
                "replica {id} has a stale timeline"
            );
            assert!(durable.timeline_history_digest.is_some());
            assert_eq!(
                snapshot.authority.unwrap().current_configuration,
                self.configuration
            );
            assert_eq!(
                durable.recovery_state,
                crate::durable::PgRecoveryState::Ready
            );
            assert!(!durable.postgres_stopped);
            let observed = crate::native::PgNativeObserver::new(pod.application.instance().clone())
                .snapshot()
                .await
                .unwrap()
                .evidence
                .unwrap();
            assert_eq!(
                durable.system_identifier.as_ref(),
                Some(&observed.system_identifier)
            );
            assert_eq!(durable.timeline_id, Some(observed.timeline_id));
            assert_eq!(observed.in_recovery, id != self.primary_id());
            assert!(durable.flush_lsn <= observed.flush_lsn);
        }
    }

    pub async fn link(&self) {
        let primary = self.primary_id();
        let mut ids = self.pods.keys().copied().collect::<Vec<_>>();
        ids.sort_by_key(|id| (*id != primary, *id));
        for id in &ids {
            for other in &ids {
                if id != other {
                    self.pod(*id).peer(self.pod(*other)).await;
                }
            }
        }
    }

    async fn activate(&self) {
        for member in &self.configuration.members {
            let pod = self.pod(member.identity.replica_id.value());
            pod.effect(RuntimeEffectAction::SetAccessStatus {
                read: AccessStatus::Granted,
                write: if member.role == ReplicaRole::Primary {
                    AccessStatus::Granted
                } else {
                    AccessStatus::NotPrimary
                },
            })
            .await
            .unwrap();
        }
    }

    pub async fn candidate(&mut self, id: i64) -> PgPod {
        self.incarnation = self.incarnation.checked_add(1).unwrap();
        let name = format!(
            "{}-{id}-{}",
            self.root.path().file_name().unwrap().to_string_lossy(),
            self.incarnation
        );
        let pod = PgPod::new(
            self.root.path().join(format!("r{id}x{}", self.incarnation)),
            native_identity(id, &name),
        )
        .await;
        self.addresses.extend(Self::addresses(&pod));
        pod
    }

    pub async fn build_candidate(&self, candidate: &PgPod, id: &str) -> BuildAuthority {
        let primary = self.pod(self.primary_id());
        let authority = primary.authorize(candidate, id).await;
        primary
            .effect(RuntimeEffectAction::WaitForCatchup)
            .await
            .unwrap();
        assert!(
            candidate
                .application
                .instance()
                .connect_application()
                .await
                .is_err()
        );
        primary.build(candidate, &authority).await.unwrap();
        candidate.refresh().await;
        assert!(
            candidate
                .application
                .instance()
                .connect_application()
                .await
                .is_err()
        );
        let session = Self::connect(candidate, true).await;
        let rows: BTreeMap<i64, String> = session
            .client
            .query("SELECT id, value FROM phase6_rows ORDER BY id", &[])
            .await
            .unwrap()
            .into_iter()
            .map(|row| (row.get(0), row.get(1)))
            .collect();
        assert_eq!(rows, self.expected);
        authority
    }

    pub async fn add(&mut self, id: i64) {
        self.add_with_restart(id, None).await;
    }

    pub async fn add_with_restart(
        &mut self,
        id: i64,
        restart: Option<(AdmissionCut, RestartPart)>,
    ) {
        assert!(!self.pods.contains_key(&id));
        let candidate = self.candidate(id).await;
        let build = self
            .build_candidate(&candidate, &format!("scale-{id}-{}", self.incarnation))
            .await;
        let old = self.configuration.clone();
        let mut members = old.members.clone();
        members.push(ConfigurationMember {
            identity: candidate.identity.clone(),
            role: ReplicaRole::ActiveSecondary,
        });
        let next = ConfigurationDescriptor::new(
            Epoch::new(
                old.epoch.data_loss_number,
                old.epoch.configuration_number.checked_add(1).unwrap(),
            ),
            old.primary_id,
            members,
            EffectivePolicy::fixed((old.members.len() + 1) as u32, 30)
                .unwrap()
                .write_quorum,
        );
        let mut intent = ScaleUpIntent {
            operation_id: OperationId::default(),
            resource_uid: ResourceUid::new("postgres-native-test"),
            spec_generation: next.epoch.configuration_number as u64,
            desired_replicas: next.members.len() as u32,
            previous_configuration: old.clone(),
            current_configuration: next.clone(),
            previous_policy: EffectivePolicy::fixed(old.members.len() as u32, 30).unwrap(),
            current_policy: EffectivePolicy::fixed(next.members.len() as u32, 30).unwrap(),
            primary: self.pod(self.primary_id()).identity.clone(),
            target: candidate.identity.clone(),
            build_id: build.build_id.clone(),
            snapshot_boundary_lsn: build.replication_boundary_lsn,
            catch_up_boundary_lsn: build.replication_boundary_lsn,
        };
        intent.operation_id = intent.expected_operation_id();
        self.pods.insert(id, candidate);
        if let Some((AdmissionCut::Built, part)) = restart {
            self.restart_at(part).await;
            if part == RestartPart::AgentHost {
                self.build_candidate(self.pod(id), build.build_id.as_str())
                    .await;
            }
        }
        self.link().await;
        let mut ids = self.pods.keys().copied().collect::<Vec<_>>();
        ids.sort_by_key(|member| (*member != id, *member == self.primary_id()));
        for current_only in [false, true] {
            for member in &ids {
                self.pod(*member)
                    .effect(RuntimeEffectAction::AdmitAuthority(Box::new(
                        AdmittedAuthority {
                            local_identity: self.pod(*member).identity.clone(),
                            transition_kind: (!current_only).then_some(TransitionKind::ScaleUp),
                            previous_configuration: (!current_only).then_some(old.clone()),
                            current_configuration: next.clone(),
                            switchover_handoff: None,
                            secondary_removal: None,
                            scale_up: Some(Box::new(ScaleUpConfigurationEvidence::Admission {
                                intent: intent.clone(),
                            })),
                        },
                    )))
                    .await
                    .unwrap();
            }
            if let Some((cut, part)) = restart
                && cut
                    == if current_only {
                        AdmissionCut::CurrentOnly
                    } else {
                        AdmissionCut::PreviousCurrent
                    }
            {
                self.restart_at(part).await;
            }
            if !current_only {
                self.pod(id)
                    .effect(RuntimeEffectAction::ChangeRole(
                        ReplicaRole::ActiveSecondary,
                    ))
                    .await
                    .unwrap();
                self.pod(self.primary_id())
                    .effect(RuntimeEffectAction::WaitForCatchup)
                    .await
                    .unwrap();
            }
        }
        self.configuration = next;
        self.activate().await;
        self.assert_contents().await;
    }

    async fn restart_at(&mut self, part: RestartPart) {
        let id = self.primary_id();
        match part {
            RestartPart::ApplicationProcess => self.pod(id).restart_application().await.unwrap(),
            RestartPart::AgentHost => {
                let old = self.pods.remove(&id).unwrap();
                let restarted = old.reopen().await;
                self.addresses.extend(Self::addresses(&restarted));
                self.pods.insert(id, restarted);
                self.link().await;
            }
        }
    }

    async fn witness(&self, id: i64, sequence: u64) -> SecondaryRemovalWitness {
        let pod = self.pod(id);
        let snapshot = pod.runtime.snapshot().await;
        let state = pod.store.load_state().await.unwrap();
        let authority = snapshot.authority.unwrap();
        SecondaryRemovalWitness {
            resource_uid: state.identity.resource_uid,
            identity: pod.identity.clone(),
            role: snapshot.role,
            process_session_id: pod.session.clone(),
            report_sequence: sequence,
            epoch: authority.current_configuration.epoch,
            previous_configuration_id: authority.previous_configuration.map(|c| c.configuration_id),
            current_configuration_id: authority.current_configuration.configuration_id,
            verified_replication_lsn: snapshot.verified_replication_lsn.unwrap(),
            write_status: snapshot.write_status,
            pending_operation_id: state.pending_effect.map(|p| p.effect.operation_id),
            retained_operation_id: state.retained_result.map(|r| r.operation_id),
        }
    }

    pub async fn remove(&mut self, id: i64) {
        assert_ne!(id, self.primary_id());
        let removed = self.pod(id);
        let old = self.configuration.clone();
        let current_policy = EffectivePolicy::fixed((old.members.len() - 1) as u32, 30).unwrap();
        let next = ConfigurationDescriptor::new(
            Epoch::new(
                old.epoch.data_loss_number,
                old.epoch.configuration_number + 1,
            ),
            old.primary_id,
            old.members
                .iter()
                .filter(|m| m.identity != removed.identity)
                .cloned()
                .collect(),
            current_policy.write_quorum,
        );
        let resource_uid = ResourceUid::new("postgres-native-test");
        let mut intent = SecondaryScaleDownIntent {
            operation_id: OperationId::default(),
            resource_uid: resource_uid.clone(),
            spec_generation: next.epoch.configuration_number as u64,
            desired_replicas: next.members.len() as u32,
            previous_configuration: old.clone(),
            current_configuration: next.clone(),
            previous_policy: EffectivePolicy::fixed(old.members.len() as u32, 30).unwrap(),
            current_policy,
            primary: self.pod(self.primary_id()).identity.clone(),
            target: removed.identity.clone(),
            cleanup: ReplicaCleanupIdentity {
                pod: CleanupResourceIdentity::Present {
                    name: format!("p{id}"),
                    uid: removed.identity.instance_id.to_string(),
                },
                pvc: CleanupResourceIdentity::Present {
                    name: format!("v{id}"),
                    uid: format!("pvc-{id}"),
                },
                endpoint: CleanupResourceIdentity::Present {
                    name: derive_replica_endpoint_name(&resource_uid, &removed.identity),
                    uid: format!("endpoint-{id}"),
                },
            },
        };
        intent.operation_id = intent.expected_operation_id();
        let survivors = next
            .members
            .iter()
            .map(|m| m.identity.replica_id.value())
            .collect::<Vec<_>>();
        let primary = self.pod(self.primary_id());
        primary
            .effect_as(
                intent.command_operation_id(SecondaryRemovalStage::Prepare, &primary.identity),
                RuntimeEffectAction::PrepareSecondaryRemoval {
                    intent: Box::new(intent.clone()),
                    process_session_id: primary.session.clone(),
                    report_sequence: 1,
                },
            )
            .await
            .unwrap();
        let preparation = primary
            .store
            .load_state()
            .await
            .unwrap()
            .prepared_secondary_removal
            .unwrap();
        let mut old_witnesses = Vec::new();
        for &member in &survivors {
            if member != self.primary_id() {
                self.pod(member)
                    .effect(RuntimeEffectAction::SetAccessStatus {
                        read: AccessStatus::ReconfigurationPending,
                        write: AccessStatus::ReconfigurationPending,
                    })
                    .await
                    .unwrap();
                self.pod(member).refresh().await;
            }
            old_witnesses.push(self.witness(member, 2).await);
        }
        let mut evidence = SecondaryRemovalEvidence {
            preparation,
            previous_read_quorum: old_witnesses,
            reduced_write_quorum: Vec::new(),
        };
        for current_only in [false, true] {
            for &member in &survivors {
                let pod = self.pod(member);
                pod.effect_as(
                    intent.command_operation_id(
                        if current_only {
                            SecondaryRemovalStage::CurrentOnly
                        } else {
                            SecondaryRemovalStage::PreviousCurrent
                        },
                        &pod.identity,
                    ),
                    RuntimeEffectAction::AdmitAuthority(Box::new(AdmittedAuthority {
                        local_identity: pod.identity.clone(),
                        previous_configuration: (!current_only).then_some(old.clone()),
                        current_configuration: next.clone(),
                        transition_kind: (!current_only)
                            .then_some(TransitionKind::SecondaryScaleDown),
                        switchover_handoff: None,
                        scale_up: None,
                        secondary_removal: Some(evidence.clone()),
                    })),
                )
                .await
                .unwrap();
            }
            if !current_only {
                for &member in &survivors {
                    evidence
                        .reduced_write_quorum
                        .push(self.witness(member, 3).await);
                }
                primary
                    .effect(RuntimeEffectAction::WaitForCatchup)
                    .await
                    .unwrap();
            }
        }
        let mut witnesses = Vec::new();
        for &member in &survivors {
            witnesses.push(self.witness(member, 4).await);
        }
        let committed = SecondaryScaleDownCleanup {
            evidence,
            current_only_write_quorum: witnesses.clone(),
            retirement: None,
        };
        for &member in &survivors {
            let pod = self.pod(member);
            for witness in witnesses.iter().filter(|w| w.identity != pod.identity) {
                pod.effect(RuntimeEffectAction::ObserveSecondaryRemovalWitness(
                    Box::new(witness.clone()),
                ))
                .await
                .unwrap();
            }
            pod.effect_as(
                intent.command_operation_id(SecondaryRemovalStage::AcceptCommit, &pod.identity),
                RuntimeEffectAction::AcceptSecondaryRemovalCommit(Box::new(committed.clone())),
            )
            .await
            .unwrap();
        }
        let report = ReplicaRetirementReport {
            intent: intent.clone(),
            operation_id: intent
                .command_operation_id(SecondaryRemovalStage::Retire, &removed.identity),
            process_session_id: removed.session.clone(),
            report_sequence: 5,
            epoch: next.epoch,
            role: ReplicaRole::None,
            read_status: AccessStatus::NotPrimary,
            write_status: AccessStatus::NotPrimary,
            application_closed: true,
            peers_fenced: true,
        };
        removed
            .effect_as(
                report.operation_id.clone(),
                RuntimeEffectAction::RetireReplica(Box::new(RetiredAuthority {
                    committed,
                    report,
                })),
            )
            .await
            .unwrap();
        self.retired.push(self.pods.remove(&id).unwrap());
        self.configuration = next;
        self.activate().await;
        self.assert_contents().await;
    }

    pub async fn replace(&mut self, id: i64) {
        assert_ne!(id, self.primary_id());
        self.pod(id)
            .effect(RuntimeEffectAction::ChangeRole(ReplicaRole::None))
            .await
            .unwrap();
        let candidate = self.candidate(id).await;
        let build = self
            .build_candidate(&candidate, &format!("replace-{id}-{}", self.incarnation))
            .await;
        let old = self.configuration.clone();
        let replacement = candidate.identity.clone();
        self.retired.push(self.pods.insert(id, candidate).unwrap());
        self.link().await;
        let next = ConfigurationDescriptor::new(
            Epoch::new(
                old.epoch.data_loss_number,
                old.epoch.configuration_number + 1,
            ),
            old.primary_id,
            old.members
                .iter()
                .map(|member| {
                    if member.identity.replica_id.value() == id {
                        ConfigurationMember {
                            identity: replacement.clone(),
                            role: ReplicaRole::ActiveSecondary,
                        }
                    } else {
                        member.clone()
                    }
                })
                .collect(),
            old.write_quorum,
        );
        let primary = self.primary_id();
        let ids = self.ordered_ids(primary);
        for current_only in [false, true] {
            for member in &ids {
                self.pod(*member)
                    .effect(RuntimeEffectAction::AdmitAuthority(Box::new(
                        AdmittedAuthority {
                            local_identity: self.pod(*member).identity.clone(),
                            previous_configuration: (!current_only).then_some(old.clone()),
                            current_configuration: next.clone(),
                            transition_kind: (!current_only).then_some(TransitionKind::Replacement),
                            switchover_handoff: None,
                            secondary_removal: None,
                            scale_up: None,
                        },
                    )))
                    .await
                    .unwrap();
                if *member == id && !current_only {
                    self.pod(id)
                        .effect(RuntimeEffectAction::ChangeRole(
                            ReplicaRole::ActiveSecondary,
                        ))
                        .await
                        .unwrap();
                }
            }
            if !current_only {
                self.pod(primary)
                    .effect(RuntimeEffectAction::WaitForCatchup)
                    .await
                    .unwrap();
            }
        }
        self.configuration = next;
        self.pod(primary)
            .effect(RuntimeEffectAction::RetireBuild(build.build_id.clone()))
            .await
            .unwrap();
        self.pod(id)
            .effect(RuntimeEffectAction::RetireBuild(build.build_id))
            .await
            .unwrap();
        self.activate().await;
        self.assert_contents().await;
    }

    fn ordered_ids(&self, primary: i64) -> Vec<i64> {
        let mut ids = self
            .configuration
            .members
            .iter()
            .map(|m| m.identity.replica_id.value())
            .collect::<Vec<_>>();
        ids.sort_by_key(|id| (*id == primary, *id));
        ids
    }

    pub async fn refresh_configuration(&mut self) {
        let next = ConfigurationDescriptor::new(
            Epoch::new(
                self.configuration.epoch.data_loss_number,
                self.configuration.epoch.configuration_number + 1,
            ),
            self.configuration.primary_id,
            self.configuration.members.clone(),
            self.configuration.write_quorum,
        );
        for id in self.ordered_ids(self.primary_id()) {
            self.pod(id).admit(next.clone()).await;
        }
        self.configuration = next;
        self.activate().await;
        self.assert_contents().await;
    }

    pub async fn restart_peer(&mut self, id: i64) {
        let before = self
            .pod(id)
            .application
            .native_driver()
            .durable_state()
            .await;
        let old = self.pods.remove(&id).unwrap();
        let reopened = old.reopen().await;
        self.addresses.extend(Self::addresses(&reopened));
        assert_eq!(
            reopened
                .application
                .native_driver()
                .durable_state()
                .await
                .identity,
            before.identity
        );
        self.pods.insert(id, reopened);
        self.link().await;
        self.refresh_configuration().await;
    }

    pub async fn fence_quorum_loss(&self) {
        let primary = self.pod(self.primary_id());
        for id in self.ordered_ids(self.primary_id()) {
            if id == self.primary_id() {
                continue;
            }
            self.pod(id).disconnect_receiver().await.unwrap();
            self.pod(id)
                .effect(RuntimeEffectAction::SetAccessStatus {
                    read: AccessStatus::Granted,
                    write: AccessStatus::NotPrimary,
                })
                .await
                .unwrap();
        }
        assert!(
            primary
                .effect(RuntimeEffectAction::RefreshApplicationProgress)
                .await
                .is_err()
        );
        assert_ne!(
            primary.runtime.partition_report().await.write_status,
            AccessStatus::Granted
        );
        kuberic_agent::runtime_adapter::RuntimeAdapter::new(
            primary.store.clone(),
            primary.runtime.clone(),
        )
        .resume_pending()
        .await
        .unwrap();
    }

    pub async fn reopen_agent_metadata(&mut self, id: i64) {
        let pod = self.pods.get_mut(&id).unwrap();
        let before = pod.store.load_state().await.unwrap();
        pod.store = std::sync::Arc::new(
            kuberic_agent::sqlite_store::SqliteStore::open_existing(
                kuberic_agent::sqlite_store::SqliteStore::metadata_database_path(&pod.root),
                Some(&before.identity),
            )
            .unwrap(),
        );
        assert_eq!(pod.store.load_state().await.unwrap(), before);
        pod.refresh().await;
    }

    pub async fn rejoin(&mut self, id: i64) {
        assert_ne!(id, self.primary_id());
        let build = self
            .build_candidate(
                self.pod(id),
                &format!(
                    "rejoin-{id}-{}",
                    self.configuration.epoch.configuration_number
                ),
            )
            .await;
        let existing = self.pod(id).runtime.snapshot().await.authority;
        if let Some(mut authority) =
            existing.filter(|a| a.current_configuration == self.configuration)
        {
            authority.previous_configuration = None;
            authority.transition_kind = None;
            self.pod(id)
                .effect(RuntimeEffectAction::AdmitAuthority(Box::new(authority)))
                .await
                .unwrap();
        } else {
            self.pod(id).admit(self.configuration.clone()).await;
        }
        self.pod(id)
            .effect(RuntimeEffectAction::ChangeRole(
                ReplicaRole::ActiveSecondary,
            ))
            .await
            .unwrap();
        self.pod(self.primary_id())
            .effect(RuntimeEffectAction::RetireBuild(build.build_id.clone()))
            .await
            .unwrap();
        self.pod(id)
            .effect(RuntimeEffectAction::RetireBuild(build.build_id))
            .await
            .unwrap();
        self.refresh_configuration().await;
    }

    pub async fn change_primary(&mut self, target: i64, planned: bool) -> i64 {
        let old_primary = self.primary_id();
        assert_ne!(old_primary, target);
        let previous = self.configuration.clone();
        let mut ids = previous
            .members
            .iter()
            .map(|m| m.identity.clone())
            .collect::<Vec<_>>();
        ids.sort_by_key(|identity| (identity.replica_id.value() != target, identity.replica_id));
        let next = native_configuration(&ids, 0, previous.epoch.configuration_number + 1);
        let handoff = if planned {
            let source = self.pod(old_primary);
            source
                .effect(RuntimeEffectAction::PrepareSwitchover {
                    preparation_generation: next.epoch.configuration_number as u64,
                    request_id: SwitchoverRequestId::new(format!(
                        "handoff-{}",
                        next.epoch.configuration_number
                    )),
                    source: source.identity.clone(),
                    target: self.pod(target).identity.clone(),
                    starting_configuration_id: previous.configuration_id.clone(),
                    starting_epoch: previous.epoch,
                })
                .await
                .unwrap();
            let handoff = source
                .store
                .load_state()
                .await
                .unwrap()
                .prepared_switchover
                .unwrap();
            source
                .effect(RuntimeEffectAction::AdmitAuthority(Box::new(
                    AdmittedAuthority {
                        local_identity: source.identity.clone(),
                        previous_configuration: Some(previous.clone()),
                        current_configuration: next.clone(),
                        transition_kind: Some(TransitionKind::PlannedSwitchover),
                        switchover_handoff: Some(handoff.clone()),
                        secondary_removal: None,
                        scale_up: None,
                    },
                )))
                .await
                .unwrap();
            source
                .effect(RuntimeEffectAction::ChangeRole(
                    ReplicaRole::ActiveSecondary,
                ))
                .await
                .unwrap();
            Some(handoff)
        } else {
            for id in self.ordered_ids(old_primary) {
                if id != target && id != old_primary {
                    self.pod(id).disconnect_receiver().await.unwrap();
                }
            }
            self.write("elected target's exclusive acknowledgement")
                .await;
            self.pod(old_primary)
                .effect(RuntimeEffectAction::ChangeRole(ReplicaRole::None))
                .await
                .unwrap();
            None
        };
        let mut survivors = self.ordered_ids(target);
        survivors.retain(|id| *id != old_primary);
        for id in &survivors {
            self.pod(*id)
                .effect(RuntimeEffectAction::AdmitAuthority(Box::new(
                    AdmittedAuthority {
                        local_identity: self.pod(*id).identity.clone(),
                        previous_configuration: Some(previous.clone()),
                        current_configuration: next.clone(),
                        transition_kind: Some(if planned {
                            TransitionKind::PlannedSwitchover
                        } else {
                            TransitionKind::Failover
                        }),
                        switchover_handoff: handoff.clone(),
                        secondary_removal: None,
                        scale_up: None,
                    },
                )))
                .await
                .unwrap();
        }
        self.pod(target)
            .effect(RuntimeEffectAction::ChangeRole(ReplicaRole::Primary))
            .await
            .unwrap();
        assert!(
            self.pod(target)
                .application
                .instance()
                .connect_application()
                .await
                .is_err()
        );
        self.configuration = next;
        for id in survivors {
            self.pod(id)
                .effect(RuntimeEffectAction::SetAccessStatus {
                    read: AccessStatus::Granted,
                    write: if id == target {
                        AccessStatus::Granted
                    } else {
                        AccessStatus::NotPrimary
                    },
                })
                .await
                .unwrap();
        }
        let actual = self.contents(target).await;
        assert_eq!(actual, self.expected);
        old_primary
    }

    pub fn old_incarnation(&self) -> &PgPod {
        self.retired.last().expect("replaced replica")
    }

    pub async fn shutdown(self) {
        let errors = self.teardown().await;
        assert!(errors.is_empty(), "fixture teardown failures: {errors:?}");
    }

    pub async fn shutdown_after_fault(self) -> Vec<(i64, kuberic_runtime::RuntimeError)> {
        self.teardown().await
    }

    async fn teardown(mut self) -> Vec<(i64, kuberic_runtime::RuntimeError)> {
        let path = self.root.path().to_path_buf();
        let mut errors = Vec::new();
        for pod in self.pods.values_mut().chain(self.retired.iter_mut()) {
            let probe = pod
                .application
                .instance()
                .is_running()
                .await
                .then(|| ProcessProbe::postgres(pod.application.instance().data_dir()));
            if let Err(error) = pod.shutdown().await {
                errors.push((pod.identity.replica_id.value(), error));
            }
            if let Some(probe) = probe {
                probe.assert_reaped();
            }
        }
        self.pods.clear();
        self.retired.clear();
        for address in &self.addresses {
            let _free =
                std::net::TcpListener::bind(address).expect("fixture listener survived teardown");
        }
        drop(self);
        assert!(!path.exists(), "fixture data survived teardown");
        errors
    }
}
