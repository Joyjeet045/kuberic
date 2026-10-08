use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize};

use super::*;
use crate::authority::BuildAuthorityKind;

#[derive(Default)]
struct Control {
    aborts: AtomicUsize,
}

#[test]
fn dropped_configuration_preparation_invalidates_native_generation_synchronously() {
    let identity = ReplicaIdentity {
        replica_id: ReplicaId::new(1),
        instance_id: crate::protocol::types::ReplicaInstanceId::new("prepared-drop"),
        agent_generation: crate::protocol::types::AgentGeneration::new("prepared-drop-generation"),
    };
    let configuration = ManagedReplicaConfiguration {
        local_identity: identity.clone(),
        previous_configuration: None,
        current_configuration: ConfigurationDescriptor::new(
            Epoch::new(0, 1),
            identity.replica_id,
            vec![crate::protocol::types::ConfigurationMember {
                identity,
                role: ReplicaRole::Primary,
            }],
            1,
        ),
        switchover_handoff: None,
        secondary_removal: None,
        scale_up: None,
        build_kind: BuildAuthorityKind::Provisioning,
    };
    let active = Arc::new(AtomicBool::new(true));
    let native_generation = Arc::new(AtomicU64::new(7));
    let preparation = ManagedConfigurationPreparation {
        fence: ManagedOperationFence {
            configuration: Some(configuration.clone()),
            engine_session_id: "prepared-drop-session".into(),
            engine_generation: 7,
        },
        configuration,
        host_generation: 3,
        active: active.clone(),
        native_generation: native_generation.clone(),
    };
    drop(preparation);
    assert!(!active.load(Ordering::Acquire));
    assert_eq!(native_generation.load(Ordering::Acquire), 8);
}

#[async_trait]
impl Replicator for Control {
    async fn open(&self) -> Result<String> {
        Ok(String::new())
    }

    async fn change_role(&self, _: Epoch, _: ReplicaRole) -> Result<()> {
        Ok(())
    }

    async fn update_epoch(&self, _: Epoch) -> Result<()> {
        Ok(())
    }

    async fn close(&self) -> Result<()> {
        Ok(())
    }

    fn abort(&self) {
        self.aborts.fetch_add(1, Ordering::SeqCst);
    }

    async fn current_progress(&self) -> Result<Lsn> {
        Ok(0)
    }

    async fn catch_up_capability(&self) -> Result<Lsn> {
        Ok(0)
    }
}

#[test]
fn attachment_retries_bind_only_the_original_creation_reservation() {
    let control = Arc::new(Control::default());
    let interfaces = ReplicatorInterfaces::secondary(control, None);
    let reservation = ReplicatorCreationReservation::new(RuntimeHostToken::new());
    let attachment = interfaces.prepare_attachment(reservation).unwrap();
    let retry = interfaces.prepare_attachment(reservation).unwrap();
    assert_eq!(
        attachment.identity(RuntimeHostToken::new()),
        retry.identity(RuntimeHostToken::new()),
    );
    let other = ReplicatorCreationReservation::new(RuntimeHostToken::new());
    assert_ne!(
        reservation.identity(RuntimeHostToken::new()),
        other.identity(RuntimeHostToken::new()),
    );
    assert!(interfaces.prepare_attachment(other).is_err());
    assert_eq!(
        interfaces
            .prepare_attachment(reservation)
            .unwrap()
            .identity(RuntimeHostToken::new()),
        reservation.identity(RuntimeHostToken::new()),
    );
}

#[test]
fn unregistered_capability_bundle_aborts_once_after_its_last_handle_drops() {
    let control = Arc::new(Control::default());
    let interfaces = ReplicatorInterfaces::secondary(control.clone(), None);
    let reservation = ReplicatorCreationReservation::new(RuntimeHostToken::new());
    let attachment = interfaces.prepare_attachment(reservation).unwrap();
    drop(interfaces);
    assert_eq!(control.aborts.load(Ordering::SeqCst), 0);
    drop(attachment);
    assert_eq!(control.aborts.load(Ordering::SeqCst), 1);
}

#[test]
fn successful_host_registration_disarms_bundle_cleanup() {
    let control = Arc::new(Control::default());
    let interfaces = ReplicatorInterfaces::secondary(control.clone(), None);
    let reservation = ReplicatorCreationReservation::new(RuntimeHostToken::new());
    let attachment = interfaces.prepare_attachment(reservation).unwrap();
    attachment.disarm();
    drop(attachment);
    drop(interfaces);
    assert_eq!(control.aborts.load(Ordering::SeqCst), 0);
}
