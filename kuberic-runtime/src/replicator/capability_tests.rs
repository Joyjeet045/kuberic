use std::sync::atomic::AtomicUsize;

use super::*;

#[derive(Default)]
struct Control {
    aborts: AtomicUsize,
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
