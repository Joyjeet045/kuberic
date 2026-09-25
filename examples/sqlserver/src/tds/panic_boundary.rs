use std::cell::Cell;
use std::future::{Future, poll_fn};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::pin;
use std::sync::Once;
use std::task::Poll;

use crate::ObservationFailureKind;
use crate::runtime_error::RuntimeError;

thread_local! {
    static IN_DRIVER_POLL: Cell<bool> = const { Cell::new(false) };
}

static INSTALL_HOOK: Once = Once::new();

struct DriverPollScope(bool);

impl DriverPollScope {
    fn enter() -> Self {
        Self(IN_DRIVER_POLL.replace(true))
    }
}

impl Drop for DriverPollScope {
    fn drop(&mut self) {
        IN_DRIVER_POLL.set(self.0);
    }
}

fn install_hook() {
    INSTALL_HOOK.call_once(|| {
        let previous = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            // Hooks run before catch_unwind; driver payloads may contain server
            // text or private paths. All other panics retain the host's hook.
            if !IN_DRIVER_POLL.try_with(Cell::get).unwrap_or(false) {
                previous(info);
            }
        }));
    });
}

pub(super) async fn contain<T>(
    stage: &'static str,
    future: impl Future<Output = Result<T, RuntimeError>>,
) -> Result<T, RuntimeError> {
    install_hook();
    let mut future = pin!(future);
    poll_fn(|context| {
        // Never carry this thread-local scope across Pending: another task can
        // run on this worker before the driver is polled again.
        let _scope = DriverPollScope::enter();
        match catch_unwind(AssertUnwindSafe(|| future.as_mut().poll(context))) {
            Ok(result) => result,
            Err(_) => Poll::Ready(Err(RuntimeError::new(
                ObservationFailureKind::Malformed,
                stage,
                "TDS driver panicked; connection discarded",
            ))),
        }
    })
    .await
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    use super::*;

    struct Dropped(Arc<AtomicBool>);

    impl Drop for Dropped {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    #[tokio::test]
    async fn driver_panics_before_and_after_suspension_discard_owned_state() {
        for suspend in [false, true] {
            let dropped = Arc::new(AtomicBool::new(false));
            let state = Dropped(dropped.clone());
            let result: Result<(), RuntimeError> = contain("query", async move {
                let _state = state;
                if suspend {
                    tokio::task::yield_now().await;
                }
                panic!("private-driver-panic-payload");
            })
            .await;
            let error = result.unwrap_err();
            assert_eq!(error.kind, ObservationFailureKind::Malformed);
            assert_eq!(error.stage, "query");
            assert_eq!(error.message, "TDS driver panicked; connection discarded");
            assert!(!format!("{error:?}").contains("private-driver-panic-payload"));
            assert!(dropped.load(Ordering::SeqCst));
            assert!(!IN_DRIVER_POLL.get());
        }
    }

    #[tokio::test]
    async fn normal_driver_results_are_preserved() {
        assert_eq!(contain("query", async { Ok(42) }).await.unwrap(), 42);
        let original = RuntimeError::new(ObservationFailureKind::Tls, "login", "TLS failed");
        let result = contain("query", async { Err::<(), _>(original.clone()) })
            .await
            .unwrap_err();
        assert_eq!(result.kind, original.kind);
        assert_eq!(result.stage, original.stage);
        assert_eq!(result.message, original.message);
        assert!(!IN_DRIVER_POLL.get());
    }

    #[test]
    fn pending_driver_cancellation_restores_scope_and_drops_state() {
        let dropped = Arc::new(AtomicBool::new(false));
        let state = Dropped(dropped.clone());
        let mut future = Box::pin(contain("query", async move {
            let _state = state;
            std::future::pending::<Result<(), RuntimeError>>().await
        }));
        let waker = futures::task::noop_waker();
        let mut context = std::task::Context::from_waker(&waker);
        assert!(future.as_mut().poll(&mut context).is_pending());
        assert!(!IN_DRIVER_POLL.get());
        assert!(!dropped.load(Ordering::SeqCst));
        drop(future);
        assert!(dropped.load(Ordering::SeqCst));
        assert!(!IN_DRIVER_POLL.get());
    }

    #[test]
    fn hook_preserves_unrelated_panics_on_the_same_and_other_threads() {
        const CHILD: &str = "SQLSERVER_PANIC_HOOK_TEST_CHILD";
        if std::env::var_os(CHILD).is_none() {
            let output = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "tds::panic_boundary::tests::hook_preserves_unrelated_panics_on_the_same_and_other_threads",
                    "--nocapture",
                ])
                .env(CHILD, "1")
                .output()
                .unwrap();
            assert!(output.status.success(), "{output:?}");
            assert!(String::from_utf8_lossy(&output.stdout).contains("1 passed"));
            assert!(!String::from_utf8_lossy(&output.stderr).contains("private-driver-payload"));
            return;
        }

        let forwarded = Arc::new(AtomicUsize::new(0));
        let count = forwarded.clone();
        std::panic::set_hook(Box::new(move |info| {
            count.fetch_add(1, Ordering::SeqCst);
            eprintln!("host panic hook: {info}");
        }));

        let result: Result<(), RuntimeError> =
            futures::executor::block_on(contain("query", async {
                std::thread::spawn(|| {
                    assert!(catch_unwind(|| panic!("unrelated thread panic")).is_err());
                })
                .join()
                .unwrap();
                panic!("private-driver-payload");
            }));
        assert!(result.is_err());
        assert_eq!(forwarded.load(Ordering::SeqCst), 1);

        let mut pending = Box::pin(contain(
            "query",
            std::future::pending::<Result<(), RuntimeError>>(),
        ));
        let waker = futures::task::noop_waker();
        let mut context = std::task::Context::from_waker(&waker);
        assert!(pending.as_mut().poll(&mut context).is_pending());
        assert!(catch_unwind(|| panic!("unrelated task panic")).is_err());
        drop(pending);
        assert!(catch_unwind(|| panic!("unrelated later panic")).is_err());
        assert_eq!(forwarded.load(Ordering::SeqCst), 3);
    }
}
