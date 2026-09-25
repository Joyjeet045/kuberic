use std::io::Write;

use sqlserver_replicated::ObservationFailureKind;
use sqlserver_replicated::monitor::ObservationReport;
use sqlserver_replicated::runtime_error::RuntimeError;
use tokio::sync::{mpsc, oneshot};

struct PendingWrite {
    bytes: Vec<u8>,
    completed: oneshot::Sender<Result<(), RuntimeError>>,
}

pub(super) struct OutputWriter {
    sender: mpsc::Sender<PendingWrite>,
}

impl OutputWriter {
    pub(super) fn new() -> Result<Self, RuntimeError> {
        let (sender, mut receiver) = mpsc::channel::<PendingWrite>(1);
        // Tokio stdout uses its blocking pool, whose shutdown waits for blocked
        // writes. This process-owned thread must not hold async runtime shutdown.
        // Normal writes are acknowledged; cancellation may abandon a partial frame.
        let _worker = std::thread::Builder::new()
            .name("sqlserver-observer-output".to_owned())
            .spawn(move || {
                let mut stdout = std::io::stdout();
                while let Some(request) = receiver.blocking_recv() {
                    let result = stdout
                        .write_all(&request.bytes)
                        .and_then(|()| stdout.flush())
                        .map_err(|_| output_error());
                    if request.completed.send(result).is_err() {
                        break;
                    }
                }
            })
            .map_err(|_| {
                RuntimeError::new(
                    ObservationFailureKind::Unsupported,
                    "output",
                    "cannot start the output worker",
                )
            })?;
        Ok(Self { sender })
    }

    pub(super) async fn write(&self, report: &ObservationReport) -> Result<(), RuntimeError> {
        let mut bytes = serde_json::to_vec(report).map_err(|_| {
            RuntimeError::new(
                ObservationFailureKind::Malformed,
                "output",
                "cannot encode observation as JSON",
            )
        })?;
        bytes.push(b'\n');
        let (completed, response) = oneshot::channel();
        self.sender
            .send(PendingWrite { bytes, completed })
            .await
            .map_err(|_| output_error())?;
        response.await.map_err(|_| output_error())?
    }
}

fn output_error() -> RuntimeError {
    RuntimeError::new(
        ObservationFailureKind::Unreachable,
        "output",
        "cannot write observation to stdout",
    )
}
