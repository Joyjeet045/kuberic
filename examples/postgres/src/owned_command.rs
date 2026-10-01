use std::collections::BTreeMap;
use std::process::{ExitStatus, Output};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::io::AsyncReadExt;
use tokio::process::Command;

use crate::instance::PgError;
use crate::owned_process::OwnedProcess;

#[derive(Default)]
struct Commands {
    next_id: u64,
    closed: bool,
    close_epoch: u64,
    running: BTreeMap<u64, OwnedProcess>,
    error: Option<PgError>,
}

/// Registration, launch permission and teardown share one lock. The future's
/// guard reaps on cancellation; the registry also survives a dropped future so
/// synchronous host abort can join every helper and retain cleanup failures.
#[derive(Default)]
pub(crate) struct OwnedCommands(Mutex<Commands>);

pub(crate) struct CapturedCommands {
    owner: Arc<OwnedCommands>,
    ids: Vec<u64>,
    close_epoch: u64,
    was_closed: bool,
}

impl CapturedCommands {
    pub(crate) fn cancel_unstarted(&self) {
        let mut commands = self.owner.0.lock().unwrap();
        if commands.close_epoch == self.close_epoch && commands.error.is_none() {
            commands.closed = self.was_closed;
        }
    }

    pub(crate) fn terminate(&self) -> Result<(), PgError> {
        let mut commands = self.owner.0.lock().unwrap();
        for &id in &self.ids {
            OwnedCommands::remove(&mut commands, id);
        }
        commands.error.clone().map_or(Ok(()), Err)
    }
}

impl OwnedCommands {
    pub(crate) fn capture(self: &Arc<Self>) -> CapturedCommands {
        let mut commands = self.0.lock().unwrap();
        let was_closed = commands.closed;
        commands.closed = true;
        match commands.close_epoch.checked_add(1) {
            Some(next) => commands.close_epoch = next,
            None => {
                commands.error.get_or_insert_with(|| {
                    PgError::GenerationExhausted("helper cleanup epoch".into())
                });
            }
        }
        CapturedCommands {
            owner: self.clone(),
            ids: commands.running.keys().copied().collect(),
            close_epoch: commands.close_epoch,
            was_closed,
        }
    }

    pub(crate) async fn output_with_timeout(
        &self,
        command: &mut Command,
        timeout: Duration,
        operation: &'static str,
    ) -> Result<Output, PgError> {
        match tokio::time::timeout(timeout, self.output(command)).await {
            Ok(result) => result,
            Err(_) => {
                // Dropping output reaps its exact command tree. Retained cleanup
                // failure takes precedence over a retryable helper deadline.
                let cleanup = self.0.lock().unwrap().error.clone().map_or(Ok(()), Err);
                Err(PgError::Timeout(format!("{operation} command timeout")).with_cleanup(cleanup))
            }
        }
    }

    pub(crate) async fn output(&self, command: &mut Command) -> Result<Output, PgError> {
        let (id, mut stdout, mut stderr, started) = {
            let mut commands = self.0.lock().unwrap();
            if commands.closed {
                return Err(PgError::Process("owned commands are closed".into()));
            }
            if let Some(error) = &commands.error {
                return Err(error.clone());
            }
            let id = commands.next_id;
            let Some(next) = id.checked_add(1) else {
                let error = PgError::GenerationExhausted("helper identity".into());
                commands.closed = true;
                commands.error = Some(error.clone());
                return Err(error);
            };
            commands.next_id = next;
            let mut process = match OwnedProcess::spawn(command) {
                Ok(process) => process,
                Err(error) => {
                    // Retaining a spawn/identity failure also covers the rare
                    // case where the undispatched root could not be reaped.
                    commands.error.get_or_insert(error.clone());
                    return Err(error);
                }
            };
            let (stdout, stderr) = process.take_output();
            commands.running.insert(id, process);
            // Ownership is registered before the supervisor may launch.
            let started = commands.running.get(&id).unwrap().start();
            (id, stdout, stderr, started)
        };
        let guard = CommandGuard { owner: self, id };
        if let Err(error) = started {
            return Err(error.with_cleanup(guard.finish()));
        }
        let mut out = Vec::new();
        let mut err = Vec::new();
        let collected = tokio::try_join!(
            async {
                stdout
                    .read_to_end(&mut out)
                    .await
                    .map_err(|error| PgError::Process(format!("read owned stdout: {error}")))
            },
            async {
                stderr
                    .read_to_end(&mut err)
                    .await
                    .map_err(|error| PgError::Process(format!("read owned stderr: {error}")))
            },
            guard.wait(),
        );
        let cleanup = guard.finish();
        match collected {
            Ok((_, _, status)) => {
                cleanup?;
                Ok(Output {
                    status,
                    stdout: out,
                    stderr: err,
                })
            }
            Err(error) => Err(PgError::Process(format!(
                "{}: {error}; stdout: {}; stderr: {}",
                command.as_std().get_program().to_string_lossy(),
                String::from_utf8_lossy(&out),
                String::from_utf8_lossy(&err),
            ))
            .with_cleanup(cleanup)),
        }
    }

    #[cfg(test)]
    pub(crate) fn terminate(&self, close: bool) -> Result<(), PgError> {
        let mut commands = self.0.lock().unwrap();
        commands.closed |= close;
        if close {
            match commands.close_epoch.checked_add(1) {
                Some(next) => commands.close_epoch = next,
                None => {
                    commands.error.get_or_insert_with(|| {
                        PgError::GenerationExhausted("helper cleanup epoch".into())
                    });
                }
            }
        }
        let ids: Vec<_> = commands.running.keys().copied().collect();
        for id in ids {
            Self::remove(&mut commands, id);
        }
        commands.error.clone().map_or(Ok(()), Err)
    }

    fn remove(commands: &mut Commands, id: u64) {
        if let Some(process) = commands.running.get_mut(&id) {
            match process.terminate() {
                Ok(()) => {
                    commands.running.remove(&id);
                }
                Err(error) => {
                    tracing::error!(%error, "owned helper cleanup failed");
                    commands.error.get_or_insert(error);
                }
            }
        }
    }
}

struct CommandGuard<'a> {
    owner: &'a OwnedCommands,
    id: u64,
}

impl CommandGuard<'_> {
    async fn wait(&self) -> Result<ExitStatus, PgError> {
        loop {
            {
                let mut commands = self.owner.0.lock().unwrap();
                let process = commands
                    .running
                    .get_mut(&self.id)
                    .ok_or_else(|| PgError::Process("owned command cancelled".into()))?;
                if let Some(status) = process.completion()? {
                    return Ok(status);
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    fn finish(self) -> Result<(), PgError> {
        let owner = self.owner;
        drop(self);
        owner.0.lock().unwrap().error.clone().map_or(Ok(()), Err)
    }
}

impl Drop for CommandGuard<'_> {
    fn drop(&mut self) {
        OwnedCommands::remove(&mut self.owner.0.lock().unwrap(), self.id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::process::ExitStatusExt;

    #[tokio::test]
    async fn helper_identity_and_cleanup_epoch_exhaustion_remain_closed() {
        let commands = Arc::new(OwnedCommands::default());
        commands.0.lock().unwrap().next_id = u64::MAX;
        let error = commands
            .output(Command::new("sh").arg("-c").arg("exit 0"))
            .await
            .unwrap_err();
        assert!(matches!(error, PgError::GenerationExhausted(_)));
        assert!(commands.0.lock().unwrap().running.is_empty());
        let capture = commands.capture();
        capture.cancel_unstarted();
        assert!(commands.0.lock().unwrap().closed);
        assert!(capture.terminate().is_err());

        let commands = Arc::new(OwnedCommands::default());
        commands.0.lock().unwrap().close_epoch = u64::MAX - 1;
        let old = commands.capture();
        assert_eq!(commands.0.lock().unwrap().close_epoch, u64::MAX);
        let exhausted = commands.capture();
        exhausted.cancel_unstarted();
        old.cancel_unstarted();
        assert!(commands.0.lock().unwrap().closed);
        assert!(matches!(
            exhausted.terminate(),
            Err(PgError::GenerationExhausted(_))
        ));
        assert_eq!(commands.0.lock().unwrap().close_epoch, u64::MAX);
    }

    #[tokio::test]
    async fn helper_output_preserves_streams_status_and_environment() {
        let commands = OwnedCommands::default();
        let output = commands
            .output(
                Command::new("sh")
                    .args([
                        "-c",
                        "printf '%s' \"$LC_ALL\"; printf diagnostic >&2; exit 37",
                    ])
                    .env("LC_ALL", "C"),
            )
            .await
            .unwrap();
        assert_eq!(output.status.code(), Some(37));
        assert_eq!(output.stdout, b"C");
        assert_eq!(output.stderr, b"diagnostic");
        assert!(commands.0.lock().unwrap().running.is_empty());
        commands.terminate(true).unwrap();
    }

    #[tokio::test]
    async fn helper_signal_is_not_a_supervisor_cleanup_failure() {
        let commands = OwnedCommands::default();
        let output = commands
            .output(Command::new("sh").args(["-c", "kill -TERM $$"]))
            .await
            .unwrap();
        assert_eq!(output.status.signal(), Some(15));
        commands.terminate(true).unwrap();
    }

    #[tokio::test]
    async fn helper_pipes_are_drained_concurrently() {
        let commands = OwnedCommands::default();
        let output = tokio::time::timeout(
            Duration::from_secs(10),
            commands.output(Command::new("sh").args([
                "-c",
                "head -c 131072 /dev/zero; head -c 131072 /dev/zero >&2",
            ])),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(output.status.success());
        assert_eq!(output.stdout, vec![0; 131072]);
        assert_eq!(output.stderr, vec![0; 131072]);
    }
}
