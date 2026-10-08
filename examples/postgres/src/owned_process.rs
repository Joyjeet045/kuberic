use std::collections::HashSet;
use std::io::{self, Read};
use std::os::fd::OwnedFd;
use std::os::unix::net::UnixStream;
use std::os::unix::process::ExitStatusExt;
use std::path::Path;
use std::process::{ExitStatus, Stdio};
use std::time::{Duration, Instant};

use rustix::event::{PollFd, PollFlags, Timespec, poll};
use rustix::process::{Pid, PidfdFlags, Signal, kill_process, pidfd_open, pidfd_send_signal};
use tokio::process::{Child, ChildStderr, ChildStdout, Command};

use crate::instance::PgError;

const POLL_INTERVAL: Duration = Duration::from_millis(10);
const TREE_TIMEOUT: Duration = Duration::from_secs(1);
const EXIT_TIMEOUT: Duration = Duration::from_secs(1);

struct Process {
    pid: u32,
    fd: OwnedFd,
}

impl Process {
    fn open(pid: u32) -> io::Result<Self> {
        let pid_value = Pid::from_raw(pid as i32)
            .ok_or_else(|| io::Error::other("invalid owned process ID"))?;
        Ok(Self {
            pid,
            fd: pidfd_open(pid_value, PidfdFlags::empty())?,
        })
    }

    fn events(&self) -> io::Result<PollFlags> {
        let mut fds = [PollFd::new(&self.fd, PollFlags::IN)];
        poll(
            &mut fds,
            Some(&Timespec {
                tv_sec: 0,
                tv_nsec: 0,
            }),
        )?;
        Ok(fds[0].revents())
    }

    fn exited(&self) -> io::Result<bool> {
        let events = self.events()?;
        Ok(events.contains(PollFlags::IN) || events.contains(PollFlags::HUP))
    }

    fn reaped(&self) -> io::Result<bool> {
        // Unlike POLLIN (also true for zombies), pidfd POLLHUP proves reaping
        // without relying on a /proc path that could belong to a reused PID.
        Ok(self.events()?.contains(PollFlags::HUP))
    }

    fn signal(&self, signal: Signal) -> io::Result<()> {
        match pidfd_send_signal(&self.fd, signal) {
            Ok(()) | Err(rustix::io::Errno::SRCH) => Ok(()),
            Err(error) => Err(error.into()),
        }
    }

    fn stat(&self) -> io::Result<(char, u32)> {
        let stat = std::fs::read_to_string(format!("/proc/{}/stat", self.pid))?;
        let fields = stat
            .rsplit_once(')')
            .ok_or_else(|| io::Error::other("invalid process stat"))?
            .1;
        let mut fields = fields.split_whitespace();
        let state = fields.next().and_then(|value| value.chars().next());
        let parent = fields.next().and_then(|value| value.parse().ok());
        state
            .zip(parent)
            .ok_or_else(|| io::Error::other("invalid process identity"))
    }
}

/// Linux pidfds retain identity even after exit. PostgreSQL backends create their
/// own sessions, so killing only the launcher's process group is insufficient.
pub(crate) struct OwnedProcess {
    child: Child,
    channel: UnixStream,
    processes: Vec<Process>,
    postmaster: Option<usize>,
    postmaster_ancestors: Vec<usize>,
}

impl OwnedProcess {
    pub(crate) fn check_support() -> Result<(), PgError> {
        Process::open(std::process::id())
            .and_then(|process| process.stat())
            .map(|_| ())
            .map_err(|error| {
                PgError::Process(format!("Linux process ownership unavailable: {error}"))
            })
    }

    pub(crate) fn spawn(command: &Command) -> Result<Self, PgError> {
        Self::check_support()?;
        let spawn = || -> io::Result<_> {
            let (channel, input) = UnixStream::pair()?;
            let source = command.as_std();
            let mut supervisor = Command::new(crate::process_supervisor::executable()?);
            supervisor
                .arg(crate::process_supervisor::ARGUMENT)
                .arg(source.get_program())
                .args(source.get_args())
                .stdin(Stdio::from(OwnedFd::from(input)))
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .process_group(0)
                .kill_on_drop(true);
            for (key, value) in source.get_envs() {
                match value {
                    Some(value) => supervisor.env(key, value),
                    None => supervisor.env_remove(key),
                };
            }
            if let Some(directory) = source.get_current_dir() {
                supervisor.current_dir(directory);
            }
            Ok((supervisor.spawn()?, channel))
        };
        let (child, channel) =
            spawn().map_err(|error| PgError::Process(format!("owned command spawn: {error}")))?;
        Self::new(child, channel)
    }

    fn new(mut child: Child, channel: UnixStream) -> Result<Self, PgError> {
        match Process::open(child.id().expect("newly spawned PostgreSQL child")) {
            Ok(process) => Ok(Self {
                child,
                channel,
                processes: vec![process],
                postmaster: None,
                postmaster_ancestors: Vec::new(),
            }),
            Err(error) => {
                // The unreaped child reserves this PID. The supervisor cannot
                // launch anything before the start handshake, so this fallback
                // cannot leave undiscovered descendants.
                let pid = Pid::from_raw(child.id().unwrap() as i32).unwrap();
                let _ = kill_process(pid, Signal::QUIT);
                let deadline = Instant::now() + EXIT_TIMEOUT;
                while child.try_wait().ok().flatten().is_none() && Instant::now() < deadline {
                    std::thread::sleep(POLL_INTERVAL);
                }
                let mut reaped = child.try_wait().ok().flatten().is_some();
                if !reaped {
                    let _ = child.start_kill();
                    let deadline = Instant::now() + EXIT_TIMEOUT;
                    while !reaped && Instant::now() < deadline {
                        reaped = child.try_wait().ok().flatten().is_some();
                        std::thread::sleep(POLL_INTERVAL);
                    }
                }
                Err(PgError::Process(format!(
                    "retain PostgreSQL pidfd: {error}; child reaped: {reaped}"
                )))
            }
        }
    }

    pub(crate) fn take_output(&mut self) -> (ChildStdout, ChildStderr) {
        (
            self.child.stdout.take().expect("piped owned stdout"),
            self.child.stderr.take().expect("piped owned stderr"),
        )
    }

    pub(crate) fn completion(&mut self) -> Result<Option<ExitStatus>, PgError> {
        let completed = (|| -> io::Result<Option<ExitStatus>> {
            if !self.wait_exited(Duration::ZERO)? {
                return Ok(None);
            }
            // The supervisor has exited, so a missing/short reply is an error,
            // never a reason to block the owner or assume command success.
            self.channel.set_nonblocking(true)?;
            let mut bytes = [0; 4];
            self.channel.read_exact(&mut bytes)?;
            Ok(Some(ExitStatus::from_raw(i32::from_ne_bytes(bytes))))
        })();
        completed.map_err(|error| PgError::Process(format!("owned command completion: {error}")))
    }

    pub(crate) fn exited(&self) -> Result<bool, PgError> {
        self.processes[0]
            .exited()
            .and_then(|root_exited| {
                self.processes[self.postmaster.unwrap_or(0)]
                    .exited()
                    .map(|postmaster_exited| root_exited || postmaster_exited)
            })
            .map_err(|error| PgError::Process(format!("observe PostgreSQL exit: {error}")))
    }

    pub(crate) fn start(&self) -> Result<(), PgError> {
        self.command(crate::process_supervisor::START)
            .map_err(|error| PgError::Process(format!("start PostgreSQL supervisor: {error}")))
    }

    fn command(&self, command: u8) -> io::Result<()> {
        if rustix::io::write(&self.channel, &[command])? != 1 {
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "supervisor command was not written",
            ));
        }
        Ok(())
    }

    fn freeze_tree(&mut self) -> io::Result<()> {
        let deadline = Instant::now() + TREE_TIMEOUT;
        let mut known = HashSet::new();
        for process in &self.processes {
            if !process.exited()? {
                known.insert(process.pid);
            }
        }
        let mut index = 0;
        let mut pass_size = 0;
        while index < self.processes.len() {
            if Instant::now() >= deadline {
                return Err(io::Error::other(format!(
                    "owned PostgreSQL tree discovery timed out at {index}/{} processes",
                    self.processes.len()
                )));
            }
            let process = &self.processes[index];
            if !process.exited()? {
                process.signal(Signal::STOP)?;
                loop {
                    if process.exited()? || matches!(process.stat()?.0, 'T' | 't' | 'Z') {
                        break;
                    }
                    if Instant::now() >= deadline {
                        return Err(io::Error::other("owned PostgreSQL process did not stop"));
                    }
                    std::thread::sleep(POLL_INTERVAL);
                }
                if !process.exited()? {
                    // A stopped parent cannot fork or reap, so its children's
                    // identities stay reserved while their pidfds are acquired.
                    let mut children = Vec::new();
                    for task in std::fs::read_dir(format!("/proc/{}/task", process.pid))? {
                        let task = task?;
                        let ids = std::fs::read_to_string(task.path().join("children"))?;
                        for pid in ids.split_whitespace() {
                            if Instant::now() >= deadline {
                                return Err(io::Error::other(format!(
                                    "owned PostgreSQL tree discovery timed out while scanning {} at {index}/{} processes",
                                    process.pid,
                                    self.processes.len()
                                )));
                            }
                            let pid = pid.parse().map_err(io::Error::other)?;
                            if known.insert(pid) {
                                let child = Process::open(pid)?;
                                if child.stat()?.1 != process.pid || process.exited()? {
                                    return Err(io::Error::other(
                                        "owned descendant identity changed",
                                    ));
                                }
                                children.push(child);
                            }
                        }
                    }
                    self.processes.extend(children);
                }
            }
            index += 1;
            if index == self.processes.len() && pass_size != self.processes.len() {
                // A launcher can exit while we stop it. Rescan the stopped
                // subreaper for descendants adopted since its first scan.
                pass_size = self.processes.len();
                index = 0;
            }
        }
        Ok(())
    }

    fn signal_all(&self, signal: Signal) -> io::Result<()> {
        let mut result = Ok(());
        for (index, process) in self.processes.iter().enumerate() {
            // The subreaper must survive until all descendants have been reaped.
            if index == 0 && signal != Signal::CONT {
                continue;
            }
            if let Err(error) = process.signal(signal) {
                result = Err(error);
            }
        }
        result
    }

    pub(crate) fn remember_descendants(&mut self) -> Result<(), PgError> {
        let result = self.freeze_tree();
        let resumed = self.signal_all(Signal::CONT);
        result
            .and(resumed)
            .map_err(|error| PgError::Process(format!("capture PostgreSQL descendants: {error}")))
    }

    pub(crate) fn bind_postmaster(&mut self, backend_pid: u32) -> Result<(), PgError> {
        let result = (|| {
            self.freeze_tree()?;
            let backend = self
                .processes
                .iter()
                .find(|process| process.pid == backend_pid)
                .ok_or_else(|| io::Error::other("ready backend is not an owned descendant"))?;
            let parent = backend.stat()?.1;
            if backend.exited()? {
                return Err(io::Error::other(
                    "ready backend exited during identity verification",
                ));
            }
            let index = self
                .processes
                .iter()
                .position(|process| process.pid == parent)
                .ok_or_else(|| io::Error::other("postmaster is not an owned descendant"))?;
            if index == 0 || self.processes[index].exited()? {
                return Err(io::Error::other("postmaster identity is not live"));
            }
            self.postmaster = Some(index);
            self.postmaster_ancestors.clear();
            let mut parent = self.processes[index].stat()?.1;
            while parent != self.processes[0].pid {
                let ancestor = self
                    .processes
                    .iter()
                    .position(|process| process.pid == parent)
                    .ok_or_else(|| io::Error::other("postmaster ancestor is not owned"))?;
                self.postmaster_ancestors.push(ancestor);
                parent = self.processes[ancestor].stat()?.1;
            }
            Ok(())
        })();
        let resumed = self.signal_all(Signal::CONT);
        result
            .and(resumed)
            .map_err(|error| PgError::Process(format!("bind owned postmaster: {error}")))
    }

    pub(crate) fn stop(&mut self, data_dir: &Path) -> Result<(), PgError> {
        if self.postmaster.is_none() {
            return self.terminate();
        }
        let graceful = (|| {
            self.remember_descendants()?;
            if self.exited()? {
                return Ok(());
            }
            let postmaster = self
                .postmaster
                .ok_or_else(|| PgError::Process("postmaster not yet identified".into()))?;
            let pid = std::fs::read_to_string(data_dir.join("postmaster.pid"))
                .ok()
                .and_then(|text| text.lines().next()?.parse().ok());
            if pid != Some(self.processes[postmaster].pid) {
                return Err(PgError::Process(
                    "PID file does not identify an owned live process".into(),
                ));
            }
            // pg_ctl reopens the mutable PID file when signalling. Instead send
            // its fast-shutdown signal through the verified postmaster pidfd.
            self.processes[postmaster]
                .signal(Signal::INT)
                .map_err(|error| PgError::Process(format!("fast PostgreSQL shutdown: {error}")))?;
            if self
                .wait_exited(Duration::from_secs(2))
                .map_err(|error| PgError::Process(format!("wait PostgreSQL shutdown: {error}")))?
            {
                Ok(())
            } else {
                let pending = self
                    .processes
                    .iter()
                    .filter(|process| !process.reaped().unwrap_or(false))
                    .map(|process| {
                        let state = process.stat().map(|(state, _)| state).unwrap_or('?');
                        let wait = std::fs::read_to_string(format!("/proc/{}/wchan", process.pid))
                            .unwrap_or_default();
                        format!("{}:{state}:{}", process.pid, wait.trim())
                    })
                    .collect::<Vec<_>>()
                    .join(",");
                tracing::warn!(%pending,
                    "fast PostgreSQL shutdown budget exhausted; escalating owned cleanup");
                if self.processes[postmaster]
                    .exited()
                    .map_err(|error| PgError::Process(error.to_string()))?
                    && self
                        .postmaster_ancestors
                        .iter()
                        .any(|index| !self.processes[*index].exited().unwrap_or(false))
                {
                    return Err(PgError::Process(format!(
                        "fast PostgreSQL shutdown timed out; launcher outlived stopped postmaster: {pending}"
                    )));
                }
                // The grace budget is not the fence proof. terminate() below
                // must still prove successful supervisor exit and reaping of
                // every descendant; otherwise shutdown remains an error.
                Ok(())
            }
        })();
        let cleanup = self.terminate();
        match graceful {
            Ok(()) => cleanup,
            Err(error) => Err(error.with_cleanup(cleanup)),
        }
    }

    fn wait_exited(&mut self, timeout: Duration) -> io::Result<bool> {
        let deadline = Instant::now() + timeout;
        loop {
            // Only the owner reaps; observation must retain cleanup responsibility.
            let status = self.child.try_wait()?;
            let mut exited = true;
            let mut reaped = true;
            for process in &self.processes {
                exited &= process.exited()?;
                reaped &= process.reaped()?;
            }
            if let Some(status) = status {
                if !status.success() && (exited || Instant::now() >= deadline) {
                    return Err(io::Error::other(format!(
                        "PostgreSQL supervisor exited unexpectedly: {status}; descendant reaping unproven"
                    )));
                }
                if status.success() && reaped {
                    return Ok(true);
                }
            }
            if Instant::now() >= deadline {
                if status.is_some() {
                    return Err(io::Error::other(
                        "PostgreSQL supervisor exited without reaping all owned descendants",
                    ));
                }
                return Ok(false);
            }
            std::thread::sleep(POLL_INTERVAL);
        }
    }

    pub(crate) fn terminate(&mut self) -> Result<(), PgError> {
        let mut failures = Vec::new();
        // SIGQUIT lets the postmaster reap its backends, including when it is
        // behind a launcher. Escalation never depends on the async runtime.
        for signal in [Signal::QUIT, Signal::KILL] {
            let discovery = self.freeze_tree();
            if discovery.is_ok() && self.processes.len() == 1 {
                // Cancellation can arrive before the supervisor's spawn. With
                // no captured children, request protocol completion instead of
                // accepting a killed ownership root as proof of reaping. If
                // START was already in flight, the next pass captures its child.
                let cancelled = self.processes[0].exited().and_then(|exited| {
                    if exited {
                        Ok(())
                    } else {
                        self.command(crate::process_supervisor::STOP)
                    }
                });
                if let Err(error) = cancelled {
                    failures.push(error.to_string());
                }
            }
            for result in [
                discovery,
                self.signal_all(signal),
                self.signal_all(Signal::CONT),
            ] {
                if let Err(error) = result {
                    failures.push(error.to_string());
                }
            }
            match self.wait_exited(EXIT_TIMEOUT) {
                Ok(true) if failures.is_empty() => return Ok(()),
                Ok(true) => break,
                Ok(false) if signal == Signal::KILL => {
                    failures.push("owned PostgreSQL processes did not exit".into());
                }
                Ok(false) => {}
                Err(error) => failures.push(error.to_string()),
            }
        }
        Err(PgError::Process(format!(
            "terminate owned PostgreSQL: {}",
            failures.join("; ")
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncBufReadExt, BufReader};

    fn supervisor(script: &str) -> OwnedProcess {
        OwnedProcess::spawn(Command::new("sh").args(["-c", script])).unwrap()
    }

    #[cfg(feature = "testing")]
    #[tokio::test]
    async fn exhausted_fast_shutdown_budget_requires_proven_escalation() {
        let data = crate::testing::TestDataDir::new("stop-budget");
        let mut owned = supervisor("trap '' INT; echo $$; exec sleep 30");
        let (stdout, _stderr) = owned.take_output();
        owned.start().unwrap();
        let pid: u32 = BufReader::new(stdout)
            .lines()
            .next_line()
            .await
            .unwrap()
            .unwrap()
            .parse()
            .unwrap();
        owned.remember_descendants().unwrap();
        owned.postmaster = Some(
            owned
                .processes
                .iter()
                .position(|process| process.pid == pid)
                .unwrap(),
        );
        std::fs::write(data.path().join("postmaster.pid"), format!("{pid}\n")).unwrap();
        let start = Instant::now();
        owned.stop(data.path()).unwrap();
        assert!(
            start.elapsed() >= Duration::from_secs(2),
            "the mocked postmaster ignores fast shutdown"
        );
        assert!(
            start.elapsed() < Duration::from_secs(5),
            "existing escalation budgets remain bounded"
        );
        assert!(
            owned
                .processes
                .iter()
                .all(|process| process.reaped().unwrap())
        );
        owned.stop(data.path()).unwrap();
    }

    #[tokio::test]
    async fn undispatched_supervisor_cannot_launch_after_cleanup() {
        let mut owned = supervisor("exec sleep 30");
        let pid = owned.child.id().unwrap();
        owned.terminate().unwrap();
        owned.terminate().unwrap();
        assert_eq!(owned.processes.len(), 1);
        assert!(!Path::new(&format!("/proc/{pid}")).exists());
    }

    #[tokio::test]
    async fn signalled_supervisor_is_never_successful_cleanup() {
        let mut owned = supervisor("exec sleep 30");
        owned.child.kill().await.unwrap();
        for _ in 0..2 {
            let error = owned.terminate().unwrap_err();
            assert!(
                error.to_string().contains("supervisor exited unexpectedly"),
                "{error}"
            );
        }
        assert!(owned.processes[0].reaped().unwrap());
    }

    #[tokio::test]
    async fn exited_pidfd_is_not_proof_of_reaping() {
        // A std Child is not registered with Tokio's orphan reaper.
        let mut child = std::process::Command::new("sh")
            .args(["-c", "exit 0"])
            .spawn()
            .unwrap();
        let process = Process::open(child.id()).unwrap();
        let deadline = Instant::now() + EXIT_TIMEOUT;
        while !process.exited().unwrap() {
            assert!(Instant::now() < deadline);
            std::thread::sleep(POLL_INTERVAL);
        }
        assert!(!process.reaped().unwrap(), "a zombie is not reaped");
        child.wait().unwrap();
        assert!(process.exited().unwrap());
        assert!(process.reaped().unwrap());
    }

    #[tokio::test]
    async fn successful_supervisor_exit_cannot_hide_an_unreaped_descendant() {
        let mut zombie = std::process::Command::new("sh")
            .args(["-c", "exit 0"])
            .spawn()
            .unwrap();
        let process = Process::open(zombie.id()).unwrap();
        let mut owned = supervisor("exec sleep 30");
        owned.command(crate::process_supervisor::STOP).unwrap();
        // Model an incomplete ownership root: successful exit alone cannot
        // certify a retained descendant that is still a zombie.
        owned.processes.push(process);
        let error = owned.wait_exited(EXIT_TIMEOUT).unwrap_err();
        zombie.wait().unwrap();
        assert!(
            error
                .to_string()
                .contains("without reaping all owned descendants"),
            "{error}"
        );
        assert!(owned.wait_exited(EXIT_TIMEOUT).unwrap());
    }

    #[tokio::test]
    async fn cancellation_racing_start_still_reaps_the_launched_tree() {
        for _ in 0..4 {
            let mut owned = supervisor("exec sleep 30");
            owned.start().unwrap();
            owned.terminate().unwrap();
            assert!(
                owned
                    .processes
                    .iter()
                    .all(|process| process.reaped().unwrap())
            );
        }
    }

    #[tokio::test]
    async fn exited_pidfd_never_signals_a_replacement_numeric_pid() {
        let mut original = tokio::process::Command::new("sleep")
            .arg("30")
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let mut identity = Process::open(original.id().unwrap()).unwrap();
        original.kill().await.unwrap();
        let mut replacement = tokio::process::Command::new("sleep")
            .arg("30")
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        // Model numeric PID reuse without depending on the kernel's allocator.
        identity.pid = replacement.id().unwrap();
        assert!(identity.exited().unwrap());
        identity.signal(Signal::KILL).unwrap();
        identity.signal(Signal::CONT).unwrap();
        assert!(replacement.try_wait().unwrap().is_none());
        replacement.kill().await.unwrap();
    }

    #[tokio::test]
    async fn unresponsive_owned_child_is_killed_and_reaped_without_pid_file() {
        let mut owned = supervisor("trap '' QUIT; echo ready; while :; do :; done");
        let pid = owned.child.id().unwrap();
        let (stdout, _) = owned.take_output();
        owned.start().unwrap();
        let mut line = String::new();
        BufReader::new(stdout).read_line(&mut line).await.unwrap();
        assert_eq!(line, "ready\n");
        tokio::task::spawn_blocking(move || {
            owned.terminate().unwrap();
            owned.terminate().unwrap();
        })
        .await
        .unwrap();
        assert!(!Path::new(&format!("/proc/{pid}")).exists());
    }

    #[tokio::test]
    async fn live_launcher_does_not_delay_reaping_adopted_children() {
        let mut owned = supervisor("sh -c 'sleep 30 & echo $!'; sleep 30");
        let (stdout, _) = owned.take_output();
        owned.start().unwrap();
        let mut line = String::new();
        BufReader::new(stdout).read_line(&mut line).await.unwrap();
        let orphan = Process::open(line.trim().parse().unwrap()).unwrap();
        orphan.signal(Signal::TERM).unwrap();
        let reaped = tokio::time::timeout(EXIT_TIMEOUT, async {
            while !orphan.reaped().unwrap() {
                tokio::time::sleep(POLL_INTERVAL).await;
            }
        })
        .await;
        let launcher_alive = !owned.exited().unwrap();
        owned.terminate().unwrap();
        assert!(launcher_alive);
        reaped.expect("adopted child must be reaped while the launcher is still alive");
    }
}
