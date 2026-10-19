//! Child-process management: bounded capture, process-group kill, reaping.

use std::future::Future;
use std::pin::Pin;
use std::sync::OnceLock;
use std::time::Duration;

use super::command_environment::CommandEnvironment;
use super::group_exit;
use super::ProcessCleanup;
use super::process_cleanup::Reaped;
use crate::exec::ExecProgress;

type BoxTask = Pin<Box<dyn Future<Output = std::io::Result<std::process::Output>> + Send>>;

static TASK_WRAPPER: OnceLock<fn(BoxTask) -> BoxTask> = OnceLock::new();

/// Register a wrapper applied to every internal waiter task before it is
/// spawned, so a host can carry its ambient task-locals (request context,
/// identity) into it. First registration wins; returns whether it took effect.
pub fn set_task_wrapper(wrapper: fn(BoxTask) -> BoxTask) -> bool {
    TASK_WRAPPER.set(wrapper).is_ok()
}

fn spawn_wrapped(
    fut: impl Future<Output = std::io::Result<std::process::Output>> + Send + 'static,
) -> tokio::task::JoinHandle<std::io::Result<std::process::Output>> {
    let fut: BoxTask = Box::pin(fut);
    tokio::spawn(match TASK_WRAPPER.get() {
        Some(wrap) => wrap(fut),
        None => fut,
    })
}

/// Run `cmd` to completion with its output captured, or kill it -- and every
/// process it started -- when `deadline` passes.
///
/// `tokio::time::timeout(deadline, cmd.output())` only abandons the future.
/// Without `kill_on_drop` the child is not even signalled, and a shell's
/// pipeline (`grep -r … | head`) is a set of grandchildren that no kill of the
/// direct child reaches anyway. One `grep -rl … /` the shell tool had reported
/// as "timed out after 600s and was killed" ran on for half an hour at a full
/// core inside a two-CPU container, starving the agent that had started it.
///
/// So the child is spawned as the leader of its own process group and the
/// whole group is signalled when the deadline fires. The result has the same
/// shape as `timeout(deadline, cmd.output())`, so a caller's match arms do not
/// change. Stdio is set the way `output()` sets it: stdin closed, both output
/// streams captured.
pub async fn output_or_kill(
    cmd: &mut tokio::process::Command,
    deadline: Duration,
) -> Result<std::io::Result<std::process::Output>, tokio::time::error::Elapsed> {
    output_or_kill_with_progress(cmd, deadline, None).await
}

/// `output_or_kill` with streamed bytes sent to the supplied progress meter.
pub async fn output_or_kill_with_progress(
    cmd: &mut tokio::process::Command,
    deadline: Duration,
    progress: Option<ExecProgress>,
) -> Result<std::io::Result<std::process::Output>, tokio::time::error::Elapsed> {
    let cleanup = ProcessCleanup::default();
    match cleanup
        .scope(tokio::time::timeout(deadline, output_unbounded_with_progress(cmd, progress)))
        .await
    {
        Ok(result) => Ok(result),
        Err(elapsed) => {
            cleanup.wait().await;
            Err(elapsed)
        }
    }
}

/// Capture a command with no deadline, killing its process group if the
/// future is dropped. The child is reaped by an owned waiter even when the
/// caller cancels; a scoped [`super::ProcessCleanup`] can await that waiter.
pub async fn output_unbounded(
    cmd: &mut tokio::process::Command,
) -> std::io::Result<std::process::Output> {
    output_with_input_and_progress(cmd, None, None).await
}

/// Capture a command with no deadline while reporting output chunks.
pub async fn output_unbounded_with_progress(
    cmd: &mut tokio::process::Command,
    progress: Option<ExecProgress>,
) -> std::io::Result<std::process::Output> {
    output_with_input_and_progress(cmd, None, progress).await
}

/// Capture a command, optionally supplying stdin, in the current cleanup scope.
pub async fn output_with_input(
    cmd: &mut tokio::process::Command,
    input: Option<Vec<u8>>,
) -> std::io::Result<std::process::Output> {
    output_with_input_and_progress(cmd, input, None).await
}

/// Capture a command, optionally supplying stdin, and stream output bytes to
/// the execution progress meter.
pub async fn output_with_input_and_progress(
    cmd: &mut tokio::process::Command,
    input: Option<Vec<u8>>,
    progress: Option<ExecProgress>,
) -> std::io::Result<std::process::Output> {
    use std::process::Stdio;
    cmd.stdin(if input.is_some() {
        Stdio::piped()
    } else {
        Stdio::null()
    })
    .stdout(Stdio::piped())
    .stderr(Stdio::piped())
    .kill_on_drop(true);
    CommandEnvironment::apply(cmd);
    own_process_group(cmd.as_std_mut());
    let mut child = cmd.spawn()?;
    let stdin = child.stdin.take();
    let reaped = Reaped::register();
    let (cancel, cancellation) = tokio::sync::watch::channel(false);
    let waiter = spawn_wrapped(async move {
        let _reaped = reaped;
        let write_input = async move {
            if let (Some(mut stdin), Some(input)) = (stdin, input) {
                use tokio::io::AsyncWriteExt;
                // An early child exit closes stdin. Its exit status/output is
                // authoritative; a broken pipe must not hide it.
                let _ = stdin.write_all(&input).await;
            }
        };
        let (_, output) = tokio::join!(
            write_input,
            collect_command_output(child, cancellation, progress)
        );
        output
    });
    let _cancel_on_drop = CancelOnDrop(cancel);
    waiter.await.map_err(std::io::Error::other)?
}

// The caller signals only the owned waiter. It never retains a PID after
// that waiter reaps the child, so late future drops cannot kill a reused PID.
struct CancelOnDrop(tokio::sync::watch::Sender<bool>);

impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        self.0.send_replace(true);
    }
}

struct CommandGroup(Option<u32>);

impl CommandGroup {
    fn kill(&self) {
        if let Some(pid) = self.0 {
            kill_process_group(pid);
        }
    }
}

impl Drop for CommandGroup {
    fn drop(&mut self) {
        self.kill();
    }
}

async fn collect_command_output(
    mut child: tokio::process::Child,
    mut cancellation: tokio::sync::watch::Receiver<bool>,
    progress: Option<ExecProgress>,
) -> std::io::Result<std::process::Output> {
    let mut group = CommandGroup(child.id());
    let mut stdout = child
        .stdout
        .take()
        .ok_or_else(|| std::io::Error::other("command stdout is not piped"))?;
    let mut stderr = child
        .stderr
        .take()
        .ok_or_else(|| std::io::Error::other("command stderr is not piped"))?;
    let mut stdout_bytes = Vec::new();
    let mut stderr_bytes = Vec::new();
    {
        let drain = async {
            tokio::try_join!(
                read_stream(&mut stdout, &mut stdout_bytes, progress.as_ref()),
                read_stream(&mut stderr, &mut stderr_bytes, progress.as_ref()),
            )
        };
        tokio::pin!(drain);
        tokio::select! {
            biased;
            _ = async { let _ = cancellation.wait_for(|cancelled| *cancelled).await; } => {
                // The leader has not been reaped, even if it already exited.
                // Its PID cannot be reused while signalling this group.
                group.kill();
                child.start_kill()?;
                if let Ok(result) = tokio::time::timeout(Duration::from_secs(2), drain).await {
                    result?;
                }
            }
            result = &mut drain => { result?; }
        }
    }
    let status = tokio::select! {
        biased;
        _ = async { let _ = cancellation.wait_for(|cancelled| *cancelled).await; } => {
            group.kill();
            child.start_kill()?;
            // Pipe EOF and the shell's exit can precede a descendant's exit.
            // Keep the leader unreaped while checking the reserved group.
            let stopped = match group.0 {
                Some(pid) => group_exit::wait(pid).await,
                None => Ok(()),
            };
            let status = child.wait().await?;
            // Disarm before propagating a check error after reaping: its PID
            // could now be reused, including while unwinding this function.
            group.0 = None;
            stopped?;
            status
        }
        result = child.wait() => result?,
    };
    // No await between reaping and disarming. Only this waiter owns the PID.
    group.0 = None;
    Ok(std::process::Output {
        status,
        stdout: stdout_bytes,
        stderr: stderr_bytes,
    })
}

async fn read_stream(
    reader: &mut (impl tokio::io::AsyncRead + Unpin),
    output: &mut Vec<u8>,
    progress: Option<&ExecProgress>,
) -> std::io::Result<()> {
    use tokio::io::AsyncReadExt;
    let mut chunk = [0; 8_192];
    loop {
        let read = reader.read(&mut chunk).await?;
        if read == 0 {
            return Ok(());
        }
        output.extend_from_slice(&chunk[..read]);
        if let Some(progress) = progress {
            progress.record(&chunk[..read]);
        }
    }
}

/// Make `cmd` the leader of a new process group when it is spawned, so that
/// [`kill_process_group`] can reach everything it starts. A no-op off Unix.
pub fn own_process_group(cmd: &mut std::process::Command) {
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        cmd.process_group(0);
    }
    #[cfg(not(unix))]
    {
        let _ = cmd;
    }
}

/// Send SIGKILL to the process group led by `pid` -- a child spawned via
/// [`own_process_group`] and everything it started. Off Unix the direct child
/// is what `kill_on_drop` reaches and no group exists to signal.
pub fn kill_process_group(pid: u32) {
    #[cfg(unix)]
    {
        use rustix::process::{Pid, Signal};
        let Some(pid) = i32::try_from(pid).ok().and_then(Pid::from_raw) else {
            return;
        };
        // A signal to a process group this process created; the kernel
        // validates the target. A failure (group already gone) is expected.
        let _ = rustix::process::kill_process_group(pid, Signal::KILL);
    }
    #[cfg(not(unix))]
    {
        let _ = pid;
    }
}
