use super::SupervisorArgs;
use super::worker_args::{live_priority_normalize_args, normalize_args, realtime_args};
use crate::log_stream;
use serde_json::json;
use std::error::Error;
use std::process::ExitStatus;
use std::time::Duration;
use tokio::process::{Child, Command};
use tokio::task::JoinHandle;

pub(super) struct RealtimeChild {
    venue: String,
    child: Child,
}

pub(super) async fn wait_optional_child(
    child: &mut Option<Child>,
) -> Option<std::io::Result<ExitStatus>> {
    match child {
        Some(child) => Some(child.wait().await),
        None => std::future::pending().await,
    }
}

pub(super) async fn shutdown_signal(task: &JoinHandle<()>) {
    while !task.is_finished() {
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

pub(super) fn spawn_realtime_children(
    args: &SupervisorArgs,
) -> Result<Vec<RealtimeChild>, Box<dyn Error>> {
    let mut children = Vec::new();
    for venue in &args.realtime_venues {
        let mut command = Command::new(&args.realtime_bin);
        command.args(realtime_args(args, venue));
        children.push(RealtimeChild {
            venue: venue.clone(),
            child: spawn_child(&format!("realtime-{venue}"), command)?,
        });
    }
    Ok(children)
}

pub(super) async fn wait_any_realtime_child(
    children: &mut [RealtimeChild],
) -> std::io::Result<(String, ExitStatus)> {
    loop {
        for child in children.iter_mut() {
            if let Some(status) = child.child.try_wait()? {
                return Ok((child.venue.clone(), status));
            }
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

pub(super) fn spawn_normalize(args: &SupervisorArgs) -> Result<Child, Box<dyn Error>> {
    let mut command = Command::new(&args.normalize_bin);
    command.args(normalize_args(args));
    spawn_child("normalize", command)
}

pub(super) fn spawn_live_priority_normalize(
    args: &SupervisorArgs,
) -> Result<Child, Box<dyn Error>> {
    let mut command = Command::new(&args.normalize_bin);
    command.args(live_priority_normalize_args(args));
    spawn_child("live-priority-normalize", command)
}

pub(super) fn spawn_normalize_for_phase(
    args: &SupervisorArgs,
    bootstrap_active: bool,
) -> Result<Child, Box<dyn Error>> {
    if bootstrap_active {
        spawn_live_priority_normalize(args)
    } else {
        spawn_normalize(args)
    }
}

fn spawn_child(role: &str, mut command: Command) -> Result<Child, Box<dyn Error>> {
    log_stream::info(
        "crypto_market_ingest_worker_spawn",
        json!({ "worker_role": role }),
    )?;
    Ok(command.kill_on_drop(true).spawn()?)
}

pub(super) async fn kill_child(child: &mut Child) {
    if matches!(child.try_wait(), Ok(Some(_))) {
        return;
    }
    let _ = child.kill().await;
}

pub(super) async fn kill_realtime_children(children: &mut [RealtimeChild]) {
    for child in children {
        kill_child(&mut child.child).await;
    }
}

pub(super) async fn kill_optional_child(child: &mut Option<Child>) {
    if let Some(child) = child {
        kill_child(child).await;
    }
}
