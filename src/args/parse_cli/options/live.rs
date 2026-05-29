use super::super::super::Args;
use super::super::required_arg;
use std::error::Error;

pub(super) fn apply_option(
    arg: &str,
    args: &mut impl Iterator<Item = String>,
    parsed: &mut Args,
) -> Result<(), Box<dyn Error>> {
    match arg {
        "--live-nats-url" => {
            parsed.live_nats_url = Some(required_arg(
                args,
                "--live-nats-url requires a nats:// URL",
            )?);
        }
        "--live-nats-stream" => {
            parsed.live_nats_stream =
                required_arg(args, "--live-nats-stream requires a stream name")?;
        }
        "--live-nats-subject-prefix" => {
            parsed.live_nats_subject_prefix =
                required_arg(args, "--live-nats-subject-prefix requires a subject prefix")?;
        }
        "--live-nats-required" => {
            parsed.live_nats_required = true;
        }
        _ => unreachable!("live option dispatch mismatch: {arg}"),
    }
    Ok(())
}
