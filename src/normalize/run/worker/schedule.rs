use crate::normalize::args::NormalizeArgs;
use std::error::Error;
use std::time::Duration;

pub(super) fn worker_sleep_duration(args: &NormalizeArgs) -> Result<Duration, Box<dyn Error>> {
    let sleep_ms = if args.live_priority_only {
        args.schedule_interval_ms.min(60_000)
    } else {
        args.schedule_interval_ms
    };
    Ok(Duration::from_millis(u64::try_from(sleep_ms)?))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args() -> NormalizeArgs {
        let raw = vec![
            "--l0-s3-bucket".to_owned(),
            "l0".to_owned(),
            "--l1-s3-bucket".to_owned(),
            "l1".to_owned(),
        ];
        crate::normalize::args::parse_args(raw.into_iter())
            .unwrap()
            .unwrap()
    }

    #[test]
    fn worker_sleep_uses_configured_schedule_for_regular_worker() {
        let mut args = args();
        args.schedule_interval_ms = 900_000;

        let duration = worker_sleep_duration(&args).unwrap();

        assert_eq!(duration, Duration::from_millis(900_000));
    }

    #[test]
    fn worker_sleep_caps_live_priority_only_worker_to_one_minute() {
        let mut args = args();
        args.schedule_interval_ms = 900_000;
        args.live_priority_only = true;

        let duration = worker_sleep_duration(&args).unwrap();

        assert_eq!(duration, Duration::from_millis(60_000));
    }
}
