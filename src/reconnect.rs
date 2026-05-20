use std::time::Duration;

pub const DEFAULT_INITIAL_BACKOFF_SECS: u64 = 1;
pub const DEFAULT_MAX_BACKOFF_SECS: u64 = 60;
pub const DEFAULT_BACKOFF_MULTIPLIER: u32 = 2;
pub const DEFAULT_STALE_MESSAGE_TIMEOUT_SECS: u64 = 30;

#[derive(Debug, Clone, Copy)]
pub struct ReconnectPolicy {
    pub initial_backoff: Duration,
    pub max_backoff: Duration,
    pub multiplier: u32,
    pub stale_message_timeout: Duration,
}

impl ReconnectPolicy {
    pub fn default_24x7() -> Self {
        Self {
            initial_backoff: Duration::from_secs(DEFAULT_INITIAL_BACKOFF_SECS),
            max_backoff: Duration::from_secs(DEFAULT_MAX_BACKOFF_SECS),
            multiplier: DEFAULT_BACKOFF_MULTIPLIER,
            stale_message_timeout: Duration::from_secs(DEFAULT_STALE_MESSAGE_TIMEOUT_SECS),
        }
    }

    pub fn next_backoff(&self, current: Duration) -> Duration {
        let candidate = current
            .checked_mul(self.multiplier)
            .unwrap_or(self.max_backoff);
        std::cmp::min(candidate, self.max_backoff)
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct ReconnectState {
    pub reconnect_count: u64,
    pub last_reconnect_at_ms: Option<i64>,
    pub consecutive_failures: u32,
}

impl ReconnectState {
    pub fn record_reconnect(&mut self, now_ms: i64) {
        self.reconnect_count += 1;
        self.last_reconnect_at_ms = Some(now_ms);
        self.consecutive_failures = self.consecutive_failures.saturating_add(1);
    }

    pub fn reset_after_healthy_run(&mut self) {
        self.consecutive_failures = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn next_backoff_doubles_until_cap() {
        let policy = ReconnectPolicy {
            initial_backoff: Duration::from_secs(1),
            max_backoff: Duration::from_secs(60),
            multiplier: 2,
            stale_message_timeout: Duration::from_secs(30),
        };
        assert_eq!(
            policy.next_backoff(Duration::from_secs(1)),
            Duration::from_secs(2)
        );
        assert_eq!(
            policy.next_backoff(Duration::from_secs(2)),
            Duration::from_secs(4)
        );
        assert_eq!(
            policy.next_backoff(Duration::from_secs(32)),
            Duration::from_secs(60)
        );
        assert_eq!(
            policy.next_backoff(Duration::from_secs(60)),
            Duration::from_secs(60)
        );
    }

    #[test]
    fn next_backoff_caps_when_overflow() {
        let policy = ReconnectPolicy {
            initial_backoff: Duration::from_secs(1),
            max_backoff: Duration::from_secs(60),
            multiplier: u32::MAX,
            stale_message_timeout: Duration::from_secs(30),
        };
        assert_eq!(
            policy.next_backoff(Duration::from_secs(u64::MAX / 2)),
            Duration::from_secs(60)
        );
    }

    #[test]
    fn record_reconnect_increments_counters() {
        let mut state = ReconnectState::default();
        state.record_reconnect(1_700_000_000_000);
        state.record_reconnect(1_700_000_001_000);
        assert_eq!(state.reconnect_count, 2);
        assert_eq!(state.last_reconnect_at_ms, Some(1_700_000_001_000));
        assert_eq!(state.consecutive_failures, 2);
    }

    #[test]
    fn reset_after_healthy_run_clears_failures_only() {
        let mut state = ReconnectState::default();
        state.record_reconnect(1);
        state.record_reconnect(2);
        state.reset_after_healthy_run();
        assert_eq!(state.reconnect_count, 2);
        assert_eq!(state.consecutive_failures, 0);
        assert_eq!(state.last_reconnect_at_ms, Some(2));
    }

    #[test]
    fn default_24x7_matches_documented_parameters() {
        let policy = ReconnectPolicy::default_24x7();
        assert_eq!(policy.initial_backoff, Duration::from_secs(1));
        assert_eq!(policy.max_backoff, Duration::from_secs(60));
        assert_eq!(policy.multiplier, 2);
        assert_eq!(policy.stale_message_timeout, Duration::from_secs(30));
    }
}
