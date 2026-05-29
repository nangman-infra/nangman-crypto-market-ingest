mod defaults;
mod env_config;
mod help;
mod parse;
mod types;
mod validation;

pub use help::print_help;
pub use parse::parse_args;
pub use types::SupervisorArgs;

pub(super) const DEFAULT_L0_S3_BUCKET: &str = defaults::DEFAULT_L0_S3_BUCKET;
pub(super) const DEFAULT_L1_S3_BUCKET: &str = defaults::DEFAULT_L1_S3_BUCKET;
pub(super) const DEFAULT_L0_RUN_KEY_OVERLAP_MS: i64 = defaults::DEFAULT_L0_RUN_KEY_OVERLAP_MS;
