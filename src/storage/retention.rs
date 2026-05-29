mod config;
mod planner;
mod runner;

pub use config::{
    S3RetentionConfig, S3RetentionStats, default_l0_retention_prefixes,
    default_l1_retention_prefixes, l0_s3_retention_config, l1_s3_retention_config,
};
pub use runner::run_s3_retention_once;

#[cfg(test)]
mod tests;
