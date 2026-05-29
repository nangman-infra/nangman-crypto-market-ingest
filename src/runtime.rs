use crate::maintenance::{
    log_unsealed_orphan_cleanup, spawn_eviction_loop, spawn_l0_s3_retention_loop,
};
use crate::venue_runner::{run_binance, run_upbit};
use market_ingest_app::args::{Venue, parse_args, print_help};
use std::env;
use std::error::Error;

pub(crate) async fn run() -> Result<(), Box<dyn Error>> {
    let Some(args) = parse_args(env::args().skip(1))? else {
        print_help();
        return Ok(());
    };

    log_unsealed_orphan_cleanup(&args);
    let eviction_handle = spawn_eviction_loop(&args);
    let retention_handle = spawn_l0_s3_retention_loop(&args);

    let result = match args.venue {
        Venue::Binance => run_binance(args).await,
        Venue::Upbit => run_upbit(args).await,
    };

    for handle in [eviction_handle, retention_handle].into_iter().flatten() {
        handle.abort();
        let _ = handle.await;
    }
    result
}
