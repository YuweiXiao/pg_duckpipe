//! Logging initialization — shared tracing-subscriber setup for daemon and PG worker.
//!
//! Both entry points call [`init_subscriber`] once at startup.  The default
//! filter is derived from the `debug` flag; `RUST_LOG` overrides it at runtime.

/// Initialize the global tracing subscriber.
///
/// - `debug`: enables `DEBUG` level for `duckpipe*` crates; everything else stays `INFO`.
/// - `RUST_LOG` environment variable overrides the default filter when set.
pub fn init_subscriber(debug: bool) {
    let filter = if debug {
        "duckpipe=debug,duckpipe_core=debug,info"
    } else {
        "duckpipe=info,duckpipe_core=info,warn"
    };
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(filter)),
        )
        .with_target(false)
        .init();
}
