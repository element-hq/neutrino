use std::path::Path;
use std::sync::{Once, OnceLock};
use tracing_subscriber::{
    Layer, filter::FilterExt, layer::SubscriberExt, registry::LookupSpan, util::SubscriberInitExt,
};

#[cfg(not(target_os = "android"))]
const DEFAULT_FILTER: &str =
    "neutrino_event=info,neutrino_http=info,neutrino_main=info,neutrino_ffi=info";

/// Filename prefix for the rotating log files, e.g. `neutrino.2026-08-04-07`.
/// Deliberately NOT the host's own prefix (the embedded build's bug reporter
/// rotates `logs.*` in the same directory for the matrix-rust-sdk): two
/// rotators sharing a prefix would prune each other's files, and the reporter
/// collects the whole directory either way.
const FILE_LOG_PREFIX: &str = "neutrino";

/// How many hourly files to keep before the appender prunes the oldest. A day
/// of history: enough to cover a bug reported well after the fact, and bounded
/// so the set stays inside a bug report's upload budget.
const FILE_LOG_MAX_FILES: usize = 24;

// Compact, Logcat-friendly event formatter, ported from matrix-rust-sdk's
// `EventFormatter::for_logcat`. Logcat already records the timestamp and
// priority for every line, so we omit both and emit just:
//   `target: message | file:line | spans: a{..} > b{..}`
// Paths under the crates.io registry are shortened to `<crates.io>/…` so a
// dependency's frame doesn't drown the line in the cargo cache prefix.
#[cfg(target_os = "android")]
mod logcat_format {
    use tracing::{Event, Subscriber};
    use tracing_subscriber::{
        fmt::{FmtContext, FormatEvent, FormatFields, FormattedFields, format::Writer},
        registry::LookupSpan,
    };

    pub(crate) struct LogcatEventFormatter;

    impl LogcatEventFormatter {
        fn write_filename(writer: &mut Writer<'_>, filename: &str) -> std::fmt::Result {
            const CRATES_IO_PATH_MATCHER: &str = ".cargo/registry/src/index.crates.io";
            let crates_io_filename = filename
                .split_once(CRATES_IO_PATH_MATCHER)
                .and_then(|(_, rest)| rest.split_once('/').map(|(_, rest)| rest));

            if let Some(filename) = crates_io_filename {
                writer.write_str("<crates.io>/")?;
                writer.write_str(filename)
            } else {
                writer.write_str(filename)
            }
        }
    }

    impl<S, N> FormatEvent<S, N> for LogcatEventFormatter
    where
        S: Subscriber + for<'a> LookupSpan<'a>,
        N: for<'a> FormatFields<'a> + 'static,
    {
        fn format_event(
            &self,
            ctx: &FmtContext<'_, S, N>,
            mut writer: Writer<'_>,
            event: &Event<'_>,
        ) -> std::fmt::Result {
            let meta = event.metadata();

            write!(writer, "{}: ", meta.target())?;

            ctx.format_fields(writer.by_ref(), event)?;

            if let Some(filename) = meta.file() {
                writer.write_str(" | ")?;
                Self::write_filename(&mut writer, filename)?;
                if let Some(line_number) = meta.line() {
                    write!(writer, ":{line_number}")?;
                }
            }

            if let Some(scope) = ctx.event_scope() {
                writer.write_str(" | spans: ")?;

                let mut first = true;

                for span in scope.from_root() {
                    if !first {
                        writer.write_str(" > ")?;
                    }

                    first = false;

                    write!(writer, "{}", span.name())?;

                    if let Some(fields) = &span.extensions().get::<FormattedFields<N>>()
                        && !fields.is_empty()
                    {
                        write!(writer, "{{{fields}}}")?;
                    }
                }
            }

            writeln!(writer)
        }
    }
}

/// The always-on log sink for this host: stdout off-device.
///
/// Filtered per-layer rather than registry-wide, because the file sink brings
/// its own filter and a registry-wide one would gate both to whichever is
/// narrower.
#[cfg(not(target_os = "android"))]
fn platform_layer<S>() -> Box<dyn Layer<S> + Send + Sync>
where
    S: tracing::Subscriber + for<'a> LookupSpan<'a>,
{
    Box::new(
        tracing_subscriber::fmt::layer().with_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| DEFAULT_FILTER.into()),
        ),
    )
}

/// The always-on log sink for this host: logcat on Android.
#[cfg(target_os = "android")]
fn platform_layer<S>() -> Box<dyn Layer<S> + Send + Sync>
where
    S: tracing::Subscriber + for<'a> LookupSpan<'a>,
{
    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_ansi(false)
        .event_format(logcat_format::LogcatEventFormatter)
        .with_writer(paranoid_android::AndroidLogMakeWriter::new(
            "io.element.neutrino".to_owned(),
        ));

    // Default (no RUST_LOG): every level from every `neutrino*` target — all of
    // our crates (`neutrino_event`, `neutrino_ffi`, …) plus any custom `target:`
    // — PLUS the federation transport stack we depend on. A raw string prefix is
    // the only way to express the `neutrino*` rule: EnvFilter/Targets match on
    // `::`-delimited path segments, so a `neutrino` directive would NOT match
    // `neutrino_ffi`. A new neutrino crate is picked up automatically.
    //
    // The embedded federation medium is injected from out-of-tree crates
    // (`iroh_ble_transport`, `blew`, `iroh` — the iroh/BLE composition).
    // Without their targets, BLE advertise/scan/discovery/connect failures
    // are completely invisible — the entire transport is silent, which is
    // exactly the class of failure we must never hide. The prefixes below
    // are plain strings, not dependencies: they cost nothing when the
    // medium isn't linked in, and keep it debuggable when it is. Include
    // the BLE stack down to DEBUG and the QUIC layer at INFO (its
    // DEBUG/TRACE is per-packet noise). Note `Level::TRACE > … > Level::ERROR`, so `level <= DEBUG` keeps
    // everything except TRACE. `RUST_LOG` still overrides the whole thing.
    // `boxed()` is disambiguated via UFCS: both `FilterExt` and `Layer` define it
    // for these types (EnvFilter/FilterFn are each both a filter and a layer), so
    // method syntax is ambiguous — we want the `Filter` one.
    let filter = match tracing_subscriber::EnvFilter::try_from_default_env() {
        Ok(env) => FilterExt::boxed(env),
        Err(_) => FilterExt::boxed(tracing_subscriber::filter::filter_fn(|meta| {
            let target = meta.target();
            if target.starts_with("neutrino") {
                true
            } else if target.starts_with("iroh_ble_transport") || target.starts_with("blew") {
                *meta.level() <= tracing::Level::DEBUG
            } else if target.starts_with("iroh") {
                *meta.level() <= tracing::Level::INFO
            } else {
                false
            }
        })),
    };

    Box::new(fmt_layer.with_filter(filter))
}

/// The file sink's default admission rule (no `RUST_LOG`): our own crates only,
/// DEBUG and above. A raw string prefix is the only way to express the
/// `neutrino*` rule — `EnvFilter`/`Targets` match on `::`-delimited path
/// segments, so a `neutrino` directive would NOT match `neutrino_ffi` — and it
/// picks up a new neutrino crate for free. Note `Level::TRACE > … >
/// Level::ERROR`, so `level <= DEBUG` is "everything except TRACE".
fn keeps_in_file_sink(target: &str, level: &tracing::Level) -> bool {
    target.starts_with("neutrino") && *level <= tracing::Level::DEBUG
}

/// Rotating-file sink for `log_dir`, filtered independently of the platform
/// sink, plus the guard that must outlive it (see below). `Err` carries a
/// ready-to-log reason: a directory we cannot write must cost us the file sink
/// only, never the platform sink, so the caller installs the subscriber anyway
/// and reports this afterwards.
type FileSink<S> = (
    Box<dyn Layer<S> + Send + Sync>,
    tracing_appender::non_blocking::WorkerGuard,
);

fn file_layer<S>(dir: &Path) -> Result<FileSink<S>, String>
where
    S: tracing::Subscriber + for<'a> LookupSpan<'a>,
{
    // The host normally creates this (Android's bug reporter mkdirs its log
    // directory at startup), but the server must not depend on having been
    // beaten to it.
    std::fs::create_dir_all(dir).map_err(|e| format!("cannot create {}: {e}", dir.display()))?;

    let appender = tracing_appender::rolling::Builder::new()
        .rotation(tracing_appender::rolling::Rotation::HOURLY)
        .filename_prefix(FILE_LOG_PREFIX)
        .max_log_files(FILE_LOG_MAX_FILES)
        .build(dir)
        .map_err(|e| format!("cannot open a log file in {}: {e}", dir.display()))?;

    // Writes move to a worker thread: the server's async tasks must never block
    // on file I/O to emit a log line. The cost is that the returned guard has to
    // outlive every logging call — dropping it stops the worker, silently
    // truncating the tail of the log, which is exactly the part a bug report
    // needs. Handed back to the caller, which owns process-lifetime state.
    let (writer, guard) = tracing_appender::non_blocking(appender);

    // Full `fmt` output, unlike the Logcat formatter above: a file carries no
    // out-of-band timestamp or level, so both have to be in the line.
    //
    // The default filter is deliberately tighter than the platform sink's: this
    // keeps a day of history that a bug report uploads, so TRACE from every
    // `neutrino*` target would blow the report's size budget for noise. DEBUG
    // and above only, and none of the transport crates' targets. `RUST_LOG`
    // still overrides it, and overrides both sinks identically.
    let filter = match tracing_subscriber::EnvFilter::try_from_default_env() {
        Ok(env) => FilterExt::boxed(env),
        Err(_) => FilterExt::boxed(tracing_subscriber::filter::filter_fn(|meta| {
            keeps_in_file_sink(meta.target(), meta.level())
        })),
    };

    Ok((
        Box::new(
            tracing_subscriber::fmt::layer()
                .with_ansi(false)
                .with_writer(writer)
                .with_filter(filter),
        ),
        guard,
    ))
}

pub fn init_tracing(log_dir: Option<&Path>) {
    // Guarded as a whole, not just by `try_init`: a second call — two nodes in
    // one test process, or `entrypoint` after the FFI's own call — must not
    // build a second file appender whose layer `try_init` would then discard,
    // leaving an orphaned worker thread writing to a file nobody reads.
    static INIT: Once = Once::new();
    INIT.call_once(|| init_tracing_once(log_dir));
}

fn init_tracing_once(log_dir: Option<&Path>) {
    // Built before the subscriber is installed, so a failure here cannot be
    // logged yet — carry the reason out and report it once logging works.
    let mut file_error = None;
    let file = log_dir.and_then(|dir| match file_layer(dir) {
        Ok((layer, guard)) => {
            // The writer's flush-on-drop guard, kept for the process lifetime:
            // this function runs at most once, so `set` cannot lose a guard that
            // an installed layer still depends on.
            static GUARD: OnceLock<tracing_appender::non_blocking::WorkerGuard> = OnceLock::new();
            let _ = GUARD.set(guard);
            Some(layer)
        }
        Err(reason) => {
            file_error = Some(reason);
            None
        }
    });

    // `try_init` (not `init`): idempotent, so a second `entrypoint` in the same
    // process — two nodes in a test, or a re-entrant embed — is a no-op rather
    // than a panic on the already-set global subscriber.
    //
    // Only `platform_layer` is `cfg`-split; this composition is shared, so the
    // generic plumbing that carries the file sink is typechecked on every host
    // rather than only when building for Android.
    let _ = tracing_subscriber::registry()
        .with(platform_layer())
        .with(file)
        .try_init();

    install_panic_logger();

    // Now that a sink exists, say why the other one doesn't. A host that asked
    // for file logs and silently didn't get them would collect bug reports with
    // no server logs in them and no hint as to why.
    if let Some(reason) = file_error {
        tracing::error!(target: "neutrino_main", "log file sink disabled: {reason}");
    }
}

/// Route panics through the tracing subscriber installed above (and so to logcat
/// on Android), once. Without this a panic on a server task — an `unwrap` in a
/// serve future, say — unwinds to Rust's default hook, which writes to stderr;
/// Android discards stderr, so the server would vanish (its listener dropped)
/// with nothing in the log. The previous hook is chained so any host-installed
/// reporting (e.g. matrix-rust-sdk's) still runs.
fn install_panic_logger() {
    use std::sync::Once;
    static HOOK: Once = Once::new();
    HOOK.call_once(|| {
        let previous = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            let location = info
                .location()
                .map(|l| l.to_string())
                .unwrap_or_else(|| "unknown".to_owned());
            let message = info
                .payload()
                .downcast_ref::<&str>()
                .copied()
                .or_else(|| info.payload().downcast_ref::<String>().map(String::as_str))
                .unwrap_or("<non-string panic payload>");
            // `target: "neutrino_main"` so the Android filter (targets starting
            // with `neutrino`) captures it.
            tracing::error!(target: "neutrino_main", %location, "panic in neutrino server: {message}");
            previous(info);
        }));
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use tracing::Level;
    use tracing_subscriber::registry::Registry;

    #[test]
    fn file_sink_admits_our_crates_from_debug_upwards() {
        assert!(keeps_in_file_sink("neutrino_main", &Level::ERROR));
        assert!(keeps_in_file_sink("neutrino_main", &Level::DEBUG));
        // A prefix rule, so a crate added later needs no change here.
        assert!(keeps_in_file_sink("neutrino_not_yet_written", &Level::INFO));
    }

    #[test]
    fn file_sink_rejects_trace_and_foreign_targets() {
        // TRACE is where the per-packet noise lives; a day of it would crowd out
        // the useful lines in a bug report's upload budget.
        assert!(!keeps_in_file_sink("neutrino_lb", &Level::TRACE));
        // The platform sink keeps the transport crates' targets; the file sink
        // deliberately does not.
        assert!(!keeps_in_file_sink("iroh_ble_transport", &Level::DEBUG));
        assert!(!keeps_in_file_sink("blew", &Level::ERROR));
        assert!(!keeps_in_file_sink("hyper", &Level::ERROR));
        // A near-miss must not sneak in on a substring match.
        assert!(!keeps_in_file_sink("not_neutrino_at_all", &Level::ERROR));
    }

    #[test]
    fn file_layer_creates_the_directory_and_opens_a_prefixed_file() {
        let tmp = tempfile::tempdir().expect("tempdir");
        // Nested and absent: the host normally makes this, but we must not
        // require it to have got there first.
        let dir = tmp.path().join("cache/logs");
        let (_layer, _guard) = file_layer::<Registry>(&dir).expect("file layer");

        assert!(dir.is_dir(), "log directory should have been created");
        let names: Vec<String> = std::fs::read_dir(&dir)
            .expect("read log dir")
            .filter_map(|e| Some(e.ok()?.file_name().to_string_lossy().into_owned()))
            .collect();
        // The prefix must stay distinct from the host's own rotated `logs.*`
        // set in the same directory, or the two rotators prune each other.
        assert!(
            names.iter().any(|n| n.starts_with("neutrino.")),
            "expected a neutrino.* log file, found {names:?}"
        );
    }

    #[test]
    fn file_layer_reports_an_unusable_directory_instead_of_failing_startup() {
        let tmp = tempfile::tempdir().expect("tempdir");
        // A file where the directory should be: `create_dir_all` cannot win.
        let blocker = tmp.path().join("logs");
        std::fs::write(&blocker, b"not a directory").expect("write blocker");

        let err = file_layer::<Registry>(&blocker)
            .err()
            .expect("a file in the way must not yield a working sink");
        // The caller logs this verbatim, so it has to name the path.
        assert!(
            err.contains(&blocker.display().to_string()),
            "error should name the offending path, got {err:?}"
        );
    }
}
