//! Processor registry — resolves a processor's declared `:artifact` table to
//! a loadable `cdylib`.
//!
//! There is no `rdfc:file` / `builtin://` special-casing: a pipeline that
//! wants to use a processor simply `owl:import`s that processor's own
//! `processor.ttl` (e.g. `processors/echo/processor.ttl` or
//! `kolibrie-processor/processor.ttl`), which brings in both its argument
//! shape and its `:abiStable`/`:artifact` declarations. The orchestrator's
//! generic SHACL shape (targeting subjects of `:rustImplementationOf` — see
//! `runner/index.ttl`) extracts those directly into `proc.config` as JSON;
//! the runner never independently fetches or parses any Turtle itself.

use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use tracing::info;

use crate::processor::{Processor, ProcessorContext};

/// This runner binary's own compiled target triple (e.g.
/// `x86_64-unknown-linux-gnu`), baked in by `build.rs` from cargo's `TARGET`
/// env var — not reconstructed from `std::env::consts::{OS, ARCH}`, which
/// can't distinguish `gnu` vs `musl` libc or `msvc` vs `gnu` on Windows.
pub const TARGET_TRIPLE: &str = env!("RDFC_TARGET_TRIPLE");

// ── Config extracted from proc.config ─────────────────────────────────────────

/// One `(target triple) -> (path, optional checksum)` entry, as extracted by
/// the orchestrator from a processor's `:artifact` declarations.
#[derive(Debug, Deserialize)]
pub struct Artifact {
    pub target_triple: String,
    /// Already resolved by the orchestrator — relative IRIs in `:artifact`
    /// blocks resolve against wherever the declaring file was fetched from,
    /// so this is always a usable local filesystem path or `http(s)://` URL,
    /// never a bare relative reference.
    pub path: String,
    /// Omitted entirely for local development builds — when absent, the
    /// artifact is loaded without a checksum check.
    #[serde(default)]
    pub sha256: Option<String>,
}

/// Runner-specific configuration for one processor definition, extracted
/// from `proc.config` (JSON-LD → plain JSON) by the orchestrator's SHACL
/// shape targeting subjects of `:rustImplementationOf`.
#[derive(Debug, Deserialize)]
pub struct ProcessorConfig {
    /// The `:abiStable` token declared on the processor's class, if any.
    /// Compared against `rdfc_processor_sdk::ABI_TOKEN` before attempting to
    /// download or load anything, so an ABI mismatch is a clear error
    /// instead of a failure deep inside `abi_stable`.
    #[serde(default)]
    pub abi_token: Option<String>,
    #[serde(default)]
    pub artifacts: Vec<Artifact>,
}

// ── Registry ──────────────────────────────────────────────────────────────────

pub struct ProcessorRegistry;

impl ProcessorRegistry {
    pub fn new() -> Self {
        ProcessorRegistry
    }

    /// Create a processor instance from the runner config, the processor's
    /// class URI (from `@type` in the JSON-LD arguments), and a prepared context.
    pub async fn create(
        &self,
        config: &ProcessorConfig,
        type_uri: &str,
        ctx: ProcessorContext,
    ) -> Result<Box<dyn Processor>> {
        let artifact = config
            .artifacts
            .iter()
            .find(|a| a.target_triple == TARGET_TRIPLE)
            .ok_or_else(|| {
                let available: Vec<&str> = config
                    .artifacts
                    .iter()
                    .map(|a| a.target_triple.as_str())
                    .collect();
                anyhow::anyhow!(
                    "processor '{type_uri}' has no prebuilt artifact for this runner's target \
                     '{TARGET_TRIPLE}' (it offers: {})",
                    if available.is_empty() {
                        "none".to_string()
                    } else {
                        available.join(", ")
                    }
                )
            })?;

        check_abi_token(config.abi_token.as_deref(), type_uri)?;

        let so_path = resolve_artifact(artifact)
            .await
            .with_context(|| format!("resolving artifact for processor '{type_uri}'"))?;
        load_dynamic(&so_path, type_uri, ctx)
    }
}

impl Default for ProcessorRegistry {
    fn default() -> Self {
        Self::new()
    }
}

fn check_abi_token(abi_token: Option<&str>, type_uri: &str) -> Result<()> {
    if let Some(token) = abi_token {
        if token != rdfc_processor_sdk::ABI_TOKEN {
            bail!(
                "processor '{type_uri}' was built against abi token '{token}', but this runner \
                 expects '{}' — rebuild the processor against a matching rdfc-processor-sdk version",
                rdfc_processor_sdk::ABI_TOKEN
            );
        }
    }
    Ok(())
}

// ── Artifact resolution (local path or http(s) download + cache) ─────────────

async fn resolve_artifact(artifact: &Artifact) -> Result<PathBuf> {
    if artifact.path.starts_with("http://") || artifact.path.starts_with("https://") {
        return resolve_remote(artifact).await;
    }

    // The orchestrator hands relative `:path` IRIs back already resolved to
    // absolute IRIs — for a local file that means a `file://` URI, not a
    // bare filesystem path. Accept both: `file://` URIs (the real-world
    // case, coming from the orchestrator's SHACL extraction) and plain paths
    // (e.g. constructed directly in tests, or written by hand).
    let path = if let Some(rest) = artifact.path.strip_prefix("file://") {
        reqwest::Url::parse(&artifact.path)
            .ok()
            .and_then(|url| url.to_file_path().ok())
            .unwrap_or_else(|| PathBuf::from(rest))
    } else {
        PathBuf::from(&artifact.path)
    };

    resolve_local(artifact, path).await
}

async fn resolve_local(artifact: &Artifact, path: PathBuf) -> Result<PathBuf> {
    if !path.is_file() {
        bail!("processor artifact '{}' not found", path.display());
    }
    if let Some(expected) = &artifact.sha256 {
        verify_checksum(&path, expected)
            .await
            .with_context(|| format!("verifying '{}'", path.display()))?;
    }
    Ok(path)
}

async fn resolve_remote(artifact: &Artifact) -> Result<PathBuf> {
    let url = reqwest::Url::parse(&artifact.path)
        .with_context(|| format!("invalid artifact URL '{}'", artifact.path))?;

    let cache_path = cache_path_for(artifact);
    if cache_path.is_file() {
        let usable = match &artifact.sha256 {
            Some(expected) => checksum_matches(&cache_path, expected).await,
            None => true,
        };
        if usable {
            info!("using cached processor artifact: {}", cache_path.display());
            return Ok(cache_path);
        }
    }

    info!("downloading processor artifact from '{url}'");
    download(&url, &cache_path).await?;
    if let Some(expected) = &artifact.sha256 {
        verify_checksum(&cache_path, expected)
            .await
            .with_context(|| format!("verifying downloaded artifact '{}'", cache_path.display()))?;
    }
    Ok(cache_path)
}

// ── Checksums + cache ──────────────────────────────────────────────────────────

fn cache_dir() -> PathBuf {
    dirs::cache_dir()
        .unwrap_or_else(std::env::temp_dir)
        .join("rdfc-runner")
        .join("processors")
}

/// Cache key: prefer the checksum (content-addressed, safe to keep forever);
/// fall back to hashing the URL itself when no `:sha256` was given (always
/// re-verified against nothing, so this only dedupes repeated downloads
/// within a run/host, it doesn't guarantee freshness).
fn cache_path_for(artifact: &Artifact) -> PathBuf {
    let filename = Path::new(&artifact.path)
        .file_name()
        .and_then(|f| f.to_str())
        .unwrap_or("processor");
    let key_source = artifact.sha256.as_deref().unwrap_or(&artifact.path);
    let key = to_hex(&Sha256::digest(key_source.as_bytes()));
    cache_dir().join(format!("{key}-{filename}"))
}

async fn sha256_file(path: &Path) -> Result<String> {
    let path = path.to_path_buf();
    tokio::task::spawn_blocking(move || -> Result<String> {
        use std::io::Read;

        let mut file =
            std::fs::File::open(&path).with_context(|| format!("opening '{}'", path.display()))?;
        let mut hasher = Sha256::new();
        let mut buf = [0u8; 64 * 1024];
        loop {
            let n = file
                .read(&mut buf)
                .with_context(|| format!("reading '{}'", path.display()))?;
            if n == 0 {
                break;
            }
            hasher.update(&buf[..n]);
        }
        Ok(to_hex(&hasher.finalize()))
    })
    .await
    .context("checksum thread panicked")?
}

async fn verify_checksum(path: &Path, expected_sha256: &str) -> Result<()> {
    let actual = sha256_file(path).await?;
    if !actual.eq_ignore_ascii_case(expected_sha256) {
        bail!(
            "checksum mismatch for '{}': manifest declares {expected_sha256}, computed {actual}",
            path.display()
        );
    }
    Ok(())
}

async fn checksum_matches(path: &Path, expected_sha256: &str) -> bool {
    sha256_file(path)
        .await
        .map(|actual| actual.eq_ignore_ascii_case(expected_sha256))
        .unwrap_or(false)
}

fn to_hex(bytes: &[u8]) -> String {
    use std::fmt::Write;
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = write!(s, "{b:02x}");
    }
    s
}

async fn download(url: &reqwest::Url, dest: &Path) -> Result<()> {
    if let Some(parent) = dest.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .with_context(|| format!("creating cache directory '{}'", parent.display()))?;
    }
    let bytes = reqwest::get(url.clone())
        .await
        .and_then(|r| r.error_for_status())
        .with_context(|| format!("downloading '{url}'"))?
        .bytes()
        .await
        .with_context(|| format!("reading download body from '{url}'"))?;

    // Write to a temp file first so a crash mid-download can't leave a
    // corrupt file at `dest` that a later checksum check would wrongly trust.
    let tmp = dest.with_extension("part");
    tokio::fs::write(&tmp, &bytes)
        .await
        .with_context(|| format!("writing '{}'", tmp.display()))?;
    tokio::fs::rename(&tmp, dest)
        .await
        .with_context(|| format!("finalizing '{}'", dest.display()))?;
    Ok(())
}

// ── Loading + DynamicProcessor ────────────────────────────────────────────────

/// Load `so_path` via `abi_stable`, find the registration for `type_uri`, and
/// wrap it as a [`Processor`].
///
/// This deliberately does *not* use `ProcessorModuleRef::load_from_file` —
/// that API caches the loaded root module in a `static` keyed by the Rust
/// *type* `ProcessorModuleRef`, on the assumption that one Rust type
/// corresponds to exactly one dynamic library. Here every processor cdylib
/// shares that same type, so the first `load_from_file` call would poison
/// all subsequent ones with its own (wrong) module. Instead, each `.so` is
/// loaded and its root module extracted independently via the lower-level
/// `RawLibrary` / `LibHeader` API, which does the same version + type-layout
/// checks without any cross-library caching.
fn load_dynamic(so_path: &Path, type_uri: &str, ctx: ProcessorContext) -> Result<Box<dyn Processor>> {
    use rdfc_processor_sdk::abi_stable::library::{lib_header_from_raw_library, RawLibrary};

    let raw_lib = RawLibrary::load_at(so_path).map_err(|e| {
        anyhow::anyhow!("failed to load processor library '{}': {e}", so_path.display())
    })?;
    // Leaked deliberately: the returned root module borrows `'static` from
    // the library, and we never unload processor libraries once loaded.
    let raw_lib: &'static RawLibrary = Box::leak(Box::new(raw_lib));

    let lib_header = unsafe { lib_header_from_raw_library(raw_lib) }.map_err(|e| {
        anyhow::anyhow!(
            "failed to read processor library header for '{}': {e}\n\n\
             This usually means the library was built against a different \
             `abi_stable` version than this runner expects.",
            so_path.display()
        )
    })?;

    let root: rdfc_processor_sdk::ProcessorModuleRef = lib_header.init_root_module().map_err(|e| {
        anyhow::anyhow!(
            "failed to load processor module from '{}': {e}\n\n\
             This usually means the library was built against a different \
             `abi_stable`/Rust compiler version than this runner, or the \
             file is corrupt. Rebuild the processor against the pinned \
             `rdfc-processor-sdk` version and try again.",
            so_path.display()
        )
    })?;

    let registrations = (root.registrations())();
    let registration = registrations
        .iter()
        .find(|r| r.type_uri.as_str() == type_uri)
        .ok_or_else(|| {
            let available: Vec<&str> = registrations.iter().map(|r| r.type_uri.as_str()).collect();
            anyhow::anyhow!(
                "'{}' does not export a processor for '{type_uri}' (it exports: {})",
                so_path.display(),
                if available.is_empty() {
                    "none".to_string()
                } else {
                    available.join(", ")
                }
            )
        })?;

    Ok(Box::new(DynamicProcessor::new(registration.factory, ctx)))
}

/// Wraps a loaded processor's `factory` fn as an async [`Processor`].
struct DynamicProcessor {
    factory: extern "C" fn(
        rdfc_processor_sdk::RawProcessorArgs,
    ) -> rdfc_processor_sdk::async_ffi::FfiFuture<
        rdfc_processor_sdk::abi_stable::std_types::RResult<
            (),
            rdfc_processor_sdk::abi_stable::std_types::RString,
        >,
    >,
    ctx: Option<ProcessorContext>, // taken in produce()
}

impl DynamicProcessor {
    fn new(
        factory: extern "C" fn(
            rdfc_processor_sdk::RawProcessorArgs,
        ) -> rdfc_processor_sdk::async_ffi::FfiFuture<
            rdfc_processor_sdk::abi_stable::std_types::RResult<
                (),
                rdfc_processor_sdk::abi_stable::std_types::RString,
            >,
        >,
        ctx: ProcessorContext,
    ) -> Self {
        DynamicProcessor {
            factory,
            ctx: Some(ctx),
        }
    }
}

#[async_trait::async_trait]
impl Processor for DynamicProcessor {
    async fn init(&mut self) -> Result<()> {
        Ok(())
    }

    async fn transform(&mut self) -> Result<()> {
        Ok(())
    }

    /// Run the processor by calling its exported factory and awaiting the
    /// returned `FfiFuture` directly — no dedicated OS thread needed, since
    /// polling an `FfiFuture` just calls back into the (already-loaded)
    /// processor's own compiled code.
    async fn produce(&mut self) -> Result<()> {
        let ctx = self
            .ctx
            .take()
            .ok_or_else(|| anyhow::anyhow!("DynamicProcessor.produce() called twice"))?;

        let raw_args = ctx.into_raw_args();
        match (self.factory)(raw_args).await {
            rdfc_processor_sdk::abi_stable::std_types::RResult::ROk(()) => Ok(()),
            rdfc_processor_sdk::abi_stable::std_types::RResult::RErr(e) => {
                bail!("processor returned an error: {e}")
            }
        }
    }
}
