//! CLI for the genuinely post-build steps that [`rdfc_processor_buildtools`]'s
//! `generate()` (called from `build.rs`) can't do itself — computing a real
//! checksum needs the artifact to already exist, which it doesn't until
//! after the crate has finished linking. Intended for CI, not local dev.
//!
//! Subcommands:
//! - `abi-token` — print `rdfc_processor_sdk::ABI_TOKEN`, so CI doesn't need
//!   to hardcode (and risk drifting from) that string.
//! - `checksum <file>` — print a file's SHA-256 (portable across the
//!   `sha256sum` (Linux) / `shasum -a 256` (macOS) split).
//! - `class-uris <ttl-file>` — print every `a rdfc:Processor` subject in a
//!   file, one per line (so CI doesn't need to hardcode a processor's class
//!   URI(s) — it can read them out of `processor.ttl` itself).
//! - `snippet --class <uri> --abi-token <token> --target-triple <triple>
//!   --path <path> [--sha256 <hash>] --out <file>` — write (or merge into) a
//!   single-artifact manifest snippet. If `--out` already exists, the new
//!   class/artifact is merged in rather than overwriting.
//! - `merge <output> <input>...` — union snippets into one file (same
//!   operation `generate()` does locally; exposed here for the CI publish
//!   job, which merges snippets downloaded from every matrix leg).
//! - `flatten --crate-name <name> --processor-ttl <path> --generated-ttl
//!   <path> --out-dir <dir> [--out-name <stem>]` — for a repo publishing
//!   several processor crates under one GitHub Release (a flat asset
//!   namespace): copy `processor.ttl` to `<out-name>.ttl` (defaulting to
//!   `<crate-name>.ttl`, rewriting its `owl:imports` target to
//!   `<crate-name>-generated.ttl`), and copy `generated.ttl` to
//!   `<crate-name>-generated.ttl` (rewriting every `:path` to be prefixed
//!   with `<crate-name>-`, matching how the built libraries themselves must be
//!   renamed before upload). For multi-processor crates where each processor
//!   lives in its own source TTL, run `flatten` once per source TTL, passing
//!   `--out-name <crate-name>-<source-stem>` to give each a unique filename.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use rdfc_processor_buildtools::internal::{parse, render, ProcessorData};
use rdfc_processor_buildtools::{merge, Artifact};
use sha2::{Digest, Sha256};

fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let Some((cmd, rest)) = args.split_first() else {
        bail!("usage: rdfc-processor-buildtools <abi-token|checksum|class-uris|snippet|merge|flatten> ...");
    };

    match cmd.as_str() {
        "abi-token" => {
            println!("{}", rdfc_processor_sdk::ABI_TOKEN);
            Ok(())
        }
        "checksum" => cmd_checksum(rest),
        "class-uris" => cmd_class_uris(rest),
        "snippet" => cmd_snippet(rest),
        "merge" => cmd_merge(rest),
        "flatten" => cmd_flatten(rest),
        other => bail!("unknown subcommand '{other}'"),
    }
}

fn cmd_checksum(args: &[String]) -> Result<()> {
    use std::io::Read;

    let [file] = args else {
        bail!("usage: checksum <file>");
    };
    let mut f = std::fs::File::open(file).with_context(|| format!("opening '{file}'"))?;
    let mut hasher = Sha256::new();
    let mut buf = [0u8; 64 * 1024];
    loop {
        let n = f.read(&mut buf).with_context(|| format!("reading '{file}'"))?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    println!("{}", to_hex(&hasher.finalize()));
    Ok(())
}

fn cmd_class_uris(args: &[String]) -> Result<()> {
    let [ttl_file] = args else {
        bail!("usage: class-uris <ttl-file>");
    };
    let ttl = std::fs::read_to_string(ttl_file).with_context(|| format!("reading '{ttl_file}'"))?;
    let (classes, _imports) = parse(&ttl)?;
    for class_uri in classes.keys() {
        println!("{class_uri}");
    }
    Ok(())
}

fn cmd_snippet(args: &[String]) -> Result<()> {
    let flags = parse_flags(args);
    let class = flags.require("class")?;
    let abi_token = flags.require("abi-token")?;
    let target_triple = flags.require("target-triple")?;
    let path = flags.require("path")?;
    let sha256 = flags.get("sha256").map(|s| s.to_string());
    let out = flags.require("out")?;
    let out_path = Path::new(out);

    let artifact = Artifact {
        target_triple: target_triple.to_string(),
        path: path.to_string(),
        sha256,
    };

    // If the output already exists (e.g. a previous class in the same loop
    // iteration already wrote to it), merge the new class in rather than
    // overwriting.
    if out_path.is_file() {
        let existing = std::fs::read_to_string(out_path)
            .with_context(|| format!("reading existing snippet '{out}'"))?;
        let (mut classes, _) = rdfc_processor_buildtools::internal::parse(&existing)?;
        let entry = classes
            .entry(class.to_string())
            .or_insert_with(rdfc_processor_buildtools::internal::ProcessorData::default);
        entry.abi_token = Some(abi_token.to_string());
        entry.artifacts.push(artifact);
        if let Some(parent) = out_path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("creating '{}'", parent.display()))?;
        }
        std::fs::write(out_path, rdfc_processor_buildtools::internal::render(&classes))
            .with_context(|| format!("writing '{out}'"))?;
        Ok(())
    } else {
        rdfc_processor_buildtools::write_snippet(class, abi_token, &artifact, out_path)
    }
}

fn cmd_merge(args: &[String]) -> Result<()> {
    let [output, inputs @ ..] = args else {
        bail!("usage: merge <output> <input>...");
    };
    if inputs.is_empty() {
        bail!("merge needs at least one input file");
    }
    let inputs: Vec<PathBuf> = inputs.iter().map(PathBuf::from).collect();
    merge(&inputs, Path::new(output))
}

fn cmd_flatten(args: &[String]) -> Result<()> {
    let flags = parse_flags(args);
    let crate_name = flags.require("crate-name")?;
    let processor_ttl = flags.require("processor-ttl")?;
    let generated_ttl = flags.require("generated-ttl")?;
    let out_dir = flags.require("out-dir")?;
    // Optional: override the output filename stem (without .ttl).
    // Defaults to <crate-name> when omitted (single-processor-per-TTL convention).
    let out_name = flags.get("out-name").unwrap_or(crate_name);

    std::fs::create_dir_all(out_dir).with_context(|| format!("creating '{out_dir}'"))?;

    // ── <name>.ttl: copy of processor.ttl, owl:imports target rewritten ──────
    let processor_text = std::fs::read_to_string(processor_ttl)
        .with_context(|| format!("reading '{processor_ttl}'"))?;
    let (_classes, imports) = parse(&processor_text)?;
    let Some(old_target) = imports.first() else {
        bail!("'{processor_ttl}' has no owl:imports statement to rewrite");
    };
    let new_target = format!("{crate_name}-generated.ttl");
    let rewritten_processor = processor_text.replace(&format!("<{old_target}>"), &format!("<{new_target}>"));
    let processor_out = Path::new(out_dir).join(format!("{out_name}.ttl"));
    std::fs::write(&processor_out, rewritten_processor)
        .with_context(|| format!("writing '{}'", processor_out.display()))?;

    // ── <name>-generated.ttl: copy of generated.ttl, :path values prefixed ───
    let generated_text = std::fs::read_to_string(generated_ttl)
        .with_context(|| format!("reading '{generated_ttl}'"))?;
    let (classes, _imports) = parse(&generated_text)?;
    let mut flattened: BTreeMap<String, ProcessorData> = BTreeMap::new();
    for (class_uri, data) in classes {
        let artifacts = data
            .artifacts
            .into_iter()
            .map(|a| {
                let basename = Path::new(&a.path)
                    .file_name()
                    .and_then(|f| f.to_str())
                    .unwrap_or(&a.path);
                Artifact {
                    target_triple: a.target_triple,
                    path: format!("{crate_name}-{basename}"),
                    sha256: a.sha256,
                }
            })
            .collect();
        flattened.insert(
            class_uri,
            ProcessorData {
                abi_token: data.abi_token,
                artifacts,
            },
        );
    }
    let generated_out = Path::new(out_dir).join(&new_target);
    std::fs::write(&generated_out, render(&flattened))
        .with_context(|| format!("writing '{}'", generated_out.display()))?;

    Ok(())
}

// ── helpers ──────────────────────────────────────────────────────────────────

fn to_hex(bytes: &[u8]) -> String {
    use std::fmt::Write;
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = write!(s, "{b:02x}");
    }
    s
}

struct Flags(BTreeMap<String, String>);

impl Flags {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).map(|s| s.as_str())
    }
    fn require(&self, key: &str) -> Result<&str> {
        self.get(key)
            .ok_or_else(|| anyhow::anyhow!("missing required --{key}"))
    }
}

fn parse_flags(args: &[String]) -> Flags {
    let mut map = BTreeMap::new();
    let mut it = args.iter();
    while let Some(arg) = it.next() {
        if let Some(key) = arg.strip_prefix("--") {
            if let Some(value) = it.next() {
                map.insert(key.to_string(), value.clone());
            }
        }
    }
    Flags(map)
}
