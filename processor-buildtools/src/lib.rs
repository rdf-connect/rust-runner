//! Build-time and CI tooling for a processor's `:artifact`/`:abiStable`
//! manifest data.
//!
//! Two entry points, for two structurally different moments:
//!
//! - [`generate`] — called from a processor crate's own `build.rs`. Runs
//!   *before* that crate's own `cdylib` exists, so it can only ever record a
//!   `:path` (no `:sha256` — nothing to hash yet). For each source file,
//!   finds every `a rdfc:Processor` class and writes the classes directly to
//!   the file its `<> owl:imports <…>` declaration names (defaulting to
//!   `generated/<source-basename>.ttl`). When multiple source files share the
//!   same import target (e.g. `processor.ttl` and `send_processor.ttl` both
//!   importing `generated.ttl`), the first source clears the target and writes
//!   its classes; each subsequent source merges its classes in. This keeps all
//!   processors in one file without any overwrite race.
//! - The `merge`/`snippet`/`flatten` CLI subcommands (see `main.rs`) —
//!   genuinely post-build steps (the artifact must already exist to hash
//!   it), used by CI to fold every matrix leg's snippet together and, for a
//!   multi-processor repo publishing one release, flatten per-crate files
//!   into the shared, collision-free namespace GitHub Release assets need.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use oxrdf::{NamedOrBlankNode, Term};
use oxttl::TurtleParser;

const NS: &str = "https://w3id.org/rdf-connect/rust#";
const RDF_TYPE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
const RDFC_PROCESSOR: &str = "https://w3id.org/rdf-connect#Processor";
const OWL_IMPORTS: &str = "http://www.w3.org/2002/07/owl#imports";

// ── Data model ──────────────────────────────────────────────────────────────

/// One target-triple entry in a processor's `:artifact` table.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Artifact {
    pub target_triple: String,
    pub path: String,
    pub sha256: Option<String>,
}

/// Everything generated/merged for one processor class.
#[derive(Debug, Clone, Default)]
pub struct ProcessorData {
    pub abi_token: Option<String>,
    pub artifacts: Vec<Artifact>,
}

// ── Turtle in ───────────────────────────────────────────────────────────────

/// Parse `ttl`, returning:
/// - every `a rdfc:Processor` subject found, with whatever `:abiStable` /
///   `:artifact` data already exists for it (empty for a hand-authored
///   source file; populated when re-parsing an already-generated snippet),
/// - every `owl:imports` target declared on `<>` (the document itself).
pub fn parse(ttl: &str) -> Result<(BTreeMap<String, ProcessorData>, Vec<String>)> {
    let triples = TurtleParser::new()
        .lenient()
        .for_reader(ttl.as_bytes())
        .collect::<Result<Vec<_>, _>>()
        .context("parsing Turtle")?;

    let mut classes: BTreeMap<String, ProcessorData> = BTreeMap::new();
    let mut imports = Vec::new();

    // Pass 1: find processor class subjects and `<> owl:imports <X>`.
    // A subject is a processor class if it either:
    //   - carries `a rdfc:Processor` (hand-authored source files), or
    //   - carries `:abiStable` (generated/merged files, where the type
    //     triple is intentionally omitted to avoid duplicating the
    //     declaration that already lives in the source .ttl).
    for t in &triples {
        let subj = match &t.subject {
            NamedOrBlankNode::NamedNode(n) => n.as_str(),
            NamedOrBlankNode::BlankNode(_) => continue,
        };
        if t.predicate.as_str() == RDF_TYPE {
            if let Term::NamedNode(obj) = &t.object {
                if obj.as_str() == RDFC_PROCESSOR {
                    classes.entry(subj.to_string()).or_default();
                }
            }
        }
        if t.predicate.as_str() == format!("{NS}abiStable")
            || t.predicate.as_str() == format!("{NS}artifact")
        {
            classes.entry(subj.to_string()).or_default();
        }
        if t.predicate.as_str() == OWL_IMPORTS && subj.is_empty() {
            if let Some(target) = term_as_str(&t.object) {
                imports.push(target.to_string());
            }
        }
    }

    // Pass 2: pull in any :abiStable / :artifact already present (the
    // "re-parsing an existing snippet" case — a fresh hand-authored
    // processor.ttl won't have any of this yet).
    let mut artifact_nodes: Vec<(String, oxrdf::BlankNode)> = Vec::new();
    for t in &triples {
        let NamedOrBlankNode::NamedNode(subj) = &t.subject else {
            continue;
        };
        let Some(data) = classes.get_mut(subj.as_str()) else {
            continue;
        };
        match t.predicate.as_str() {
            p if p == format!("{NS}abiStable") => {
                if let Some(v) = term_as_str(&t.object) {
                    data.abi_token = Some(v.to_string());
                }
            }
            p if p == format!("{NS}artifact") => {
                if let Term::BlankNode(bn) = &t.object {
                    artifact_nodes.push((subj.as_str().to_string(), bn.clone()));
                }
            }
            _ => {}
        }
    }
    for (class_uri, node) in artifact_nodes {
        let mut target_triple = None;
        let mut path = None;
        let mut sha256 = None;
        for t in &triples {
            let matches_node = matches!(&t.subject, NamedOrBlankNode::BlankNode(b) if *b == node);
            if !matches_node {
                continue;
            }
            let Some(value) = term_as_str(&t.object) else {
                continue;
            };
            match t.predicate.as_str() {
                p if p == format!("{NS}targetTriple") => target_triple = Some(value.to_string()),
                p if p == format!("{NS}path") => path = Some(value.to_string()),
                p if p == format!("{NS}sha256") => sha256 = Some(value.to_string()),
                _ => {}
            }
        }
        if let (Some(target_triple), Some(path)) = (target_triple, path) {
            if let Some(data) = classes.get_mut(&class_uri) {
                data.artifacts.push(Artifact {
                    target_triple,
                    path,
                    sha256,
                });
            }
        }
    }

    Ok((classes, imports))
}

fn term_as_str(term: &Term) -> Option<&str> {
    match term {
        Term::Literal(l) => Some(l.value()),
        Term::NamedNode(n) => Some(n.as_str()),
        _ => None,
    }
}

// ── Turtle out ──────────────────────────────────────────────────────────────

pub fn render(classes: &BTreeMap<String, ProcessorData>) -> String {
    let mut out = String::new();
    out.push_str("@prefix : <https://w3id.org/rdf-connect/rust#>.\n\n");
    for (class_uri, data) in classes {
        let mut properties: Vec<String> = Vec::new();

        if let Some(token) = &data.abi_token {
            properties.push(format!(":abiStable \"{}\"", escape(token)));
        }

        let mut artifacts = data.artifacts.clone();
        artifacts.sort();
        artifacts.dedup();
        if !artifacts.is_empty() {
            let blocks: Vec<String> = artifacts
                .iter()
                .map(|a| {
                    let sha = a
                        .sha256
                        .as_ref()
                        .map(|s| format!(" :sha256 \"{}\";", escape(s)))
                        .unwrap_or_default();
                    format!(
                        "[ :targetTriple \"{}\"; :path <{}>;{sha} ]",
                        escape(&a.target_triple),
                        a.path
                    )
                })
                .collect();
            properties.push(format!(":artifact\n        {}", blocks.join(",\n        ")));
        }

        out.push_str(&format!(
            "<{class_uri}>\n    {} .\n\n",
            properties.join(";\n    ")
        ));
    }
    out
}

fn escape(s: &str) -> String {
    s.replace('\\', "\\\\").replace('"', "\\\"")
}

// ── merge ───────────────────────────────────────────────────────────────────

/// Merge every processor class's data across `inputs` (snippet files, each
/// already in the shape [`render`] produces) and write the union to `output`.
pub fn merge(inputs: &[PathBuf], output: &Path) -> Result<()> {
    let mut merged: BTreeMap<String, ProcessorData> = BTreeMap::new();
    for input in inputs {
        if !input.is_file() {
            continue;
        }
        let ttl = std::fs::read_to_string(input)
            .with_context(|| format!("reading '{}'", input.display()))?;
        let (classes, _imports) = parse(&ttl)?;
        for (class_uri, data) in classes {
            let entry = merged.entry(class_uri).or_default();
            if entry.abi_token.is_none() {
                entry.abi_token = data.abi_token;
            }
            entry.artifacts.extend(data.artifacts);
        }
    }
    if let Some(parent) = output.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating '{}'", parent.display()))?;
    }
    std::fs::write(output, render(&merged))
        .with_context(|| format!("writing '{}'", output.display()))?;
    Ok(())
}

/// Write a single-class, single-artifact snippet. Used both by [`generate`]
/// (no checksum) and the CLI's post-build checksummed mode.
pub fn write_snippet(
    class_uri: &str,
    abi_token: &str,
    artifact: &Artifact,
    output: &Path,
) -> Result<()> {
    let mut classes = BTreeMap::new();
    classes.insert(
        class_uri.to_string(),
        ProcessorData {
            abi_token: Some(abi_token.to_string()),
            artifacts: vec![artifact.clone()],
        },
    );
    if let Some(parent) = output.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating '{}'", parent.display()))?;
    }
    std::fs::write(output, render(&classes))
        .with_context(|| format!("writing '{}'", output.display()))?;
    Ok(())
}

// ── generate() — called from build.rs ─────────────────────────────────────────

/// Locate this crate's own compiled `cdylib`, given the standard `build.rs`
/// environment. Works for both native and cross-compiled (`--target`)
/// builds: `OUT_DIR` is always `<target-dir>/[<target-triple>/]<profile>/
/// build/<pkg>-<hash>/out`, so its 3rd ancestor is always `<profile>/`,
/// regardless of whether the target-triple segment is present.
fn compiled_lib_path() -> Result<PathBuf> {
    let out_dir = PathBuf::from(
        std::env::var("OUT_DIR").context("OUT_DIR not set — call generate() from build.rs")?,
    );
    let profile_dir = out_dir
        .ancestors()
        .nth(3)
        .ok_or_else(|| {
            anyhow::anyhow!(
                "could not derive target/<profile> from OUT_DIR '{}'",
                out_dir.display()
            )
        })?
        .to_path_buf();

    let pkg_name = std::env::var("CARGO_PKG_NAME").context("CARGO_PKG_NAME not set")?;
    let lib_stem = pkg_name.replace('-', "_");
    let target = std::env::var("TARGET").context("TARGET not set")?;

    let filename = if target.contains("apple") {
        format!("lib{lib_stem}.dylib")
    } else if target.contains("windows") {
        format!("{lib_stem}.dll")
    } else {
        format!("lib{lib_stem}.so")
    };

    Ok(profile_dir.join(filename))
}

/// Called from `build.rs`. For each given source file (typically just
/// `["processor.ttl"]`), finds every `a rdfc:Processor` class it declares,
/// writes this crate's own per-target-triple snippet for each (no
/// `:sha256` — the artifact doesn't exist yet at `build.rs` time), and
/// writes the classes directly to the file each source's `owl:imports` points
/// at (defaulting to `generated/<source-basename>.ttl` if it doesn't declare
/// one).
///
/// When multiple source files share the same `owl:imports` target (e.g.
/// `processor.ttl` and `send_processor.ttl` both importing `generated.ttl`),
/// the first source encountered clears the target and writes its classes; each
/// subsequent source merges its classes into the already-written file. This
/// avoids the "last writer wins" overwrite that would occur if each source
/// independently regenerated the same file.
pub fn generate(ttl_files: &[&str]) -> Result<()> {
    use std::collections::BTreeSet;

    let manifest_dir =
        PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").context("CARGO_MANIFEST_DIR not set")?);
    let target_triple = std::env::var("TARGET").context("TARGET not set")?;
    let lib_path = compiled_lib_path()?;
    let abi_token = rdfc_processor_sdk::ABI_TOKEN;

    // Tracks which target files have been (re)created in this build step.
    // The first source to write a target clears it; later sources append.
    let mut initialized: BTreeSet<PathBuf> = BTreeSet::new();

    for ttl_file in ttl_files {
        println!("cargo:rerun-if-changed={ttl_file}");
        let source_path = manifest_dir.join(ttl_file);
        let ttl = std::fs::read_to_string(&source_path)
            .with_context(|| format!("reading '{}'", source_path.display()))?;
        let (classes, imports) = parse(&ttl)?;
        if classes.is_empty() {
            continue;
        }

        let source_dir = source_path.parent().unwrap_or(&manifest_dir);
        let target_file = match imports.first() {
            Some(target) => source_dir.join(target),
            None => {
                let basename = Path::new(ttl_file)
                    .file_name()
                    .and_then(|f| f.to_str())
                    .unwrap_or("processor.ttl");
                source_dir.join("generated").join(basename)
            }
        };

        let artifact = Artifact {
            target_triple: target_triple.clone(),
            path: format!("file://{}", lib_path.display()),
            sha256: None,
        };

        let mut new_classes: BTreeMap<String, ProcessorData> = classes
            .keys()
            .map(|class_uri| {
                (
                    class_uri.clone(),
                    ProcessorData {
                        abi_token: Some(abi_token.to_string()),
                        artifacts: vec![artifact.clone()],
                    },
                )
            })
            .collect();

        // First source targeting this file: clear it and write our classes.
        // Subsequent sources: read what's there, merge our classes in, rewrite.
        if initialized.contains(&target_file) {
            let existing_ttl = std::fs::read_to_string(&target_file)
                .with_context(|| format!("reading '{}'", target_file.display()))?;
            let (existing_classes, _) = parse(&existing_ttl)?;
            for (class_uri, existing_data) in existing_classes {
                let entry = new_classes.entry(class_uri).or_default();
                if entry.abi_token.is_none() {
                    entry.abi_token = existing_data.abi_token;
                }
                entry.artifacts.extend(existing_data.artifacts);
            }
        } else {
            initialized.insert(target_file.clone());
        }

        if let Some(parent) = target_file.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("creating '{}'", parent.display()))?;
        }
        std::fs::write(&target_file, render(&new_classes))
            .with_context(|| format!("writing '{}'", target_file.display()))?;
    }

    Ok(())
}

// Re-export parse/render for the CLI (main.rs) without making them part of
// the stable public API surface.
#[doc(hidden)]
pub mod internal {
    pub use super::{parse, render, ProcessorData};
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_source_processor_ttl() {
        let ttl = r#"
            @prefix rdfc: <https://w3id.org/rdf-connect#>.
            @prefix : <https://w3id.org/rdf-connect/rust#>.
            @prefix owl: <http://www.w3.org/2002/07/owl#>.

            <> owl:imports <generated.ttl>.

            rdfc:EchoProcessor a rdfc:Processor;
                :rustImplementationOf rdfc:Processor.
        "#;
        let (classes, imports) = parse(ttl).unwrap();
        assert_eq!(imports, vec!["generated.ttl".to_string()]);
        assert!(classes.contains_key("https://w3id.org/rdf-connect#EchoProcessor"));
        assert!(classes["https://w3id.org/rdf-connect#EchoProcessor"]
            .artifacts
            .is_empty());
    }

    #[test]
    fn render_and_reparse_roundtrips() {
        let mut classes = BTreeMap::new();
        classes.insert(
            "https://w3id.org/rdf-connect#EchoProcessor".to_string(),
            ProcessorData {
                abi_token: Some("token-123".to_string()),
                artifacts: vec![
                    Artifact {
                        target_triple: "x86_64-unknown-linux-gnu".to_string(),
                        path: "file:///tmp/foo.so".to_string(),
                        sha256: Some("abc".to_string()),
                    },
                    Artifact {
                        target_triple: "aarch64-apple-darwin".to_string(),
                        path: "file:///tmp/foo.dylib".to_string(),
                        sha256: None,
                    },
                ],
            },
        );
        let text = render(&classes);
        let (reparsed, _) = parse(&text).unwrap();
        let data = &reparsed["https://w3id.org/rdf-connect#EchoProcessor"];
        assert_eq!(data.abi_token.as_deref(), Some("token-123"));
        assert_eq!(data.artifacts.len(), 2);
    }

    #[test]
    fn merge_unions_artifacts_across_files() {
        let dir = std::env::temp_dir().join(format!("rdfc-buildtools-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();

        let mut a = BTreeMap::new();
        a.insert(
            "https://w3id.org/rdf-connect#EchoProcessor".to_string(),
            ProcessorData {
                abi_token: Some("token".to_string()),
                artifacts: vec![Artifact {
                    target_triple: "x86_64-unknown-linux-gnu".to_string(),
                    path: "file:///a.so".to_string(),
                    sha256: None,
                }],
            },
        );
        let mut b = BTreeMap::new();
        b.insert(
            "https://w3id.org/rdf-connect#EchoProcessor".to_string(),
            ProcessorData {
                abi_token: None,
                artifacts: vec![Artifact {
                    target_triple: "aarch64-apple-darwin".to_string(),
                    path: "file:///b.dylib".to_string(),
                    sha256: None,
                }],
            },
        );
        let f1 = dir.join("a.ttl");
        let f2 = dir.join("b.ttl");
        std::fs::write(&f1, render(&a)).unwrap();
        std::fs::write(&f2, render(&b)).unwrap();

        let out = dir.join("out.ttl");
        merge(&[f1, f2], &out).unwrap();

        let merged_text = std::fs::read_to_string(&out).unwrap();
        let (merged, _) = parse(&merged_text).unwrap();
        let data = &merged["https://w3id.org/rdf-connect#EchoProcessor"];
        assert_eq!(data.abi_token.as_deref(), Some("token"));
        assert_eq!(data.artifacts.len(), 2);

        std::fs::remove_dir_all(&dir).ok();
    }
}
