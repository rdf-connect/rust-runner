# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## v0.3.0 (2026-07-15)

### New Features

 - <csr-id-2edb6648794a8255a93994c00c2cf779ce477b75/> move to trait based processors
 - <csr-id-d84662f95815ebd9e7ff5b64ec73f3963b75706f/> add processor-buildtools, multi-processor support, and CI workflow
   - Add processor-buildtools crate with build-time TTL generation and CI
     publishing tooling (generate, merge, snippet, flatten, class-uris CLI)
   - Fix generate() to write all processors from multiple source TTLs directly
     into the shared owl:imports target file, using first-clear-then-merge
     semantics so two source TTLs pointing at the same generated.ttl no longer
     overwrite each other
   - Remove 'a rdfc:Processor' from rendered output; parse() now discovers
     classes via :abiStable / :artifact predicates instead
   - Add build.rs to echo, log, send processors
   - Add processors/multi: a three-processor crate (Echo + Send + Log) that
     exercises the multi-processor-per-crate build path
   - Strip SendProcessor back out of echo (it now exports only EchoProcessor)
   - Add .github/workflows/build-processors.yml: matrix build across four
     target triples, per-class snippet accumulation, and GitHub Release publish;
     handles multi-processor crates by reading class-uris from generated.ttl
     and flattening each source TTL separately
 - <csr-id-81c412e8aa30f3af642a6300aeacc84261c3edca/> load processors as abi_stable cdylibs via owl:imports
   Revert processor loading from static linking back to dynamic `cdylib`
   loading, this time on `abi_stable` + `async_ffi` instead of raw FFI, plus
   a way to distribute processors outside this repo.
   
   - processor-sdk: Reader/Writer are `sabi_trait` objects over `async_ffi`
     futures; `rdfc_register!` exports one `#[export_root_module]` per
     cdylib, so registration survives symbol stripping without needing
     linkme/inventory link-time collection.
   - runner: `build.rs` bakes in the exact compiled target triple. There is
     no `rdfc:file` / `builtin://` — a processor's own `processor.ttl`
     declares an `:artifact` table (target triple -> path + optional
     sha256) and an `:abiStable` token; a shared SHACL shape (index.ttl)
     makes the orchestrator extract that straight into `proc.config`, so
     the runner never parses Turtle itself. Local paths, `file://` URIs,
     and http(s) downloads (checksum-verified and cached) are all
     supported. Using a processor is just `owl:import`ing its
     processor.ttl.
   - processors/{echo,log,send}: converted back to cdylib crates with real
     local processor.ttl artifact tables.
   - kolibrie-processor: out-of-tree example with a multi-target-triple
     processor.ttl and a GitHub Actions workflow that builds, checksums,
     and publishes a release.
   - examples/echo-pipeline: runnable pipeline wiring the above together
     purely via owl:imports; verified end-to-end against the real
     orchestrator over both local and http(s) artifact resolution.
 - <csr-id-50b9902278c1c4067c6530d47578f630771d9610/> better processor deserialize args
 - <csr-id-c92371aed1056388b8f495437867094b05fd7700/> initial implementation

### Bug Fixes

 - <csr-id-2b4cefa821f5600667a13d28b5c4e6bb9050faa5/> claude fixed some things

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 6 commits contributed to the release over the course of 29 calendar days.
 - 6 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - Move to trait based processors ([`2edb664`](https://github.com/rdf-connect/rust-runner/commit/2edb6648794a8255a93994c00c2cf779ce477b75))
    - Add processor-buildtools, multi-processor support, and CI workflow ([`d84662f`](https://github.com/rdf-connect/rust-runner/commit/d84662f95815ebd9e7ff5b64ec73f3963b75706f))
    - Load processors as abi_stable cdylibs via owl:imports ([`81c412e`](https://github.com/rdf-connect/rust-runner/commit/81c412e8aa30f3af642a6300aeacc84261c3edca))
    - Claude fixed some things ([`2b4cefa`](https://github.com/rdf-connect/rust-runner/commit/2b4cefa821f5600667a13d28b5c4e6bb9050faa5))
    - Better processor deserialize args ([`50b9902`](https://github.com/rdf-connect/rust-runner/commit/50b9902278c1c4067c6530d47578f630771d9610))
    - Initial implementation ([`c92371a`](https://github.com/rdf-connect/rust-runner/commit/c92371aed1056388b8f495437867094b05fd7700))
</details>

