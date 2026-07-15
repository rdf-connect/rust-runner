# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## v0.3.0 (2026-07-15)

### New Features

 - <csr-id-2edb6648794a8255a93994c00c2cf779ce477b75/> move to trait based processors
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
 - <csr-id-c92371aed1056388b8f495437867094b05fd7700/> initial implementation

### Bug Fixes

 - <csr-id-2b4cefa821f5600667a13d28b5c4e6bb9050faa5/> claude fixed some things

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 5 commits contributed to the release over the course of 29 calendar days.
 - 4 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - Release rdfc-processor-sdk v0.3.0, rdfc-processor-buildtools v0.3.0, rdfc-rust-runner v0.3.0 ([`4df506d`](https://github.com/rdf-connect/rust-runner/commit/4df506d78e805e6502694e1de6acee1923a8d573))
    - Move to trait based processors ([`2edb664`](https://github.com/rdf-connect/rust-runner/commit/2edb6648794a8255a93994c00c2cf779ce477b75))
    - Load processors as abi_stable cdylibs via owl:imports ([`81c412e`](https://github.com/rdf-connect/rust-runner/commit/81c412e8aa30f3af642a6300aeacc84261c3edca))
    - Claude fixed some things ([`2b4cefa`](https://github.com/rdf-connect/rust-runner/commit/2b4cefa821f5600667a13d28b5c4e6bb9050faa5))
    - Initial implementation ([`c92371a`](https://github.com/rdf-connect/rust-runner/commit/c92371aed1056388b8f495437867094b05fd7700))
</details>

