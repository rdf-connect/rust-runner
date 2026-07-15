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

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 2 commits contributed to the release over the course of 5 calendar days.
 - 2 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 0 issues like '(#ID)' were seen in commit messages

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **Uncategorized**
    - Move to trait based processors ([`2edb664`](https://github.com/rdf-connect/rust-runner/commit/2edb6648794a8255a93994c00c2cf779ce477b75))
    - Add processor-buildtools, multi-processor support, and CI workflow ([`d84662f`](https://github.com/rdf-connect/rust-runner/commit/d84662f95815ebd9e7ff5b64ec73f3963b75706f))
</details>

