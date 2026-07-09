//! Kolibrie reasoning processor for RDF-Connect.
//!
//! Reads N-Triples–encoded RDF messages from `reader`, applies Datalog rules
//! (supplied inline or from a file) using the Kolibrie engine, and writes
//! newly derived facts as N-Triples to `writer`.
//!
//! ## Parameters
//!
//! | field          | type     | required | description                                             |
//! |----------------|----------|----------|---------------------------------------------------------|
//! | `reader`       | Reader   | yes      | Channel carrying N-Triples RDF messages                 |
//! | `writer`       | Writer   | yes      | Channel to which derived N-Triples are written          |
//! | `rules`        | string   | *        | Inline N3 rules (required unless `rules_file` is given) |
//! | `rules_file`   | string   | *        | Path to a file containing N3 rules                      |
//! | `window_size`  | u64      | no       | Sliding-window width in seconds (0 = no windowing)      |
//! | `window_slide` | u64      | no       | Window slide interval in seconds (0 = every message)    |
//!
//! ## N3 rule format
//!
//! ```text
//! @prefix ex: <http://example.org/> .
//!
//! { ?x ex:parent ?y } => { ?x ex:ancestor ?y }
//! { ?x ex:parent ?y . ?y ex:ancestor ?z } => { ?x ex:ancestor ?z }
//! ```
//!
//! Rules are loaded once at startup. Each IRI in a rule must either be
//! written in full (`<http://…>`) or use a declared `@prefix`.
//!
//! ## Windowing
//!
//! When `window_size > 0` the processor keeps only the triples received in the
//! last `window_size` seconds. After each batch (or after `window_slide`
//! seconds if that parameter is set) inference is re-run on the current window
//! contents.  Without windowing, triples accumulate forever and inference is
//! incremental (only newly derivable facts are emitted).

use std::collections::{HashSet, VecDeque};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use rdfc_processor_sdk::{futures::StreamExt, ProcessorArgs, Reader, Writer};

use datalog::parser_n3_logic::parse_n3_document;
use datalog::reasoning::Reasoner;
use shared::dataset_index::DatasetIndex;
use shared::dictionary::Dictionary;
use shared::rule::Rule;
use shared::rule_index::RuleIndex;
use shared::triple::Triple;

// ── Config ────────────────────────────────────────────────────────────────────

#[derive(serde::Deserialize)]
struct Config {
    reader: Reader,
    writer: Writer,
    /// Inline N3 rules text.
    #[serde(default)]
    rules: Option<String>,
    /// Path to a file containing N3 rules.
    #[serde(default)]
    rules_file: Option<String>,
    /// Sliding-window width in seconds.  0 = no windowing (accumulate forever).
    #[serde(default)]
    window_size: u64,
    /// Slide interval in seconds.  0 = re-evaluate after every incoming message.
    #[serde(default)]
    window_slide: u64,
}

// ── Entry point ───────────────────────────────────────────────────────────────

async fn kolibrie(mut args: ProcessorArgs) -> Result<(), String> {
    let Config {
        reader,
        writer,
        rules,
        rules_file,
        window_size,
        window_slide,
    } = args.deserialize()?;
    // ── 1. Load and parse rules ───────────────────────────────────────────────
    let rules_text = load_rules(rules, rules_file)?;

    // Use a temporary Reasoner solely to populate the shared dictionary while
    // parsing rule IRIs / prefixes.
    let mut seed_reasoner = Reasoner::new();
    let parsed_rules = parse_rules(&rules_text, &mut seed_reasoner)?;

    // Share the dictionary between all future per-step Reasoners so that the
    // u32 IDs encoded into the rules remain valid.
    let shared_dict: Arc<RwLock<Dictionary>> = Arc::clone(&seed_reasoner.dictionary);

    eprintln!(
        "[kolibrie] loaded {} rule(s)",
        parsed_rules.len()
    );

    // ── 2. Process stream ─────────────────────────────────────────────────────
    if window_size == 0 {
        run_incremental(reader, writer, shared_dict, parsed_rules).await
    } else {
        run_windowed(
            reader,
            writer,
            shared_dict,
            parsed_rules,
            Duration::from_secs(window_size),
            Duration::from_secs(window_slide),
        )
        .await
    }
}

rdfc_processor_sdk::rdfc_register!("https://w3id.org/rdf-connect#KolibrieProcessor", kolibrie);

// ── Incremental mode (no windowing) ──────────────────────────────────────────

/// Accumulate triples forever; after each batch emit only the facts newly
/// derivable by Kolibrie's semi-naive materialisation.
async fn run_incremental(
    reader: Reader,
    writer: Writer,
    shared_dict: Arc<RwLock<Dictionary>>,
    parsed_rules: Vec<Rule>,
) -> Result<(), String> {
    let mut reasoner = fresh_reasoner(&parsed_rules, &shared_dict);
    let mut stream = reader.into_stream();

    while let Some(bytes) = stream.next().await {
        let text = String::from_utf8_lossy(&bytes);
        let batch = parse_ntriples(&text, &mut *shared_dict.write().unwrap());
        for triple in batch {
            reasoner.insert_ground_triple(triple);
        }

        // Returns only the facts derived in this call that were not already
        // present in the dataset index.
        let new_facts = reasoner.infer_new_facts_semi_naive();
        if !new_facts.is_empty() {
            let nt = triples_to_ntriples(&new_facts, &shared_dict.read().unwrap());
            writer.write_str(&nt).await.map_err(|e| e.to_string())?;
        }
    }

    writer.close();
    Ok(())
}

// ── Windowed mode ─────────────────────────────────────────────────────────────

/// Keep only triples received within the last `window_size`.  Re-run full
/// inference after every batch (or after `slide` has elapsed).  Emit only
/// facts not previously written (append-only, no retractions).
async fn run_windowed(
    reader: Reader,
    writer: Writer,
    shared_dict: Arc<RwLock<Dictionary>>,
    parsed_rules: Vec<Rule>,
    window_size: Duration,
    slide: Duration,
) -> Result<(), String> {
    // (arrived_at, triples)
    let mut window: VecDeque<(Instant, Vec<Triple>)> = VecDeque::new();
    // Facts already written — avoids re-emitting the same inferred fact.
    let mut written: HashSet<Triple> = HashSet::new();

    let mut last_slide = Instant::now();
    let mut stream = reader.into_stream();

    while let Some(bytes) = stream.next().await {
        let arrived = Instant::now();
        let text = String::from_utf8_lossy(&bytes);
        let batch = parse_ntriples(&text, &mut *shared_dict.write().unwrap());
        if !batch.is_empty() {
            window.push_back((arrived, batch));
        }

        // Slide: skip evaluation if the slide interval hasn't elapsed yet.
        let should_evaluate = slide.is_zero() || arrived.duration_since(last_slide) >= slide;
        if !should_evaluate {
            continue;
        }
        last_slide = arrived;

        // Evict expired batches.
        while let Some(front) = window.front() {
            if arrived.duration_since(front.0) > window_size {
                window.pop_front();
            } else {
                break;
            }
        }

        // Build a fresh Reasoner from the current window contents.
        let mut reasoner = fresh_reasoner(&parsed_rules, &shared_dict);
        for (_, triples) in &window {
            for triple in triples {
                reasoner.insert_ground_triple(triple.clone());
            }
        }

        let derived = reasoner.infer_new_facts_semi_naive();
        let mut new_facts: Vec<Triple> = Vec::new();
        for fact in derived {
            if written.insert(fact.clone()) {
                new_facts.push(fact);
            }
        }

        if !new_facts.is_empty() {
            let nt = triples_to_ntriples(&new_facts, &shared_dict.read().unwrap());
            writer.write_str(&nt).await.map_err(|e| e.to_string())?;
        }
    }

    writer.close();
    Ok(())
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Build a new [`Reasoner`] with rules loaded and using the shared dictionary.
fn fresh_reasoner(rules: &[Rule], shared_dict: &Arc<RwLock<Dictionary>>) -> Reasoner {
    let mut r = Reasoner {
        dictionary: Arc::clone(shared_dict),
        rules: Vec::new(),
        dataset_index: DatasetIndex::new(),
        rule_index: RuleIndex::new(),
        constraints: Vec::new(),
        probability_seeds: std::collections::HashMap::new(),
    };
    for rule in rules {
        // add_rule also updates rule_index; use try_add_rule to surface errors.
        if let Err(e) = r.try_add_rule(rule.clone()) {
            eprintln!("[kolibrie] warning: skipping rule: {e}");
        }
    }
    r
}

/// Resolve the rules text from either the inline parameter or a file.
fn load_rules(inline: Option<String>, file: Option<String>) -> Result<String, String> {
    match (inline, file) {
        (Some(text), _) => Ok(text),
        (None, Some(path)) => {
            std::fs::read_to_string(&path)
                .map_err(|e| format!("cannot read rules file '{path}': {e}"))
        }
        (None, None) => Err(
            "kolibrie processor: either 'rules' or 'rules_file' must be provided".to_string(),
        ),
    }
}

/// Parse N3 rules from text using Kolibrie's built-in parser.
/// The `seed_reasoner`'s dictionary is populated as a side effect (IRIs are
/// encoded into it).
fn parse_rules(text: &str, seed_reasoner: &mut Reasoner) -> Result<Vec<Rule>, String> {
    match parse_n3_document(text, seed_reasoner) {
        Ok((_, (_, rules))) => Ok(rules),
        Err(e) => Err(format!("failed to parse N3 rules: {e:?}")),
    }
}

// ── N-Triples parser ──────────────────────────────────────────────────────────

/// Parse a multi-line N-Triples document into encoded [`Triple`]s.
///
/// Terms are stored in the dictionary in the following canonical form:
/// - IRI      → `http://example.org/foo`  (without angle brackets)
/// - Literal  → `"hello"` / `"42"^^<xsd:integer>` / `"hi"@en`  (with quotes)
/// - Blank    → `_:b0`
///
/// This matches how Kolibrie's N3 rule parser encodes IRIs so that rule
/// predicates and data predicates share the same dictionary IDs.
fn parse_ntriples(text: &str, dict: &mut Dictionary) -> Vec<Triple> {
    let mut triples = Vec::new();
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        // Strip trailing ' .' and optional whitespace.
        let line = line.trim_end_matches('.').trim_end();
        if let Some((s, p, o)) = parse_nt_line(line) {
            let sid = dict.encode(&s);
            let pid = dict.encode(&p);
            let oid = dict.encode(&o);
            triples.push(Triple {
                subject: sid,
                predicate: pid,
                object: oid,
            });
        }
    }
    triples
}

/// Parse one N-Triples line into three raw term strings.
fn parse_nt_line(line: &str) -> Option<(String, String, String)> {
    let (s, rest) = parse_nt_term(line)?;
    let (p, rest) = parse_nt_term(rest.trim_start())?;
    let (o, _) = parse_nt_term(rest.trim_start())?;
    Some((s, p, o))
}

/// Consume one N-Triples term from the front of `input`.
///
/// Returns `(term_string, remaining_input)`.
fn parse_nt_term(input: &str) -> Option<(String, &str)> {
    let input = input.trim_start();
    if input.is_empty() {
        return None;
    }

    if let Some(rest) = input.strip_prefix('<') {
        // IRI — store without the angle brackets to match Kolibrie conventions.
        let end = rest.find('>')?;
        Some((rest[..end].to_owned(), &rest[end + 1..]))
    } else if input.starts_with('"') {
        // Literal — keep everything including the surrounding quotes, type and language.
        let (term, rest) = consume_literal(input)?;
        Some((term, rest))
    } else if input.starts_with("_:") {
        // Blank node.
        let end = input[2..]
            .find(|c: char| c.is_whitespace())
            .map(|i| i + 2)
            .unwrap_or(input.len());
        Some((input[..end].to_owned(), &input[end..]))
    } else {
        None
    }
}

/// Consume a quoted literal (with optional `^^<type>` or `@lang` suffix).
fn consume_literal(input: &str) -> Option<(String, &str)> {
    debug_assert!(input.starts_with('"'));
    let bytes = input.as_bytes();
    let mut i = 1; // skip opening quote
    while i < bytes.len() {
        match bytes[i] {
            b'\\' => i += 2, // escape sequence — skip two bytes
            b'"' => {
                i += 1; // skip closing quote
                let rest = &input[i..];
                let (suffix, rest) = if rest.starts_with("^^<") {
                    // typed literal
                    let inner = &rest[3..];
                    let end = inner.find('>')?;
                    (
                        format!("^^<{}>", &inner[..end]),
                        &inner[end + 1..],
                    )
                } else if rest.starts_with('@') {
                    // language-tagged literal
                    let tag_end = rest[1..]
                        .find(|c: char| c.is_whitespace())
                        .map(|n| n + 1)
                        .unwrap_or(rest.len());
                    (rest[..tag_end].to_owned(), &rest[tag_end..])
                } else {
                    (String::new(), rest)
                };
                let term = format!("{}{}", &input[..i], suffix);
                return Some((term, rest));
            }
            _ => i += 1,
        }
    }
    None // unclosed literal
}

// ── N-Triples serialiser ──────────────────────────────────────────────────────

/// Encode a slice of [`Triple`]s as an N-Triples string.
fn triples_to_ntriples(triples: &[Triple], dict: &Dictionary) -> String {
    let mut out = String::new();
    for triple in triples {
        let Some(s) = dict.decode(triple.subject) else { continue };
        let Some(p) = dict.decode(triple.predicate) else { continue };
        let Some(o) = dict.decode(triple.object) else { continue };
        out.push_str(&format!(
            "{} {} {} .\n",
            nt_term(s),
            nt_term(p),
            nt_term(o)
        ));
    }
    out
}

/// Format a dictionary term as an N-Triples token.
///
/// - Blank nodes (`_:…`) and literals (`"…`) are emitted as-is.
/// - Everything else is treated as an IRI and wrapped in `<…>`.
fn nt_term(term: &str) -> String {
    if term.starts_with("_:") || term.starts_with('"') {
        term.to_owned()
    } else {
        format!("<{}>", term)
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use rdfc_processor_sdk::testing::*;
    use rdfc_processor_sdk::make_writer;
    use serde_json::json;

    // Simple ancestry rules in N3 format.
    const RULES: &str = r#"
@prefix ex: <http://example.org/> .

{ ?x ex:parent ?y } => { ?x ex:ancestor ?y }
{ ?x ex:parent ?y . ?y ex:ancestor ?z } => { ?x ex:ancestor ?z }
"#;

    // Two N-Triples messages:
    //   message 1: Alice parent Bob
    //   message 2: Bob   parent Charlie
    const MSG1: &[u8] =
        b"<http://example.org/Alice> <http://example.org/parent> <http://example.org/Bob> .";
    const MSG2: &[u8] =
        b"<http://example.org/Bob> <http://example.org/parent> <http://example.org/Charlie> .";

    async fn run_processor(messages: Vec<Vec<u8>>) -> Vec<u8> {
        let reader = make_test_reader("mock://reader", messages);
        let (writer, bridge) = make_writer("mock://writer".to_string());
        let args = make_test_args(
            json!({
                "reader": {"@type": "https://w3id.org/rdf-connect#Reader", "@id": "mock://reader"},
                "writer": {"@type": "https://w3id.org/rdf-connect#Writer", "@id": "mock://writer"},
                "rules": RULES
            }),
            [("reader", reader)],
            [("writer", writer)],
        );

        let (result, (received, _)) =
            tokio::join!(super::kolibrie(args), collect_writer(bridge));
        result.unwrap();
        received.concat()
    }

    #[tokio::test]
    async fn derives_ancestor_from_single_message() {
        let output = run_processor(vec![MSG1.to_vec()]).await;
        let text = String::from_utf8(output).unwrap();
        assert!(
            text.contains("ancestor"),
            "expected 'ancestor' in output, got: {text}"
        );
        assert!(
            text.contains("Alice"),
            "expected 'Alice' in output, got: {text}"
        );
    }

    #[tokio::test]
    async fn derives_transitive_ancestor_across_messages() {
        let output = run_processor(vec![MSG1.to_vec(), MSG2.to_vec()]).await;
        let text = String::from_utf8(output).unwrap();
        assert!(
            text.contains("ancestor"),
            "expected 'ancestor' in output, got: {text}"
        );
    }

    #[tokio::test]
    async fn empty_stream_closes_writer_cleanly() {
        let reader = make_test_reader("mock://reader", vec![]);
        let (writer, bridge) = make_writer("mock://writer".to_string());
        let args = make_test_args(
            json!({
                "reader": {"@type": "https://w3id.org/rdf-connect#Reader", "@id": "mock://reader"},
                "writer": {"@type": "https://w3id.org/rdf-connect#Writer", "@id": "mock://writer"},
                "rules": RULES
            }),
            [("reader", reader)],
            [("writer", writer)],
        );

        let (result, (received, closed)) =
            tokio::join!(super::kolibrie(args), collect_writer(bridge));
        result.unwrap();
        assert!(received.is_empty());
        assert!(closed);
    }

    // ── N-Triples parser unit tests ───────────────────────────────────────────

    #[test]
    fn parses_uri_triple() {
        let mut dict = Dictionary::new();
        let triples = parse_ntriples(
            "<http://s> <http://p> <http://o> .\n",
            &mut dict,
        );
        assert_eq!(triples.len(), 1);
        assert_eq!(dict.decode(triples[0].subject), Some("http://s"));
        assert_eq!(dict.decode(triples[0].predicate), Some("http://p"));
        assert_eq!(dict.decode(triples[0].object), Some("http://o"));
    }

    #[test]
    fn parses_typed_literal() {
        let mut dict = Dictionary::new();
        let triples = parse_ntriples(
            r#"<http://s> <http://p> "42"^^<http://www.w3.org/2001/XMLSchema#integer> ."#,
            &mut dict,
        );
        assert_eq!(triples.len(), 1);
        let o = dict.decode(triples[0].object).unwrap();
        assert!(o.contains("42"), "got: {o}");
    }

    #[test]
    fn parses_language_literal() {
        let mut dict = Dictionary::new();
        let triples = parse_ntriples(
            r#"<http://s> <http://p> "hello"@en ."#,
            &mut dict,
        );
        assert_eq!(triples.len(), 1);
        let o = dict.decode(triples[0].object).unwrap();
        assert_eq!(o, r#""hello"@en"#);
    }

    #[test]
    fn skips_comments_and_blank_lines() {
        let mut dict = Dictionary::new();
        let triples = parse_ntriples(
            "# comment\n\n<http://s> <http://p> <http://o> .\n",
            &mut dict,
        );
        assert_eq!(triples.len(), 1);
    }

    #[test]
    fn nt_term_wraps_iri() {
        assert_eq!(nt_term("http://example.org/"), "<http://example.org/>");
    }

    #[test]
    fn nt_term_passes_through_literal() {
        assert_eq!(nt_term(r#""hello"@en"#), r#""hello"@en"#);
    }

    #[test]
    fn nt_term_passes_through_bnode() {
        assert_eq!(nt_term("_:b0"), "_:b0");
    }
}
