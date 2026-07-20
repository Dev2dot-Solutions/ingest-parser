//! ingest-parser — High-performance tree-sitter parse engine.
//!
//! Accepts file paths on stdin (one per line), parses them in parallel
//! using native tree-sitter, and outputs structured JSON on stdout.
//!
//! Usage:
//!   find /path/to/repo -name "*.ts" -o -name "*.go" | ingest-parser > results.json
//!
//! Or from Go:
//!   cmd := exec.Command("ingest-parser")
//!   cmd.Stdin = strings.NewReader(filePaths)
//!   output, _ := cmd.Output()

use anyhow::{Context, Result};
use streaming_iterator::StreamingIterator;
use rayon::prelude::*;
use serde::Serialize;
use std::collections::HashMap;
use std::io::{self, BufRead};
use std::time::Instant;
use tree_sitter::{Language, Parser, Query, QueryCursor};

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ParseResult {
    path: String,
    functions: Vec<Function>,
    classes: Vec<Class>,
    imports: Vec<Import>,
    calls: Vec<FunctionCall>,
    error: Option<String>,
    duration_us: u64,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct Function {
    name: String,
    signature: String,
    line_start: usize,
    line_end: usize,
    doc_comment: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct Class {
    name: String,
    parent_class: Option<String>,
    interfaces: Vec<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct Import {
    source_entity: String,
    target_entity: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct FunctionCall {
    caller_name: String,
    callee_name: String,
}

// ---------------------------------------------------------------------------
// Language detection
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum LangId {
    TypeScript,
    Tsx,
    JavaScript,
    Go,
    Kotlin,
    Rust,
    Vue,
    Html,
    Css,
}

fn detect_language(path: &str) -> Option<LangId> {
    let ext = match path.rsplit('.').next() {
        Some(e) => e.to_lowercase(),
        None => return None,
    };
    match ext.as_str() {
        "ts" => Some(LangId::TypeScript),
        "tsx" => Some(LangId::Tsx),
        "js" | "jsx" | "mjs" | "cjs" => Some(LangId::JavaScript),
        "go" => Some(LangId::Go),
        "kt" | "kts" => Some(LangId::Kotlin),
        "rs" => Some(LangId::Rust),
        "vue" => Some(LangId::Vue),
        "html" | "htm" => Some(LangId::Html),
        "css" | "scss" => Some(LangId::Css),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Grammar registry — compiled-in static grammars
// ---------------------------------------------------------------------------

struct Grammar {
    lang: Language,
    queries: QuerySet,
}

struct QuerySet {
    functions: Option<Query>,
    classes: Option<Query>,
    imports: Option<Query>,
    calls: Option<Query>,
}

fn init_grammars() -> Result<HashMap<LangId, Grammar>> {
    let mut map = HashMap::new();

    // Helper: create a grammar entry for a language
    let mut make = |id: LangId, lang: Language, qs: QuerySet| -> Result<()> {
        map.insert(id, Grammar { lang, queries: qs });
        Ok(())
    };

    // TypeScript
    make(LangId::TypeScript, tree_sitter_typescript::LANGUAGE_TYPESCRIPT.into(), QuerySet {
        functions: Some(Query::new(
            &tree_sitter_typescript::LANGUAGE_TYPESCRIPT.into(),
            "(function_declaration name: (identifier) @name) @func
             (variable_declarator name: (identifier) @name value: (arrow_function)) @arrow
             (method_definition name: (property_identifier) @name) @method
             (variable_declarator name: (identifier) @name value: (function_expression)) @func_expr",
        )?),
        classes: Some(Query::new(
            &tree_sitter_typescript::LANGUAGE_TYPESCRIPT.into(),
            "(class_declaration name: (type_identifier) @name) @class",
        )?),
        imports: Some(Query::new(
            &tree_sitter_typescript::LANGUAGE_TYPESCRIPT.into(),
            "(import_statement source: (string) @source) @import",
        )?),
        calls: Some(Query::new(
            &tree_sitter_typescript::LANGUAGE_TYPESCRIPT.into(),
            "(call_expression function: (identifier) @callee) @call
             (call_expression function: (member_expression property: (property_identifier) @callee)) @method_call",
        )?),
    })?;

    // TSX
    make(LangId::Tsx, tree_sitter_typescript::LANGUAGE_TSX.into(), QuerySet {
        functions: Some(Query::new(
            &tree_sitter_typescript::LANGUAGE_TSX.into(),
            "(function_declaration name: (identifier) @name) @func
             (variable_declarator name: (identifier) @name value: (arrow_function)) @arrow
             (method_definition name: (property_identifier) @name) @method
             (variable_declarator name: (identifier) @name value: (function_expression)) @func_expr",
        )?),
        classes: Some(Query::new(
            &tree_sitter_typescript::LANGUAGE_TSX.into(),
            "(class_declaration name: (type_identifier) @name) @class",
        )?),
        imports: Some(Query::new(
            &tree_sitter_typescript::LANGUAGE_TSX.into(),
            "(import_statement source: (string) @source) @import",
        )?),
        calls: Some(Query::new(
            &tree_sitter_typescript::LANGUAGE_TSX.into(),
            "(call_expression function: (identifier) @callee) @call
             (call_expression function: (member_expression property: (property_identifier) @callee)) @method_call",
        )?),
    })?;

    // JavaScript
    make(LangId::JavaScript, tree_sitter_javascript::LANGUAGE.into(), QuerySet {
        functions: Some(Query::new(
            &tree_sitter_javascript::LANGUAGE.into(),
            "(function_declaration name: (identifier) @name) @func
             (variable_declarator name: (identifier) @name value: (arrow_function)) @arrow
             (variable_declarator name: (identifier) @name value: (function_expression)) @func_expr
             (method_definition name: (property_identifier) @name) @method",
        )?),
        classes: Some(Query::new(
            &tree_sitter_javascript::LANGUAGE.into(),
            "(class_declaration name: (identifier) @name) @class",
        )?),
        imports: Some(Query::new(
            &tree_sitter_javascript::LANGUAGE.into(),
            "(import_statement source: (string) @source) @import",
        )?),
        calls: Some(Query::new(
            &tree_sitter_javascript::LANGUAGE.into(),
            "(call_expression function: (identifier) @callee) @call
             (call_expression function: (member_expression property: (property_identifier) @callee)) @method_call",
        )?),
    })?;

    // Kotlin (via tree-sitter-kotlin-ng, the tree-sitter 0.24 API-compatible fork)
    make(LangId::Kotlin, tree_sitter_kotlin_ng::LANGUAGE.into(), QuerySet {
        functions: Some(Query::new(
            &tree_sitter_kotlin_ng::LANGUAGE.into(),
            "(function_declaration name: (identifier) @name) @func",
        )?),
        classes: Some(Query::new(
            &tree_sitter_kotlin_ng::LANGUAGE.into(),
            "(class_declaration name: (identifier) @name) @class
             (object_declaration name: (identifier) @name) @class",
        )?),
        imports: Some(Query::new(
            &tree_sitter_kotlin_ng::LANGUAGE.into(),
            "(import (identifier) @source) @import
             (import (qualified_identifier) @source) @import",
        )?),
        calls: Some(Query::new(
            &tree_sitter_kotlin_ng::LANGUAGE.into(),
            "(call_expression (identifier) @callee) @call",
        )?),
    })?;

    // Go
    make(LangId::Go, tree_sitter_go::LANGUAGE.into(), QuerySet {
        functions: Some(Query::new(
            &tree_sitter_go::LANGUAGE.into(),
            "(function_declaration name: (identifier) @name) @func",
        )?),
        classes: None,
        imports: Some(Query::new(
            &tree_sitter_go::LANGUAGE.into(),
            "(import_spec (interpreted_string_literal) @source) @import",
        )?),
        calls: Some(Query::new(
            &tree_sitter_go::LANGUAGE.into(),
            "(call_expression function: (identifier) @callee) @call",
        )?),
    })?;

    // Rust (tree-sitter-rust 0.24.0, exact-pinned for tree-sitter 0.24 API)
    make(LangId::Rust, tree_sitter_rust::LANGUAGE.into(), QuerySet {
        functions: Some(Query::new(
            &tree_sitter_rust::LANGUAGE.into(),
            "(function_item name: (identifier) @name) @func",
        )?),
        classes: Some(Query::new(
            &tree_sitter_rust::LANGUAGE.into(),
            "(struct_item name: (type_identifier) @name) @class
             (enum_item name: (type_identifier) @name) @class
             (trait_item name: (type_identifier) @name) @class",
        )?),
        imports: Some(Query::new(
            &tree_sitter_rust::LANGUAGE.into(),
            "(use_declaration argument: (scoped_identifier) @source) @import
             (use_declaration argument: (identifier) @source) @import",
        )?),
        calls: Some(Query::new(
            &tree_sitter_rust::LANGUAGE.into(),
            "(call_expression function: (identifier) @callee) @call
             (call_expression function: (scoped_identifier) @callee) @call
             (call_expression function: (field_expression field: (field_identifier) @callee)) @method_call",
        )?),
    })?;

    // HTML — no queries (tracked but not parsed for entities)
    make(LangId::Html, tree_sitter_html::LANGUAGE.into(), QuerySet {
        functions: None,
        classes: None,
        imports: None,
        calls: None,
    })?;

    // CSS — no queries (tracked but not parsed for entities)
    make(LangId::Css, tree_sitter_css::LANGUAGE.into(), QuerySet {
        functions: None,
        classes: None,
        imports: None,
        calls: None,
    })?;

    Ok(map)
}

// ---------------------------------------------------------------------------
// Parse a single file
// ---------------------------------------------------------------------------

fn parse_file(path: &str, source: &str, grammar: &Grammar) -> ParseResult {
    let start = Instant::now();
    let mut result = ParseResult {
        path: path.to_string(),
        functions: Vec::new(),
        classes: Vec::new(),
        imports: Vec::new(),
        calls: Vec::new(),
        error: None,
        duration_us: 0,
    };

    let mut parser = Parser::new();
    parser.set_language(&grammar.lang).ok();
    let tree = match parser.parse(source, None) {
        Some(t) => t,
        None => {
            result.error = Some("parser returned no tree".to_string());
            result.duration_us = start.elapsed().as_micros() as u64;
            return result;
        }
    };

    let source_bytes = source.as_bytes();
    let root = tree.root_node();

    // Track functions for caller resolution
    let mut function_names: HashMap<(usize, usize), String> = HashMap::new();

    // ── Functions ──────────────────────────────────────────────────────
    if let Some(ref query) = grammar.queries.functions {
        let mut cursor = QueryCursor::new();
        let mut qm = cursor.matches(query, root, source_bytes); while let Some(match_) = qm.next() {
            let mut name_node = None;
            let mut func_node = None;

            for cap in match_.captures {
                match query.capture_names()[cap.index as usize] {
                    "name" => name_node = Some(cap.node),
                    "func" | "arrow" | "method" | "func_expr" => func_node = Some(cap.node),
                    _ => {}
                }
            }

            if let Some(name) = name_node {
                let name_text = name.utf8_text(source_bytes).unwrap_or("");
                if name_text.is_empty() {
                    continue;
                }

                let (sig, line_end) = if let Some(fn_node) = func_node {
                    let fn_text = fn_node.utf8_text(source_bytes).unwrap_or("");
                    let sig_end = fn_text.find('{').unwrap_or(fn_text.len());
                    (fn_text[..sig_end].trim().to_string(), fn_node.end_position().row + 1)
                } else {
                    (name_text.to_string(), name.end_position().row + 1)
                };

                let doc_comment = extract_doc_comment(&name, source_bytes);

                result.functions.push(Function {
                    name: name_text.to_string(),
                    signature: sig,
                    line_start: name.start_position().row + 1,
                    line_end,
                    doc_comment,
                });

                function_names.insert(
                    (name.start_position().row, name.end_position().row),
                    name_text.to_string(),
                );
            }
        }
    }

    // ── Classes ────────────────────────────────────────────────────────
    if let Some(ref query) = grammar.queries.classes {
        let mut cursor = QueryCursor::new();
        let mut qm = cursor.matches(query, root, source_bytes); while let Some(match_) = qm.next() {
            let mut name_node = None;
            for cap in match_.captures {
                match query.capture_names()[cap.index as usize] {
                    "name" => name_node = Some(cap.node),
                    _ => {}
                }
            }

            if let Some(name) = name_node {
                let name_text = name.utf8_text(source_bytes).unwrap_or("");
                if name_text.is_empty() {
                    continue;
                }

                let (parent, interfaces) = extract_class_info(match_, query, source_bytes);
                result.classes.push(Class {
                    name: name_text.to_string(),
                    parent_class: parent,
                    interfaces,
                });
            }
        }
    }

    // ── Imports ────────────────────────────────────────────────────────
    if let Some(ref query) = grammar.queries.imports {
        let mut cursor = QueryCursor::new();
        let mut qm = cursor.matches(query, root, source_bytes); while let Some(match_) = qm.next() {
            let mut source_node = None;
            let mut target_node = None;

            for cap in match_.captures {
                match query.capture_names()[cap.index as usize] {
                    "source" => source_node = Some(cap.node),
                    "target" => target_node = Some(cap.node),
                    _ => {}
                }
            }

            if let Some(s) = source_node {
                let raw = s.utf8_text(source_bytes).unwrap_or("");
                let module = raw.trim_matches('\'').trim_matches('"');

                // For TypeScript/JS, extract imported names from match context
                let imp_text = match_.captures.iter().find(|c| query.capture_names()[c.index as usize] == "import")
                    .and_then(|c| c.node.utf8_text(source_bytes).ok());

                if let Some(imp) = imp_text {
                    // Named imports: { Foo, Bar }
                    if let Some(named) = imp.split('{').nth(1).and_then(|s| s.split('}').next()) {
                        for item in named.split(',').map(|s| s.trim()).filter(|s| !s.is_empty()) {
                            let name = if let Some(as_name) = item.split(" as ").nth(1) {
                                as_name.trim()
                            } else {
                                item.split_whitespace().next().unwrap_or(item)
                            };
                            result.imports.push(Import {
                                source_entity: name.to_string(),
                                target_entity: module.to_string(),
                            });
                        }
                    } else if imp.contains(" from ") {
                        // Default import: import Foo from ...
                        let default = imp.split("import ").nth(1)
                            .and_then(|s| s.split(" from ").next())
                            .map(|s| s.trim())
                            .unwrap_or("");
                        if !default.is_empty() && default != "{" {
                            result.imports.push(Import {
                                source_entity: default.to_string(),
                                target_entity: module.to_string(),
                            });
                        }
                    } else if imp.contains("* as ") {
                        // Namespace: import * as Foo from ...
                        let ns = imp.split("* as ").nth(1)
                            .and_then(|s| s.split_whitespace().next())
                            .unwrap_or("");
                        if !ns.is_empty() {
                            result.imports.push(Import {
                                source_entity: ns.to_string(),
                                target_entity: module.to_string(),
                            });
                        }
                    } else {
                        // Side-effect import: import 'module'
                        result.imports.push(Import {
                            source_entity: String::new(),
                            target_entity: module.to_string(),
                        });
                    }
                } else if let Some(t) = target_node {
                    // Kotlin: import com.example.Foo
                    let import_path = t.utf8_text(source_bytes).unwrap_or("");
                    let parts: Vec<&str> = import_path.split('.').collect();
                    let entity = parts.last().unwrap_or(&import_path).to_string();
                    result.imports.push(Import {
                        source_entity: entity,
                        target_entity: import_path.to_string(),
                    });
                } else if grammar.queries.calls.is_some() {
                    // Go: import "module/path"
                    let parts: Vec<&str> = module.split('/').collect();
                    let entity = parts.last().unwrap_or(&module).to_string();
                    result.imports.push(Import {
                        source_entity: entity,
                        target_entity: module.to_string(),
                    });
                }
            }
        }
    }

    // ── Function Calls ─────────────────────────────────────────────────
    if let Some(ref query) = grammar.queries.calls {
        let mut cursor = QueryCursor::new();
        let mut qm = cursor.matches(query, root, source_bytes); while let Some(match_) = qm.next() {
            let mut callee_node = None;
            for cap in match_.captures {
                match query.capture_names()[cap.index as usize] {
                    "callee" => callee_node = Some(cap.node),
                    _ => {}
                }
            }

            if let Some(callee) = callee_node {
                let callee_name = callee.utf8_text(source_bytes).unwrap_or("");
                if callee_name.is_empty() {
                    continue;
                }

                // Find enclosing function (caller)
                let caller = find_enclosing_function(callee, source_bytes);
                if let Some(caller_name) = caller {
                    if caller_name != callee_name {
                        result.calls.push(FunctionCall {
                            caller_name,
                            callee_name: callee_name.to_string(),
                        });
                    }
                }
            }
        }
    }

    result.duration_us = start.elapsed().as_micros() as u64;
    result
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Extract a doc comment preceding a syntax node.
fn extract_doc_comment<'a>(node: &tree_sitter::Node, source: &'a [u8]) -> Option<String> {
    // Check previous named sibling
    if let Some(prev) = node.prev_named_sibling() {
        if prev.kind() == "comment" {
            return prev.utf8_text(source).ok().map(|s| s.to_string());
        }
    }
    // Check previous sibling
    if let Some(prev) = node.prev_sibling() {
        if prev.kind() == "comment" {
            return prev.utf8_text(source).ok().map(|s| s.to_string());
        }
        // Sometimes comment is two siblings away (separated by semicolon)
        if prev.kind() == ";" {
            if let Some(prev2) = prev.prev_sibling() {
                if prev2.kind() == "comment" {
                    return prev2.utf8_text(source).ok().map(|s| s.to_string());
                }
            }
        }
    }
    None
}

/// Extract class heritage information from query captures.
fn extract_class_info(
    match_: &tree_sitter::QueryMatch,
    query: &Query,
    source: &[u8],
) -> (Option<String>, Vec<String>) {
    let mut parent = None;
    let mut interfaces = Vec::new();

    // Find the full class text from the @class capture
    for cap in match_.captures {
        if query.capture_names()[cap.index as usize] == "class" {
            if let Ok(text) = cap.node.utf8_text(source) {
                // Extract extends
                if let Some(ext) = text.split("extends").nth(1) {
                    let name = ext.split(|c: char| c.is_whitespace() || c == '{')
                        .next()
                        .unwrap_or("")
                        .trim()
                        .trim_end_matches(|c: char| c == '<' || c == ',' || c == '>');
                    if !name.is_empty() {
                        parent = Some(name.to_string());
                    }
                }
                // Extract implements
                if let Some(impls) = text.split("implements").nth(1) {
                    let raw = impls.split('{').next().unwrap_or("");
                    for iface in raw.split(',').map(|s| s.trim()).filter(|s| !s.is_empty()) {
                        let clean = iface.trim_end_matches(|c: char| c == '<' || c == ',' || c == '>');
                        interfaces.push(clean.to_string());
                    }
                }
            }
        }
    }

    (parent, interfaces)
}

/// Walk up the syntax tree from a node to find the enclosing function name.
fn find_enclosing_function(node: tree_sitter::Node, source: &[u8]) -> Option<String> {
    let mut current = node.parent();

    while let Some(parent) = current {
        match parent.kind() {
            "function_declaration"
            | "method_definition"
            | "arrow_function"
            | "function_expression"
            | "generator_function_declaration" =>
            {
                // Look for the name identifier in children
                for i in 0..parent.named_child_count() {
                    if let Some(child) = parent.named_child(i) {
                        match child.kind() {
                            "identifier" | "property_identifier" | "simple_identifier" => {
                                return child.utf8_text(source).ok().map(|s| s.to_string());
                            }
                            _ => {}
                        }
                    }
                }
                // For arrow/expression functions, check parent variable declarator
                if parent.kind() == "arrow_function" || parent.kind() == "function_expression" {
                    if let Some(var_decl) = parent.parent() {
                        if var_decl.kind() == "variable_declarator" {
                            if let Some(name_child) = var_decl.child(0) {
                                return name_child.utf8_text(source).ok().map(|s| s.to_string());
                            }
                        }
                    }
                }
                return None;
            }
            "program" | "statement_block" | "source_file" => return None,
            _ => {}
        }

        current = parent.parent();
    }

    None
}

/// Extract the first <script> block from a Vue single-file component.
/// Returns (block_content, language to parse with).
/// Note: line numbers in the parse output are relative to the start of the
/// script block, not the .vue file (v1 limitation).
fn extract_vue_script(source: &str) -> Option<(String, LangId)> {
    let lower = source.to_lowercase();
    let tag_start = lower.find("<script")?;
    let tag_end = lower[tag_start..].find('>')? + tag_start;
    let attrs = &lower[tag_start..tag_end];

    let lang = if attrs.contains("lang=\"tsx\"") || attrs.contains("lang='tsx'") {
        LangId::Tsx
    } else if attrs.contains("lang=\"ts\"") || attrs.contains("lang='ts'") {
        LangId::TypeScript
    } else {
        LangId::JavaScript
    };

    let content_start = tag_end + 1;
    let content_end = lower[content_start..].find("</script>")? + content_start;
    if content_end <= content_start {
        return None;
    }
    let content = source[content_start..content_end].to_string();
    if content.trim().is_empty() {
        return None;
    }
    Some((content, lang))
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

fn run() -> Result<()> {
    // Initialize all grammars (done once, before parallel work)
    let grammars = init_grammars().context("failed to initialize grammars")?;

    // Read file paths from stdin
    let stdin = io::stdin();
    let paths: Vec<String> = stdin
        .lock()
        .lines()
        .filter_map(|line| line.ok().map(|l| l.trim().to_string()))
        .filter(|l| !l.is_empty())
        .collect();

    if paths.is_empty() {
        eprintln!("[ingest-parser] No input paths provided on stdin");
        eprintln!("Usage: find /repo -type f | ingest-parser");
        return Ok(());
    }

    eprintln!("[ingest-parser] Processing {} files...", paths.len());

    // Process files in parallel
    let results: Vec<ParseResult> = paths
        .par_iter()
        .map(|path| {
            let lang_id = match detect_language(path) {
                Some(id) => id,
                None => {
                    return ParseResult {
                        path: path.clone(),
                        functions: Vec::new(),
                        classes: Vec::new(),
                        imports: Vec::new(),
                        calls: Vec::new(),
                        error: Some("unsupported language".to_string()),
                        duration_us: 0,
                    };
                }
            };

            let source = match std::fs::read_to_string(path) {
                Ok(s) => s,
                Err(e) => {
                    return ParseResult {
                        path: path.clone(),
                        functions: Vec::new(),
                        classes: Vec::new(),
                        imports: Vec::new(),
                        calls: Vec::new(),
                        error: Some(format!("read error: {}", e)),
                        duration_us: 0,
                    };
                }
            };

            // Vue SFCs: extract the <script> block and parse it with the
            // TypeScript/TSX/JavaScript grammar indicated by the lang attribute.
            let (parse_source, grammar_id) = if lang_id == LangId::Vue {
                match extract_vue_script(&source) {
                    Some((script, gid)) => (script, gid),
                    None => {
                        // Template/style-only component — nothing parseable
                        return ParseResult {
                            path: path.clone(),
                            functions: Vec::new(),
                            classes: Vec::new(),
                            imports: Vec::new(),
                            calls: Vec::new(),
                            error: None,
                            duration_us: 0,
                        };
                    }
                }
            } else {
                (source, lang_id)
            };

            let grammar = match grammars.get(&grammar_id) {
                Some(g) => g,
                None => {
                    return ParseResult {
                        path: path.clone(),
                        functions: Vec::new(),
                        classes: Vec::new(),
                        imports: Vec::new(),
                        calls: Vec::new(),
                        error: Some(format!("grammar not loaded for {:?}", grammar_id)),
                        duration_us: 0,
                    };
                }
            };

            parse_file(path, &parse_source, grammar)
        })
        .collect();

    // Count stats
    let total_ok = results.iter().filter(|r| r.error.is_none()).count();
    let total_err = results.iter().filter(|r| r.error.is_some()).count();
    let total_fns: usize = results.iter().map(|r| r.functions.len()).sum();
    let total_cls: usize = results.iter().map(|r| r.classes.len()).sum();
    let total_imp: usize = results.iter().map(|r| r.imports.len()).sum();
    let total_calls: usize = results.iter().map(|r| r.calls.len()).sum();

    eprintln!("[ingest-parser] Done: {} OK, {} failed, {} functions, {} classes, {} imports, {} calls",
        total_ok, total_err, total_fns, total_cls, total_imp, total_calls);

    // Output JSON to stdout
    serde_json::to_writer(io::stdout(), &results)?;
    println!();

    Ok(())
}

fn main() {
    if let Err(e) = run() {
        eprintln!("[ingest-parser] Fatal: {:#}", e);
        std::process::exit(1);
    }
}
