---
marp: true
theme: default
paginate: true
html: true
color: #2b3a4e
#
# Slide Deck: Full Text Search for ScyllaDB — Architecture & Design Review
#
# This is a Marp Markdown slide deck. To present or export:
#
# 1. VS Code: Install the "Marp for VS Code" extension, open this file, and
#    click the preview icon. Present directly or export from the command palette.
#
# 2. CLI export (PDF, PPTX, or HTML):
#      npm install -g @marp-team/marp-cli
#      marp full-text-search-review-deck.md --pdf
#      marp full-text-search-review-deck.md --pptx
#      marp full-text-search-review-deck.md --html
#
# 3. Present in browser with live navigation:
#      marp --server docs/product/
#    Press P in the browser to enter presenter mode.
#
style: |
  @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');

  :root {
    --blue-50: #f5f3ff;
    --blue-100: #ede9fe;
    --blue-200: #ddd6fe;
    --blue-300: #c4b5fd;
    --blue-400: #a78bfa;
    --blue-500: #8b5cf6;
    --blue-600: #7c3aed;
    --blue-700: #6d28d9;
    --slate-700: #334155;
    --slate-800: #1e293b;
    --slate-500: #64748b;
  }

  section {
    font-family: 'Inter', 'Segoe UI', system-ui, -apple-system, sans-serif;
    font-size: 23px;
    background: linear-gradient(180deg, #faf8ff 0%, #f3eefe 100%);
    padding: 48px 60px 56px;
    display: flex;
    flex-direction: column;
    justify-content: flex-start;
  }

  section::before {
    content: '';
    position: absolute;
    top: 16px;
    right: 20px;
    width: 80px;
    height: 80px;
    background: url('https://www.scylladb.com/wp-content/uploads/scylla-opensource-4.png') no-repeat center / contain;
    opacity: 1.00;
    pointer-events: none;
    z-index: 0;
  }

  section::after {
    font-size: 14px;
    color: var(--slate-500);
  }

  h1 {
    font-size: 38px;
    font-weight: 700;
    color: var(--blue-700);
    border-bottom: 3px solid var(--blue-200);
    padding-bottom: 8px;
    margin-bottom: 24px;
  }

  h2 {
    font-size: 30px;
    font-weight: 600;
    color: var(--blue-600);
  }

  h3 {
    font-size: 24px;
    font-weight: 600;
    color: var(--slate-700);
  }

  /* Lead / section divider slides */
  section.lead {
    background: linear-gradient(135deg, #4c1d95 0%, #5b21b6 20%, #7c3aed 45%, #8b5cf6 65%, #a78bfa 85%, #c4b5fd 100%);
    color: #ffffff;
    text-align: center;
    display: flex;
    flex-direction: column;
    justify-content: center;
    align-items: center;
    padding: 48px 60px;
  }
  section.lead h1 {
    color: #ffffff;
    border-bottom: 3px solid rgba(255,255,255,0.3);
    font-size: 44px;
  }
  section.lead h3 {
    color: var(--blue-200);
    font-weight: 400;
  }
  section.lead strong {
    color: var(--blue-100);
  }
  section.lead p {
    color: var(--blue-100);
  }
  section.lead ol, section.lead ul {
    text-align: left;
    display: inline-block;
    color: var(--blue-100);
  }
  section.lead li {
    color: var(--blue-100);
  }
  section.lead li::marker {
    color: var(--blue-200);
  }

  /* Tables */
  table {
    font-size: 17px;
    width: max-content !important;
    border-collapse: separate;
    border-spacing: 0;
    border-radius: 8px;
    overflow: hidden;
    box-shadow: 0 1px 3px rgba(0,0,0,0.06);
  }
  thead th {
    background: var(--blue-600);
    color: #ffffff;
    font-weight: 600;
    padding: 10px 14px;
    text-align: left;
    border: none;
  }
  tbody td {
    padding: 8px 14px;
    border-bottom: 1px solid var(--blue-100);
    background: #ffffff;
  }
  tbody tr:nth-child(even) td {
    background: var(--blue-50);
  }
  tbody tr:last-child td {
    border-bottom: none;
  }

  /* Code blocks */
  code {
    font-size: 17px;
    font-family: 'JetBrains Mono', 'Fira Code', 'Cascadia Code', monospace;
    background: var(--blue-50);
    border: 1px solid var(--blue-200);
    border-radius: 4px;
    padding: 1px 6px;
    color: var(--blue-700);
  }
  pre {
    background: var(--blue-50) !important;
    border: 1px solid var(--blue-200);
    border-radius: 10px;
    padding: 14px 20px !important;
    box-shadow: 0 1px 3px rgba(0,0,0,0.06);
    margin: 8px 0;
  }
  pre code {
    color: var(--slate-800);
    background: transparent;
    border: none;
    font-size: 14px;
    line-height: 1.45;
  }

  /* Blockquotes */
  blockquote {
    font-size: 20px;
    border-left: 4px solid var(--blue-400);
    background: var(--blue-50);
    padding: 12px 20px;
    border-radius: 0 8px 8px 0;
    margin: 16px 0;
    color: var(--slate-700);
  }

  /* Flex columns */
  .columns { display: flex; gap: 36px; }
  .col { flex: 1; min-width: 0; }
  .col table { width: 100% !important; }

  /* Strong text accent */
  strong { color: var(--blue-700); }

  /* Links */
  a { color: var(--blue-500); text-decoration: none; }
  a:hover { text-decoration: underline; }

  /* Bullet styling */
  ul, ol { margin-left: 0; padding-left: 24px; }
  li { margin-bottom: 4px; }
  li::marker { color: var(--blue-400); }
---

<!-- _class: lead -->

# Full Text Search for ScyllaDB

### Architecture & Design Review

Core Arch Review Meeting
60 minutes · March 2026

---

<!-- _class: no-anim -->

# Agenda (60 min)

| Time | Topic |
|------|-------|
| 0–5 min | **Problem Statement** — Why FTS, why now |
| 5–15 min | **Solution & Architecture** — Index-node approach, colocation model |
| 15–25 min | **Alternatives Considered** — SAI, OpenSearch compat, dedicated nodes |
| 25–35 min | **CQL API Design** — MATCH(), USING FUSION, analyzers |
| 35–45 min | **Milestones & Roadmap** — M1 → M4 scope & timelines |
| 45–55 min | **Operational Model** — Performance, HA, configuration, monitoring |
| 55–60 min | **Open Questions & Discussion** |

---

<!-- _class: lead -->

# The Problem

---

# The Gap: Users Leave ScyllaDB for Search

**Modern applications need keyword search, semantic similarity, and filtering in a single query.** Today, ScyllaDB users requiring full-text search must deploy external engines (Elasticsearch/OpenSearch).

<div class="columns">
<div class="col">

### Pain Points

- **Operational complexity** — separate cluster, sync pipelines, dual-write logic
- **Data consistency** — stale results from async sync
- **Increased latency** — extra network hops per query
- **Higher infra cost** — duplicated storage & compute
- **No hybrid search** — combining vector + keyword requires client-side orchestration

</div>
<div class="col">

### The Opportunity

With **Vector Search already native** in ScyllaDB, full-text search is the **last remaining gap** forcing users to external systems.

> Semantic search alone struggles with **rare/precise concepts** — proper nouns, SKUs, error codes, addresses. FTS excels at exact keyword matching. Production AI apps combine both via **hybrid search**.

</div>
</div>

---

# Strategic Value to ScyllaDB

| Benefit | Impact |
|---------|--------|
| **Increased Vector Search value** | FTS + Hybrid Search extend existing offering, increase adoption — no new SKU |
| **Competitive parity** | Matches and exceeds DataStax Enterprise (DSE Search) and Astra DB capabilities |
| **Reduced churn risk** | Eliminates need for external Elasticsearch/OpenSearch clusters |
| **AI/ML platform play** | Positions ScyllaDB as unified data platform for AI-powered applications (RAG, conversational agents, semantic product search) |

---

<!-- _class: lead -->

# Solution & Architecture

---

# Core Concepts

| Concept | Description |
|---------|-------------|
| **Full-Text Index** | Inverted index from `text` columns. Stores tokenized terms, positional info, term frequencies for BM25. |
| **Analyzer** | Pipeline of character filters → tokenizer → token filters. Applied at index-time and query-time. |
| **BM25 Scoring** | Probabilistic ranking function (same as Lucene/Elasticsearch). Term frequency saturation + document length normalization. |
| **Segment** | Immutable sub-index. New docs → new segment; segments merged in background. In-memory for M1–M3; disk in M4. |
| **Hybrid Search** | Combines FTS relevance score + vector similarity score via configurable fusion (e.g., RRF). |

**Search Engine Library:** [Tantivy](https://github.com/quickwit-oss/tantivy) — Rust, Lucene-inspired, BM25, 17+ language analyzers, phrase/boolean/fuzzy queries, incremental indexing. **Final choice will be validated in Technical Design Document.**

---

# Inverted Index Structure — Example

Given three documents in a `comments` table:

- **Doc 1:** "ScyllaDB scales reads and scales writes"
- **Doc 2:** "Vector search scales with search embeddings"
- **Doc 3:** "ScyllaDB vector search is fast"

The **inverted index** built by the analyzer (`standard`: lowercase → tokenize → stop-words):

```
 Term           Document Postings (doc_id : [positions])     Doc Freq
─────────────── ──────────────────────────────────────────── ────────
 "scylladb"     { 1: [0],  3: [0] }                           2
 "scales"       { 1: [1, 4],  2: [2] }                        2
 "reads"        { 1: [2] }                                    1
 "writes"       { 1: [5] }                                    1
 "vector"       { 2: [0],  3: [1] }                           2
 "search"       { 2: [1, 4],  3: [2] }                        2
 "embeddings"   { 2: [5] }                                    1
 "fast"         { 3: [4] }                                    1
```

---

# How Hybrid Search Works

```
                            ┌──────────────────────┐
                            │    User Query        │
                            │  "database scaling"  │
                            └─────────┬────────────┘
                                      │
                       ┌──────────────┴───────────────┐
                       ▼                              ▼
          ┌────────────────────┐          ┌────────────────────┐
          │   Full-Text Search │          │   Vector Search    │
          │   (BM25 scoring)   │          │   (ANN similarity) │
          └────────┬───────────┘          └────────┬───────────┘
                   │                               │
          ┌────────▼───────────┐          ┌────────▼────────────┐
          │  Inverted Index    │          │  Vector Index       │
          │  tokenize → match  │          │  embed → ANN lookup │
          │  → BM25 rank       │          │  → cosine rank      │
          └────────┬───────────┘          └────────┬────────────┘
                   │                               │
          Ranked list by                  Ranked list by
          text relevance                  semantic similarity
                   │                               │
                   └──────────┬────────────────────┘
                              ▼
                   ┌─────────────────────┐
                   │  Score Fusion (RRF) │
                   │  1/(k + rank_text)  │
                   │ + 1/(k + rank_vec)  │
                   └──────────┬──────────┘
                              ▼
                   ┌──────────────────────┐
                   │ Final Merged Results │
                   │ best of both worlds  │
                   └──────────────────────┘
```

---

# Scoring & Fusion

<div class="columns">
<div class="col">

### BM25 (Best Matching 25)

Probabilistic ranking extending TF-IDF:
- **Term frequency saturation** — repeated terms yield diminishing returns
- **Document length normalization** — longer docs correctly penalized
- Parameters: `k₁=1.2`, `b=0.75`
- Same function as Lucene, Elasticsearch, Tantivy

</div>
<div class="col">

### Reciprocal Rank Fusion (RRF)

Rank-based fusion for combining multiple retrieval systems:

```
RRF(d) = Σ  1 / (k + rank_r(d))
```

- No score normalization needed
- Default `k=60` smoothing constant
- Simple, parameter-light, well-validated
- Preferred default for hybrid search

Also supported: **Weighted Linear Combination** (user-defined vector/text weights)

</div>
</div>

---

# Full-Text Search Capabilities

<div class="columns">
<div class="col">

### Query Types

| Capability | Example | Description |
|------------|---------|-------------|
| **Phrase query** | `"out of memory"` | Matches exact word sequences in order |
| **Boolean query** | `error AND timeout NOT warning` | Combine terms with AND, OR, NOT operators |
| **Fuzzy matching** | `databse~1` | Typo-tolerant — matches within edit distance N |
| **Wildcard query** | `scyll*` | Prefix/trailing wildcard expansion |

</div>
<div class="col">

### Index & Result Features

| Capability | Description |
|------------|-------------|
| **Multi-column index** | One index over multiple `text` columns |
| **Field boosting** | Per-column weights (e.g., title: 2.0) |
| **Faceted search** | Term-frequency buckets per category |
| **Highlighting** | Matched-term snippets for UI display |

</div>
</div>

> All query types use the same **analyzer pipeline** (tokenizer → lowercase → stemming → stop-words) at both index-time and query-time, ensuring consistent matching.

---

# Core Design: Index-Node Architecture

FTS runs **colocated** with Vector Search on existing index nodes — same deployment, scaling, and monitoring model.

```
┌──────────────┐         CQL          ┌──────────────────────┐
│  Application │ ◄──────────────────► │   ScyllaDB Node      │
└──────────────┘                      │  (coordinator)       │
                                      └───────┬──────────────┘
                                              │ internal protocol
                                      ┌───────▼──────────────────┐
                                      │  Vector Store Index Node │
                                      │  ┌───────────────────┐   │
                                      │  │ Vector Index (ANN)│   │
                                      │  ├───────────────────┤   │
                                      │  │ Full-Text Index   │   │
                                      │  │ (BM25 inverted)   │   │
                                      │  └───────────────────┘   │
                                      └──────────────────────────┘
```

**Data path:** Client → ScyllaDB (CQL) → Coordinator → Index Node → FTS/Vector query → Results merged → Coordinator → Client

---

# Colocation Model: Colocated vs. Dedicated Nodes

| Aspect | Colocated (M1–M3) ✅ | Dedicated FTS Nodes (Future) |
|--------|----------------------|------------------------------|
| ✅ **Operational simplicity** | Single process, reuses VS provisioning/HA/scaling | Separate fleet, independent lifecycle |
| ✅ **Hybrid query latency** | Both indexes local — no cross-node hops | Fan-out to two node types, merge results |
| ✅ **Resource efficiency** | Shared CDC consumer, single process overhead | Duplicate CDC reads, extra process overhead |
| ⚠️ **Resource contention** | FTS competes with VS for CPU/memory (mitigated by thread allocation) | Full isolation |
| ⚠️ **Independent scaling** | Scale together | Scale independently |
| ⚠️ **Failure blast radius** | FTS crash takes down VS on same node | Isolated failure domains |

**Decision:** Colocated for M1–M3. Dedicated nodes scoped in follow-up PRD after M2 adoption data.

---

# Why Colocated First?

1. **Faster time to market** — no new node type, discovery, routing logic
2. **Hybrid query performance** — the key differentiator benefits from colocation (no cross-node coordination)
3. **Operational simplicity** — enabling FTS = config change (`VECTOR_STORE_FTS_THREADS > 0`), not new fleet

### When Dedicated Nodes Become Necessary

- FTS workloads interfere with Vector Search SLOs
- Divergent scaling requirements (large text corpus, small vector set)
- Disk-based storage (M4) shifts bottleneck to disk I/O → different hardware profiles

---

<!-- _class: lead -->

# Alternatives Considered

---

# Alternative: Storage-Attached Indexing (SAI)

SAI (used by Cassandra 5.0+, Astra DB) attaches index structures directly to SSTables.

### Why Index-Node Architecture Was Chosen Over SAI

| Concern | Index-Node (Chosen) | SAI |
|---------|---------------------|-----|
| **Fan-out** | Single index node serves query | Scatter-gather across all tablets |
| **Existing investment** | Reuses CDC, routing, HA, monitoring | New storage-engine integration |
| **Search capabilities** | Full BM25, phrase, boolean, analyzers (Rust) | Filtering engine — not a search engine |
| **Risk to core** | Zero storage engine changes | Modifies critical paths |
| **BM25 + tablets** | Full replica → accurate corpus-wide stats | Local stats only → cross-node IDF needed |
| **Time to market** | Incremental on existing infra | Multi-quarter effort |

---

# Alternative: OpenSearch Compatibility

| Layer | Description | Decision |
|-------|-------------|----------|
| **A) ES vs. OpenSearch** | ES and OpenSearch diverged since 2021 fork | Target OpenSearch if ever pursued |
| **B) Driver compatibility** | OpenSearch client libs per language | **Not planned** |
| **C) REST API compatibility** | JSON REST API ≠ CQL, would need new translation layer + port | **Not planned** |
| **D) Query parser syntax** | OpenSearch → Lucene internally (transparent) | N/A |
| **E) Scoring engine** | BM25, tokenization, hybrid fusion | **Addressed natively** via CQL |

### Rationale

- Same challenge as Alternator (DynamoDB compat): large investment, multiple drivers, new protocol, ongoing maintenance
- Alternator experience: limited adoption despite compat API
- Users adapt queries to CQL; underlying search logic (BM25, boolean, hybrid) is functionally equivalent

---

<!-- _class: lead -->

# CQL API Design

---

# Index Creation

```sql
-- Basic: default standard analyzer
CREATE CUSTOM INDEX idx_comment_fts
ON myapp.comments (comment)
USING 'fulltext_index';

-- With options: English analyzer
CREATE CUSTOM INDEX idx_en_comment_fts
ON myapp.en_comments (comment)
USING 'fulltext_index'
WITH OPTIONS = {
  'analyzer': 'english',
  'similarity_function': 'BM25'
};

-- Multi-column with field boosting (Milestone 3)
CREATE CUSTOM INDEX idx_en_article_fts
ON myapp.en_articles ((title, body))
USING 'fulltext_index'
WITH OPTIONS = {
  'analyzer': 'english',
  'field_boosts': '{"title": 2.0, "body": 1.0}'
};
```

Follows `CREATE CUSTOM INDEX ... USING` pattern from Vector Search. Drop via `DROP INDEX`.

---

# Index Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `analyzer` | text | `'standard'` | Built-in analyzer: `standard`, `english`, `german`, `french`, `spanish`, `italian`, `portuguese`, `russian`, `chinese`, `japanese`, `korean`, `simple`, `whitespace` |
| `custom_analyzer` | text (JSON) | — | Custom tokenizer + filters. Overrides `analyzer`. |
| `similarity_function` | text | `'BM25'` | Scoring function. Only BM25 for now. |
| `field_boosts` | text (JSON) | — | Per-column boost factors for multi-column indexes. |
| `positions` | boolean | `true` | Store term positions (required for phrase queries). |

Supported column types: `text`, `varchar`, `ascii`. No collections, UDTs, or non-text types.

---

# Query: MATCH()

```sql
-- Basic keyword search (implicit OR between terms)
SELECT id, commenter, comment
FROM myapp.comments
WHERE MATCH(comment, 'vector search scalability')
LIMIT 10;

-- Boolean query
SELECT id, title, body
FROM myapp.en_articles
WHERE MATCH((title, body), 'ScyllaDB AND "full text search" NOT deprecated')
LIMIT 20;

-- With relevance score (Milestone 2)
SELECT id, title, FTS_SCORE() AS score
FROM myapp.en_articles
WHERE MATCH(title, 'database performance')
LIMIT 10;
```

> **`LIMIT` is required** on all FTS queries. Server rejects MATCH() without explicit LIMIT.

---

# Query Language Syntax

| Syntax | Example | Description | Milestone |
|--------|---------|-------------|-----------|
| `term` | `'database'` | Single term match | M1 |
| `term1 AND term2` | `'error AND timeout'` | Both terms must appear | M1 |
| `term1 OR term2` | `'scylla OR cassandra'` | Either term | M1 |
| `NOT term` | `'error NOT warning'` | Exclude term | M1 |
| `"phrase"` | `'"out of memory"'` | Exact phrase match | M1 |
| `(group)` | `'(error OR fault) AND critical'` | Grouping | M1 |
| `term~N` | `'databse~1'` | Fuzzy match (edit distance N) | M3 |
| `term*` | `'scyll*'` | Prefix/wildcard (trailing only) | M3 |

Inspired by Lucene syntax but **not** full Lucene compatibility. No field-scoped queries, regex, proximity, or boosting within query strings.

---

# Hybrid Search API (Milestone 2)

```sql
-- Hybrid: RRF fusion of vector + FTS
SELECT id, title, body
FROM myapp.en_articles
WHERE MATCH(title, 'full text search')
ORDER BY article_vector ANN OF [0.12, 0.34, ..., 0.05]
LIMIT 10
USING FUSION = { 'strategy': 'RRF', 'k': 60 };

-- Weighted linear combination
SELECT id, title, body
FROM myapp.en_articles
WHERE MATCH(title, 'full text search')
ORDER BY article_vector ANN OF [0.12, 0.34, ..., 0.05]
LIMIT 10
USING FUSION = { 'strategy': 'WEIGHTED', 'vector_weight': 0.7, 'text_weight': 0.3 };

-- FTS as boolean filter only (no fusion) — MATCH filters, ANN ranks
SELECT id, title, body
FROM myapp.en_articles
WHERE MATCH(body, 'machine learning')
ORDER BY article_vector ANN OF [0.12, 0.34, ..., 0.05]
LIMIT 10;
```

---

# Hybrid Query Semantics

| Mode | Syntax | Behavior |
|------|--------|----------|
| **Fused ranking** | `MATCH() + ANN OF + USING FUSION` | Both scores contribute; ranked by fused score |
| **Boolean filter** | `MATCH() + ANN OF` (no FUSION) | MATCH filters; ranked by ANN only |
| **Post-filter** (M2) | FTS applied after ANN retrieval | Default for M2 |
| **Pre-filter** (M4) | FTS narrows candidates before ANN | Deferred — needs perf validation |

---

# Fusion Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `strategy` | `'RRF'` | `'RRF'` or `'WEIGHTED'` |
| `k` | `60` | RRF smoothing constant |
| `vector_weight` / `text_weight` | `0.5` / `0.5` | Weights for WEIGHTED strategy |

> **RRF** is the recommended default — no score normalization needed, parameter-light, well-validated across retrieval benchmarks.

---

# Competitive CQL Comparison

| Feature | ScyllaDB FTS | Cassandra (SASI) | DSE Search | Astra DB (SAI) |
|---------|-------------|------------------|------------|----------------|
| **Create syntax** | `USING 'fulltext_index'` | `USING 'SASIIndex'` | `CREATE SEARCH INDEX` | `USING 'StorageAttachedIndex'` |
| **Query** | `MATCH(col, 'query')` | `col LIKE '%term%'` | Solr JSON/CQL | `col : 'term'` |
| **BM25** | Native | ✗ | Via Solr | Preview (Data API) |
| **Hybrid** | Native CQL | ✗ | ✗ | Preview (Data API, not CQL) |
| **Phrase queries** | ✓ | Limited | ✓ (Solr) | ✗ |
| **Language analyzers** | 17+ + CJK | ✗ | ✓ (Solr) | 30+ (Lucene) |
| **Boolean operators** | AND, OR, NOT | ✗ | ✓ (Solr) | Limited |

> ScyllaDB is the first to offer **native CQL hybrid search** (vector + FTS + fusion).

---

<!-- _class: lead -->

# Milestones & Roadmap

---

# Milestone 1 — Basic Full-Text Search (2026.3)

*Highest value, lowest effort. Unblocks the majority of search use cases.*

| # | Feature |
|---|---------|
| 1.1 | Single-column full-text index (`text`, `varchar`, `ascii`) |
| 1.2 | Default `standard` analyzer (Unicode tokenizer, lowercase, stop-words) |
| 1.3 | BM25-scored queries (ranked results, score not yet exposed) |
| 1.4 | Boolean operators: AND, OR, NOT |
| 1.5 | Phrase queries (`"out of memory"`) |
| 1.6 | Incremental indexing (near real-time, < 3s target) |
| 1.7 | Index lifecycle: CREATE, DROP, status tracking |
| 1.8 | Prometheus/Grafana monitoring |
| 1.9 | DESCRIBE INDEX for introspection |

---

# Milestone 2 — Hybrid Search (2026.4)

*Highest-impact differentiator. Immediate value for existing Vector Search users.*

| # | Feature |
|---|---------|
| 2.1 | Hybrid query syntax: `ANN OF` + `MATCH()` + `USING FUSION` |
| 2.2 | Reciprocal Rank Fusion (RRF) — default strategy |
| 2.3 | Weighted linear combination — optional strategy |
| 2.4 | Post-filtering with FTS |
| 2.5 | `FTS_SCORE()` function — expose BM25 score as column |
| 2.6 | Pagination via CQL paging state tokens |

---

# Milestone 3 — Configurable Analysis & Multi-column (2027.1)

*Production-grade multi-language deployments.*

| # | Feature |
|---|---------|
| 3.1 | Language-specific analyzers with stemming (17+ languages) |
| 3.2 | Custom analyzer configuration via `WITH OPTIONS` JSON |
| 3.3 | Multi-column full-text index with per-field boosting |
| 3.4 | CJK tokenization (Chinese, Japanese, Korean) |
| 3.5 | Fuzzy matching (configurable edit distance) |
| 3.6 | Wildcard queries (prefix, e.g. `scyll*`) |
| 3.7 | Range queries on indexed numeric/date fields |

---

# Milestone 4 — Advanced Features (Timeline TBD)

*Analytics, operational maturity, disk persistence.*

| # | Feature |
|---|---------|
| 4.1 | Faceted search (term-frequency buckets) |
| 4.2 | Highlighting / snippets (matched text fragments) |
| 4.3 | Aggregations (histogram, range, avg, min, max) |
| 4.4 | Index-only queries (skip base table read) |
| 4.5 | **Disk-based index storage** (NVMe SSD, removes RAM constraint) |
| 4.6 | Auto-suggest / completion (edge-ngram, prefix) |
| 4.7 | Configurable segment merge policies |
| 4.8 | Pre-filtering with FTS (narrow candidates before ANN) |

---

# What's Out of Scope

- External data sources (Kafka, S3)
- Distributed joins / sub-queries
- NLP pipelines (entity extraction, summarization, LLM rewriting)
- Full Lucene query syntax compatibility
- Multi-keyspace / cross-table indexes
- Dedicated FTS node topology (follow-up PRD)
- Alternator (DynamoDB API) integration (separate PRD)
- Geospatial search
- OpenSearch REST API / driver compatibility

---

<!-- _class: lead -->

# Operational Model

---

# Performance Targets

*Baseline: single r8g.xlarge node (4 vCPU, 32 GB), 10M docs × 1 KB, English standard analyzer, in-memory index.*

| Metric | Target | Notes |
|--------|--------|-------|
| **Indexing throughput** | ≥ 10,000 docs/sec per vCPU | 1 KB avg doc size |
| **Query latency (p99)** — simple terms | < 10 ms | Single-node, in-memory |
| **Query latency (p99)** — boolean/phrase | < 50 ms | Single-node, in-memory |
| **Hybrid query latency (p99)** | < 50 ms | FTS + ANN, single coordinator |
| **Indexing lag** | < 3 seconds | Write → searchable via CDC |
| **Index size overhead** | 30–70% of raw text | Depends on analyzer & stored fields |

Validation: **Latte** (CQL load gen) + **SCT** (cluster tests). Regression: FTS index must not degrade non-FTS CQL reads/writes by > 5%.

---

# Consistency & Data Path

**Eventual consistency** — same model as Vector Search. No read-after-write guarantee.

```
1. Client writes to ScyllaDB (CQL INSERT/UPDATE/DELETE)
2. ScyllaDB records mutation in CDC log
3. Index node asynchronously consumes CDC events
4. Events batched (VECTOR_STORE_FTS_INDEXING_BATCH_SIZE = 1000)
5. Committed at intervals (VECTOR_STORE_FTS_INDEXING_COMMIT_INTERVAL_MS = 1000ms)
6. After commit → visible to MATCH() queries
```

- `fts_indexing_lag_seconds` metric tracks delay (< 3s target)
- Each index node replica independently consumes CDC
- Different replicas may be at slightly different positions

---

# High Availability & Failure Modes

**Same HA model as Vector Search.** Per-AZ deployment, coordinator routes to healthy replicas.

| Failure | Impact | Mitigation |
|---------|--------|------------|
| FTS node crash | Index lost (in-memory M1–M3) | Auto-restart; rebuild from base table; other replicas serve traffic |
| Index corruption | Stale/missing results | Drop + recreate index |
| ScyllaDB node failure | CDC stream interrupted | Driver auto-switches to other ScyllaDB node |
| Indexing lag spike | Recent data not searchable | `indexing_lag_seconds` metric + alerting |
| Network partition | Queries fail, indexing stalls | Reconnect with exponential backoff; serve from last-known state |

**Rebuild time (10M docs, 4 vCPU):** ~4 min optimistic (bottleneck: ScyllaDB full-scan throughput). M4 disk persistence eliminates rebuild.

---

# Configuration

All via environment variables on index node (`.env` file, live-reload via `SIGHUP`).

| Variable | Default | Description |
|----------|---------|-------------|
| `VECTOR_STORE_FTS_THREADS` | `0` (disabled) | Threads from `VECTOR_STORE_THREADS` pool dedicated to FTS |
| `VECTOR_STORE_FTS_MAX_INDEX_SIZE_MB` | 20% of system RAM | Max RAM for FTS index pool |
| `VECTOR_STORE_FTS_INDEXING_BATCH_SIZE` | `1000` | Mutations batched before commit |
| `VECTOR_STORE_FTS_INDEXING_COMMIT_INTERVAL_MS` | `1000` | Max ms between commits |
| `VECTOR_STORE_FTS_MAX_CLAUSES_PER_QUERY` | `1024` | Max boolean clauses per query |
| `VECTOR_STORE_FTS_MAX_WILDCARD_EXPANSIONS` | `65536` | Max terms per wildcard expansion |

**Resource protection:** CQL query timeout applies. FTS concurrency bounded by same mechanism as Vector Search. Wildcard expansion capped.

---

# Monitoring (Index Node)

| Metric | Type | Purpose |
|--------|------|---------|
| `fts_query_latency_seconds` | histogram | Per-index query latency |
| `fts_query_count_total` | counter | Total FTS queries |
| `fts_index_doc_count` | gauge | Docs indexed per index |
| `fts_index_size_bytes` | gauge | Memory consumption per index |
| `fts_indexing_lag_seconds` | gauge | Write → searchable delay |
| `fts_indexing_rate` | gauge | Docs indexed per second |
| `fts_segment_count` | gauge | Segments per index |
| `fts_merge_duration_seconds` | histogram | Segment merge times |
| `hybrid_query_latency_seconds` | histogram | End-to-end hybrid query latency |

Grafana dashboards shipped alongside existing Vector Search dashboards.

---

# Index Storage Model

<div class="columns">
<div class="col">

### M1–M3: In-Memory

- Full index in process heap (no disk)
- **Not durable** — rebuilt from base table on restart
- Sizing: FTS + vector index must fit in RAM
- Commits are memory-only (no disk I/O)
- Segment merges run in memory
- Cap via `FTS_MAX_INDEX_SIZE_MB`

</div>
<div class="col">

### M4: Disk-Based (Optional)

- Persist segments to local NVMe SSD
- OS page cache for read acceleration
- Removes RAM-size constraint
- Survives restarts (no rebuild)
- Graceful degradation when working set > cache
- Compressed storage (LZ4, Zstd)
- **Opt-in** — higher latency, lower TCO

</div>
</div>

---

# Affected Components

| Component | Impact |
|-----------|--------|
| **Vector Store Index Node** | Core FTS implementation — indexing, query processing, segment management |
| **ScyllaDB Core** | Route FTS and hybrid queries to index nodes |
| **Drivers** | No code changes — pass CQL strings through. Documentation updates only. |
| **Scylla Manager** | Back up FTS index metadata; recreate on restore |
| **Cloud** | API endpoints for FTS node lifecycle; UI for enable/disable |
| **Operator** | Not initially (same as Vector Search) |

---

# Security

- **AuthN/AuthZ:** Same CQL RBAC. No SELECT permission → no FTS queries.
- **Data at rest:** Index files covered by Encryption at Rest (when disk storage implemented in M4).
- **Data in transit:** Same TLS-protected internal protocol as Vector Search.
- **Audit:** FTS queries in CQL audit logs.
- **No new sensitive data:** Indexes derived from existing table data.

---

<!-- _class: lead -->

# Key Decisions Summary

---

# Key Technical Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| **Architecture** | Index-node (not SAI) | Leverages existing infra, zero risk to ScyllaDB core, correct BM25 stats |
| **Deployment** | Colocated with Vector Search (M1–M3) | Faster TTM, hybrid perf, operational simplicity |
| **Scoring** | BM25 | Industry standard, same as Lucene/ES/OpenSearch |
| **Default fusion** | RRF | Simple, parameter-light, no score normalization needed |
| **OpenSearch compat** | Not pursued | Alternator experience; CQL-native approach preferred |
| **Index storage** | In-memory (M1–M3), disk optional (M4) | Lowest latency first; disk removes RAM constraint later |
| **Consistency** | Eventual (< 3s target) | Same as Vector Search; R-A-W would add unacceptable latency |

---

<!-- _class: lead -->

# Open Questions & Discussion

---

# Discussion Topics

1. **Thread allocation model** — Is `VECTOR_STORE_FTS_THREADS` as a subset of `VECTOR_STORE_THREADS` the right granularity, or do we need more flexible resource isolation?

2. **M1 timeline risk** — Any concerns with 2026.3 target for basic FTS GA?

3. **Memory sizing guidance** — How should we advise customers on FTS memory budgeting alongside existing vector indexes?

4. **Hybrid search adoption** — Which existing customers are immediate candidates for M2?

---

<!-- _class: lead -->

# Appendix

---

# Appendix A: User Stories

**Application Developers:**
- Run keyword search via CQL with BM25 ranking → no external search engine
- Combine keyword + vector similarity in single query → better RAG relevance
- Use phrase, boolean, fuzzy queries → rich search experiences
- Select language/tokenizer → multi-language support including CJK

**DBAs / Platform Engineers:**
- Provision/scale FTS using same Cloud API as Vector Search → minimal new ops knowledge
- Monitor via existing Prometheus/Grafana stack → standard SLO alerting

**Driver Developers:**
- FTS CQL extension is minimal and backward-compatible → existing drivers work unchanged

---

# Appendix B: References

| Document | Location |
|----------|----------|
| Full PRD | [Confluence](https://scylladb.atlassian.net/wiki/spaces/RND/pages/263585939/Full+Text+Search+Requirement+Document) |
| Technical Design Document | *To be created* |
| Vector Search Documentation | [cloud.docs.scylladb.com](https://cloud.docs.scylladb.com/stable/vector-search/index.html) |
| Tantivy | [github.com/quickwit-oss/tantivy](https://github.com/quickwit-oss/tantivy) |
| Integration Tests | `crates/validator-vector-store/src/` |
| Benchmarking | Latte workload scripts + SCT test definitions |

---

<!-- _class: lead -->

# Thank You

### Next Steps

1. Review open questions → align on decisions
2. Kick off Technical Design Document
3. Begin M1 implementation targeting 2026.3

---

<!--
  Fragment animation — reveals slide elements one-by-one on click/key.
  Export with:  marp --html full-text-search-review-deck.md
  Slides with class "lead" are excluded (shown all at once).

  Behavior:
    1) Slide loads → content is hidden
    2) Click / → / Space / PageDown → reveal next element with fade-in
    3) All elements visible → next click/key advances to next slide
    4) Going back (← / PageUp) → previous slide shows ALL elements immediately
-->
<script>
(function () {
  function init() {
    /* ---- Configuration ---- */
    const FRAG_SEL = 'h2, h3, p, ul > li, ol > li, blockquote, table tbody > tr, pre, marp-pre';

    /* ---- Locate slides ----
       Bespoke wraps each slide in: <svg class="bespoke-marp-slide"> > <foreignObject> > <section>
       The active slide's SVG gets class "bespoke-marp-active". */
    const allSVGs = Array.from(document.querySelectorAll('svg[data-marpit-svg]'));
    if (allSVGs.length === 0) return; /* Not a bespoke presentation */

    function sectionOf(svg) {
      return svg.querySelector('foreignObject > section');
    }

    /* ---- Fragment helpers ---- */

    function getFragments(section) {
      return Array.from(section.querySelectorAll(FRAG_SEL)).filter(function (el) {
        /* Skip nested list items — only reveal top-level items */
        if (el.tagName === 'LI' && el.parentElement.closest('li')) return false;
        /* Skip paragraphs inside already-animated containers */
        if (el.tagName === 'P' && el.closest('li, blockquote')) return false;
        /* Skip headings/paragraphs inside already-animated containers */
        if ((el.tagName === 'H2' || el.tagName === 'H3') && el.closest('li, blockquote')) return false;
        return true;
      });
    }

    function isLead(section) {
      if (!section.className) return false;
      return section.className.indexOf('lead') !== -1 || section.className.indexOf('no-anim') !== -1;
    }

    function hideTableChrome(section) {
      Array.from(section.querySelectorAll('table')).forEach(function (tbl) {
        tbl.style.transition = 'none';
        tbl.style.boxShadow = 'none';
        tbl.style.border = 'none';
        tbl.style.borderRadius = '0';
        tbl.dataset.chromHidden = '1';
      });
      Array.from(section.querySelectorAll('thead')).forEach(function (th) {
        th.style.transition = 'none';
        th.style.opacity = '0';
        th.style.height = '0';
        th.style.overflow = 'hidden';
        th.style.lineHeight = '0';
        th.style.padding = '0';
      });
    }

    function showTableChrome(section, animate) {
      Array.from(section.querySelectorAll('table')).forEach(function (tbl) {
        tbl.style.transition = animate ? 'box-shadow 0.35s ease' : 'none';
        tbl.style.boxShadow = '';
        tbl.style.border = '';
        tbl.style.borderRadius = '';
        delete tbl.dataset.chromHidden;
      });
      Array.from(section.querySelectorAll('thead')).forEach(function (th) {
        th.style.transition = animate ? 'opacity 0.35s ease' : 'none';
        th.style.opacity = '1';
        th.style.height = '';
        th.style.overflow = '';
        th.style.lineHeight = '';
        th.style.padding = '';
      });
    }

    function revealTableChromeFor(tr) {
      var table = tr.closest('table');
      if (!table || !table.dataset.chromHidden) return;
      /* Only reveal when the first tbody tr of this table is being shown */
      var firstRow = table.querySelector('tbody > tr');
      if (firstRow !== tr) return;
      table.style.transition = 'box-shadow 0.35s ease';
      table.style.boxShadow = '';
      table.style.border = '';
      table.style.borderRadius = '';
      delete table.dataset.chromHidden;
      var thead = table.querySelector('thead');
      if (thead) {
        thead.style.transition = 'opacity 0.35s ease';
        thead.style.opacity = '1';
        thead.style.height = '';
        thead.style.overflow = '';
        thead.style.lineHeight = '';
        thead.style.padding = '';
      }
    }

    function hideAll(section) {
      if (!section || isLead(section)) return;
      var frags = getFragments(section);
      if (frags.length === 0) return;
      frags.forEach(function (el) {
        el.style.transition = 'none';
        el.style.opacity = '0';
        el.style.transform = 'translateY(14px)';
      });
      hideTableChrome(section);
      section.dataset.fragIdx = '0';
      section.dataset.fragCount = String(frags.length);
    }

    function showAll(section) {
      if (!section || isLead(section)) return;
      var frags = getFragments(section);
      frags.forEach(function (el) {
        el.style.transition = 'none';
        el.style.opacity = '1';
        el.style.transform = 'translateY(0)';
      });
      showTableChrome(section, false);
      section.dataset.fragIdx = String(frags.length);
      section.dataset.fragCount = String(frags.length);
    }

    function revealNext(section) {
      if (!section || isLead(section)) return false;
      if (section.dataset.fragIdx === undefined) return false;
      var idx = parseInt(section.dataset.fragIdx, 10);
      var frags = getFragments(section);
      if (idx < frags.length) {
        var el = frags[idx];
        el.style.transition = 'opacity 0.35s ease, transform 0.35s ease';
        el.style.opacity = '1';
        el.style.transform = 'translateY(0)';
        if (el.tagName === 'TR') revealTableChromeFor(el);
        section.dataset.fragIdx = String(idx + 1);
        return true;
      }
      return false; /* all shown */
    }

    function hasUnrevealedFragments(section) {
      if (!section || isLead(section)) return false;
      if (section.dataset.fragIdx === undefined) return false;
      return parseInt(section.dataset.fragIdx, 10) < parseInt(section.dataset.fragCount, 10);
    }

    function hasRevealedFragments(section) {
      if (!section || isLead(section)) return false;
      if (section.dataset.fragIdx === undefined) return false;
      return parseInt(section.dataset.fragIdx, 10) > 0;
    }

    function hideLast(section) {
      if (!section || isLead(section)) return false;
      if (section.dataset.fragIdx === undefined) return false;
      var idx = parseInt(section.dataset.fragIdx, 10);
      if (idx <= 0) return false;
      var frags = getFragments(section);
      var el = frags[idx - 1];
      el.style.transition = 'opacity 0.25s ease, transform 0.25s ease';
      el.style.opacity = '0';
      el.style.transform = 'translateY(14px)';
      /* If hiding first table row, also hide table chrome */
      if (el.tagName === 'TR') {
        var tbody = el.closest('tbody');
        if (tbody && tbody.querySelector('tr') === el) {
          hideTableChrome(section);
        }
      }
      section.dataset.fragIdx = String(idx - 1);
      return true;
    }

    /* ---- Active-slide detection ----
       Bespoke adds "bespoke-marp-active" to the <svg>, not the <section>. */

    function activeSVG() {
      return document.querySelector('svg.bespoke-marp-active');
    }

    function activeSection() {
      var svg = activeSVG();
      return svg ? sectionOf(svg) : null;
    }

    function slideIndex(svg) {
      return allSVGs.indexOf(svg);
    }

    /* ---- Initial setup: hide fragments on ALL slides ---- */
    allSVGs.forEach(function (svg) {
      var sec = sectionOf(svg);
      if (sec) hideAll(sec);
    });

    /* Show first slide's first state (fragments hidden, ready to reveal) */
    var lastIdx = slideIndex(activeSVG());

    /* ---- Intercept forward navigation (capture phase, fires before bespoke) ---- */

    document.addEventListener('keydown', function (e) {
      var isForward = (e.code === 'ArrowRight' || e.code === 'Space' ||
                       e.code === 'Enter' || e.code === 'PageDown');
      var isBackward = (e.code === 'ArrowLeft' || e.code === 'PageUp');

      if (isForward) {
        var sec = activeSection();
        if (hasUnrevealedFragments(sec)) {
          revealNext(sec);
          e.stopImmediatePropagation();
          e.preventDefault();
          return;
        }
        /* All fragments shown — let bespoke advance to next slide */
      }

      if (isBackward) {
        var sec = activeSection();
        if (hasRevealedFragments(sec)) {
          hideLast(sec);
          e.stopImmediatePropagation();
          e.preventDefault();
          return;
        }
        /* All fragments hidden — let bespoke navigate to previous slide */
      }
    }, true);

    document.addEventListener('click', function (e) {
      /* Ignore clicks on bespoke OSC (on-screen controls) */
      if (e.target && e.target.closest && e.target.closest('.bespoke-marp-osc')) return;

      var sec = activeSection();
      if (hasUnrevealedFragments(sec)) {
        revealNext(sec);
        e.stopImmediatePropagation();
        e.preventDefault();
      }
      /* All fragments shown — let bespoke handle click to advance */
    }, true);

    /* ---- Observe slide changes via MutationObserver ----
       When bespoke activates a new slide, it add/removes "bespoke-marp-active"
       on the SVG elements. We watch for that class change. */

    var observer = new MutationObserver(function () {
      var svg = activeSVG();
      if (!svg) return;
      var newIdx = slideIndex(svg);
      if (newIdx === lastIdx) return; /* same slide, no change */

      var sec = sectionOf(svg);
      if (!sec) return;

      if (newIdx > lastIdx) {
        /* Navigated FORWARD → hide fragments, ready for click-reveal */
        hideAll(sec);
      } else {
        /* Navigated BACKWARD → show all fragments immediately (req #4) */
        showAll(sec);
      }
      lastIdx = newIdx;
    });

    observer.observe(document.body, {
      attributes: true,
      subtree: true,
      attributeFilter: ['class']
    });
  }

  /* Wait for DOM + bespoke initialization */
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', function () { setTimeout(init, 200); });
  } else {
    setTimeout(init, 200);
  }
})();
</script>

