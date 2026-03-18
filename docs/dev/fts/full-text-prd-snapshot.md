<!-- Snapshot of the Full Text Search PRD taken on 2026-03-18.
     Kept in-repo so that AI tools can reference the PRD context
     without requiring an Atlassian/Confluence MCP server. -->

# Full Text Search Business Justification

## The Problem

Modern applications increasingly need to combine multiple search paradigms - keyword-based retrieval, semantic similarity, and filtering - within a single database query. Today, ScyllaDB users who require full-text search (FTS) must rely on external search engines such as Elasticsearch or OpenSearch, introducing:

- **Operational complexity**: Maintaining a separate search cluster, data synchronization pipelines, and dual-write logic.
- **Data consistency issues**: Changes in ScyllaDB are not immediately reflected in the external search engine, causing stale results.
- **Increased latency**: Network hops between ScyllaDB and the search engine add latency to every query that involves text search.
- **Higher infrastructure cost**: Running and scaling a dedicated search cluster alongside ScyllaDB doubles storage and compute costs for searchable data.
- **Inability to do hybrid search**: Combining vector similarity with keyword relevance requires application-level orchestration across two separate systems, making ranking and fusion ad-hoc and brittle.

With Vector Search already natively available in ScyllaDB, the absence of full-text search is the remaining gap that forces users to adopt external systems even for straightforward keyword search use cases.

## The Solution

Integrate a native Full Text Search capability into ScyllaDB's existing index-node-based architecture - the same architecture that powers Vector Search today. Full Text Search will:

- Be deployed alongside Vector Search nodes (or independently), reusing the same operational model for provisioning, scaling, and monitoring. Details will be designed in Technical Design Document.
- Expose a CQL-native interface using `CREATE CUSTOM INDEX ... USING 'fulltext_index'` - consistent with the Vector Search index pattern (`USING 'vector_index'`).
- Support BM25-based relevance scoring, configurable tokenization and analysis, phrase queries, boolean queries, fuzzy matching, wildcard queries, multi-column indexes with field boosting, and range filters.
- Enable **Hybrid Search** - combining full-text relevance with vector similarity in a single CQL query - allowing users to build Retrieval-Augmented Generation (RAG) pipelines, semantic product search, and conversational AI applications without leaving ScyllaDB.
- Provide incremental, real-time (below 3 seconds) indexing so that writes to ScyllaDB are immediately searchable.

### Benefits to ScyllaDB

| Benefit | Description |
|---|---|
| **Increased Vector Search value** | FTS and Hybrid Search extend the existing Vector Search offering, making it more compelling and increasing adoption without requiring a new SKU. |
| **Competitive parity** | Matches and exceeds the search capabilities of DataStax Enterprise (DSE Search) and Astra DB. |
| **Reduced churn risk** | Users no longer need to maintain external Elasticsearch/OpenSearch clusters, deepening lock-in. |
| **AI/ML platform play** | Native hybrid search positions ScyllaDB as a unified data platform for AI-powered applications. Semantic (vector) search alone struggles with rare or precise concepts — proper nouns, addresses, product SKUs, error codes, and domain-specific terminology often lack meaningful embedding representations. Full-text search excels at exact and keyword-level matching for these cases. This is why production AI applications (RAG pipelines, conversational agents, product search) combine both approaches via hybrid search to maximize recall and precision. |

## User Stories

### Scylla Customers (Application Developers)

- **As an application developer**, I want to run keyword search queries against text columns in ScyllaDB using CQL with BM25 relevance ranking, so that I do not need to deploy and synchronize a separate search engine.
- **As an application developer**, I want to combine keyword search with vector similarity search in a single query (hybrid search), so that I can build RAG and semantic search applications with better relevance without complex client-side orchestration.
- **As an application developer**, I want to use phrase queries (e.g., `"out of memory"`), boolean queries (e.g., `error AND timeout`), and fuzzy matching (e.g., `databse~1` matching `database` where `~1` denotes number of differences), so that I can build rich search experiences.
- **As an application developer**, I want to select the language and tokenizer for my text indexes, so that search works correctly for my users' languages including CJK (Chinese, Japanese, and Korean).

### Scylla Admins (DBAs / Platform Engineers)

- **As a database administrator**, I want to provision and scale Full Text Search nodes using the same Cloud API and operations model as Vector Search, so that there is minimal new operational knowledge required.
- **As a database administrator**, I want to monitor FTS index health, query latency, and indexing lag through the existing monitoring stack (Prometheus/Grafana), so that I can set SLOs and alert on degradation.

### Scylla Developers (Driver / Ecosystem Contributors)

- **As a driver developer**, I want the FTS CQL extension to be minimal and backward-compatible, so that existing drivers work with FTS queries without modification.

---

# Software Requirements Specification

## Full Text Search General Technical Overview

The following technical assumptions underpin this PRD and will be validated during the Technical Design phase:

- **Index node architecture**: Full Text Search (FTS) in ScyllaDB will leverage the index-node-based architecture introduced with Vector Search. FTS indexes will run as processes colocated with the existing vector-store index node, sharing the same lifecycle (deployment, scaling, monitoring, HA).

- **Search engine library**: [Tantivy](https://github.com/quickwit-oss/tantivy) is the most promising full-text search engine library to be used at indexing nodes. It is inspired by Apache Lucene — the indexing engine behind Elasticsearch and OpenSearch — written in Rust, and provides the core capabilities assumed throughout this PRD (BM25 scoring, configurable tokenizers with stemming for 17+ languages, phrase/boolean/fuzzy queries, incremental indexing, and faceted search). Tantivy's broad feature coverage and Lucene-inspired query syntax make it a useful reference for identifying the search features ScyllaDB should support. However, the final library choice and detailed engine-level design decisions require a deeper Technical Design Document that evaluates Tantivy against competitive alternatives and validates performance, integration complexity, and long-term maintainability. This PRD intentionally remains at the product-requirements level, incorporating only the technical vision necessary to reason about scope, feasibility, and architecture.

### Core Concepts

| Concept | Description |
|---|---|
| **Full-Text Index** | An inverted index built from one or more `text` columns in a ScyllaDB table. Stores tokenized terms, positional information, and term frequencies for BM25 scoring. |
| **Analyzer** | A pipeline of character filters, tokenizer, and token filters applied to text at index time and query time. Determines how raw text is broken into searchable terms. |
| **[BM25](https://en.wikipedia.org/wiki/Okapi_BM25) Scoring** | The ranking function (same as Apache Lucene) used to order results by relevance. |
| **Segment** | An immutable, self-contained sub-index within a full-text index. New documents are written to a new segment; segments are periodically merged in the background to reduce their count and reclaim space from deleted documents. Each segment contains its own inverted index, term dictionary, and stored fields. This is the same segmented architecture used by Apache Lucene and Tantivy. In the initial release (Milestones 1–3) segments are held entirely in memory; disk-based persistence is added in Milestone 4. |
| **Hybrid Search** | A query mode that combines a full-text relevance score with a vector similarity score using a configurable fusion function (e.g., [Reciprocal Rank Fusion](https://en.wikipedia.org/wiki/Reciprocal_rank_fusion)). |

### Scoring & Fusion

#### BM25 (Best Matching 25)

BM25 is a probabilistic ranking function that scores how relevant a document is to a given query. It extends TF-IDF with two key improvements: **term frequency saturation** (repeated terms yield diminishing returns) and **document length normalization** (longer documents are not unfairly penalized). The formula is:

$$\text{score}(D, Q) = \sum_{i=1}^{n} \text{IDF}(q_i) \cdot \fra (probablyScylla node or new componentervivice)c{f(q_i, D) \cdot (k_1 + 1)}{f(q_i, D) + k_1 \cdot \left(1 - b + b \cdot \frac{|D|}{\text{avgdl}}\right)}$$

Where:
- $f(q_i, D)$ - frequency of term $q_i$ in document $D$
- $|D|$ - document length (in terms); $\text{avgdl}$ - average document length across the corpus
- $k_1$ (default `1.2`) - controls term frequency saturation; higher values make frequency matter more
- $b$ (default `0.75`) - control possibnlblys document length normalization; `0` disables it, `1` applies full normalization
- $\text{IDF}(q_i)$ - inverse document frequency, giving higher weight to rare terms

BM25 is the same scoring function used by Apache Lucene, Elasticsearch, and Tantivy.

#### Reciprocal Rank Fusion (RRF)

RRF is a rank-based fusion strategy for combining results from multiple retrieval systems (e.g., vector search and full-text search) without requiring score normalization. Each result's fused score is computed as:

$$\text{RRF}(d) = \sum_{r \in R} \frac{1}{k + \text{rank}_r(d)}$$

Where:
- $R$ - set of result lists being fused (e.g., vector results + FTS results)
- $\text{rank}_r(d)$ - rank of document $d$ in result list $r$ (1-based)
- $k$ (default `60`) - smoothing constant that reduces the impact of high-ranked outliers

RRF is preferred as the default fusion strategy because it is simple, parameter-light, and performs well without requiring calibration between heterogeneous scoring functions (BM25 scores vs. cosine similarity). It was introduced by Cormack et al. (2009) and is widely used in RAG pipelines.

### Architecture

```
┌──────────────┐         CQL          ┌──────────────────────┐
│  Application │ ◄──────────────────► │   ScyllaDB Node      │
└──────────────┘                      │  (coordinator)       │
                                      └───────┬──────────────┘
                                              │ internal protocol
                                      ┌───────▼──────────────────┐
                                      │  Vector Store Index Node  │
                                      │  ┌───────────────────┐   │
                                      │  │ Vector Index (ANN)│   │
                                      │  ├───────────────────┤   │
                                      │  │ Full-Text Index   │   │
                                      │  │ (BM25 inverted)   │   │
                                      │  └───────────────────┘   │
                                      └──────────────────────────┘
```

FTS indexes are stored and managed per-node in the index node process. The ScyllaDB coordinator routes FTS queries to the appropriate index nodes, aggregates partial results, and returns them to the client - mirroring the Vector Search query path.

### Index Colocation: Colocated vs. Dedicated Nodes

In the initial release (Milestones 1–3), FTS indexes are **colocated** with Vector Search indexes on the same index node process. A dedicated FTS node topology is deferred to a follow-up PRD. The following table compares the two deployment models:

| Aspect | Colocated (same node) | Dedicated FTS nodes |
|---|---|---|
| **Operational simplicity** | Single process to deploy, monitor, and upgrade per node. Reuses existing Vector Search provisioning, HA, and scaling workflows. | Requires a separate fleet of FTS nodes with independent provisioning, monitoring, and upgrade procedures. |
| **Hybrid query latency** | Both vector and FTS indexes are local to the same process. Hybrid queries execute without cross-node communication, minimizing latency. | Hybrid queries require the coordinator (Scylla node or new service) to fan out to two separate node types and merge results, adding network round-trips and tail-latency risk. |
| **Resource efficiency** | FTS and Vector Search indexes share the same CDC consumer, reducing duplicated I/O on the ScyllaDB CDC log. Memory and CPU are shared within a single process, avoiding the overhead of a second OS process per node. | Each node type independently consumes the CDC log, possibly doubling CDC read I/O. Separate processes incur additional memory overhead (OS buffers, runtime, connection pools). |
| **Resource contention** | FTS indexing, segment merges, and queries compete for CPU and memory with Vector Search operations. Thread allocation (`VECTOR_STORE_FTS_THREADS`) mitigates this but does not eliminate it. | Full resource isolation. FTS workloads cannot degrade Vector Search performance, and vice versa. |
| **Independent scaling** | FTS and Vector Search scale together. If one workload requires more capacity, the entire index node fleet must scale. | Each workload scales independently based on its own resource demands. |
| **Memory planning** | Operators must account for both Vector Search index memory and FTS index memory (in-memory segments in Milestones 1–3) when sizing nodes. | Memory budgets are scoped to a single workload per node, simplifying capacity planning. |
| **Failure blast radius** | A crash in FTS code (e.g., segment corruption, OOM during merge) takes down the vector index on the same node. | FTS failures do not affect Vector Search availability, and vice versa. |

**Rationale for colocated deployment in the initial release:**

- **Faster time to market**: Avoids the engineering effort of a new node type, separate discovery and routing logic, and independent lifecycle management.
- **Hybrid query performance**: The primary differentiating feature (Milestone 2) benefits directly from colocation — fusing vector and FTS results within a single process avoids cross-node coordination.
- **Operational simplicity for early adopters**: Users evaluating FTS do not need to provision a separate fleet. Enabling FTS is a configuration change (`VECTOR_STORE_FTS_THREADS > 0`) on existing index nodes. This can be executed on the fly if there are enough free resources.

**When dedicated nodes become necessary:**

- When FTS workloads are large enough to interfere with Vector Search SLOs (or vice versa).
- When FTS and Vector Search have divergent scaling requirements (e.g., a corpus with millions of text documents but only a small number of vector-indexed columns).
- When disk-based FTS index storage (Milestone 4) shifts the bottleneck from memory to disk I/O, requiring different hardware profiles.

The dedicated node topology will be addressed in a follow-up PRD after Milestone 2 adoption data is available.

### Alternative Considered: Storage-Attached Indexing (SAI)

An alternative to the proposed index-node architecture is the **Storage-Attached Indexing (SAI)** model used by Apache Cassandra 5.0+ and DataStax Astra DB. In SAI, index structures are attached directly to SSTables — each SSTable carries its own per-column index segments on disk, and in-memory memtable indexes are maintained for recently written data. Index files are created during memtable flush and merged during compaction, tightly coupling the index lifecycle to the storage engine. 

**Why index-node architecture is chosen over SAI:**

- **Leverages existing investment**: The index-node infrastructure (CDC ingestion, coordinator routing, HA, monitoring) is already built and battle-tested for Vector Search. Adding FTS to this architecture is incremental, whereas SAI would require building new storage-engine integration from scratch.
- **Superior search capabilities**: SAI is designed as a general-purpose filtering engine, not a full-text search engine. Achieving BM25 scoring, phrase queries, boolean operators, configurable analyzers, and hybrid fusion within an SAI framework would require embedding a search-engine library (e.g., Tantivy or Lucene) into ScyllaDB's C++ data path - a significantly more complex and risky integration than running it in a dedicated Rust process.
- **No changes to ScyllaDB core**: The index-node approach adds zero risk to ScyllaDB's data path. The storage engine, compaction, memtable flush, and streaming paths remain untouched. In contrast, SAI integration would modify these critical paths, requiring extensive validation and risking regressions.
- **Independent scaling**: SAI ties index capacity to data-node topology. For workloads with large text corpora but modest data volumes (or vice versa), the index-node model allows independent right-sizing.
- **Tablet-distributed data and BM25 scoring**: ScyllaDB distributes data across tablets that can migrate between nodes dynamically. In an SAI model, index segments would naturally reside inside tablets and migrate with them — no rebuild is required. However, BM25 scoring depends on corpus-wide statistics: inverse document frequency (IDF) requires knowing how many documents across the entire table contain a given term, and document length normalization requires the average document length across the corpus. When index segments are distributed across many tablets on different nodes, each tablet only has local statistics. Computing accurate BM25 scores would require either (a) a coordination step at query time to gather global term statistics from all tablets, adding latency and fan-out, or (b) maintaining global statistics asynchronously, adding complexity and staleness. The index-node architecture avoids this problem — each index node holds a complete replica of the index with accurate corpus-wide statistics, enabling correct BM25 scoring without cross-node coordination at query time.
- **Global query fan-out**: Full-text search queries are inherently global — the `MATCH()` predicate does not include a partition key, so the coordinator cannot restrict the query to a subset of tablets. In an SAI model, every FTS query would need to fan out to all tablets across all data nodes, collect partial ranked results from each tablet, and merge them — a scatter-gather pattern that grows in cost linearly with the number of tablets. For large tables with thousands of tablets spread across many nodes, this fan-out introduces significant coordination overhead and tail-latency risk. The index-node architecture consolidates the entire index on each index node replica, so a query is served by a single index node without any fan-out to data nodes.
- **Faster time to market**: Implementing FTS on the existing index-node architecture avoids the multi-quarter engineering effort of integrating a search engine into ScyllaDB's storage engine.

**When SAI-style integration might be reconsidered:**

- If ScyllaDB's storage engine is extended to support pluggable per-SSTable index components (similar to Cassandra's SAI framework), lightweight indexing features (equality filters, range predicates) could be implemented as storage-attached indexes while full-text search remains on dedicated index nodes.
- If the eventual consistency trade-off of CDC-based ingestion becomes unacceptable for a significant number of use cases, tighter storage-engine integration may be explored to reduce indexing lag.

## Scope

### In Scope

#### Milestone 1 - Basic Full-Text Search (Highest value, lowest effort)

This milestone delivers the core FTS feature that unblocks the majority of search use cases.

| # | Feature | Description |
|---|---|---|
| 1.1 | **Single-column full-text index** | `CREATE CUSTOM INDEX ... USING 'fulltext_index'` on a single `text`, `varchar`, or `ascii` column. Other CQL types (e.g., `frozen<list<text>>`, `map`, `set`) are not supported. |
| 1.2 | **Default analyzer** | A built-in `standard` analyzer (Unicode-aware tokenizer, lowercase filter, stop-word removal) applied automatically. |
| 1.3 | **BM25-scored queries** | `SELECT ... WHERE MATCH(column, 'query terms') LIMIT k` returning rows ordered by BM25 relevance score. In Milestone 1 results are ranked by BM25 but the score is not exposed as a selectable column; `FTS_SCORE()` is introduced in Milestone 2. |
| 1.4 | **Boolean query operators** | Support `AND`, `OR`, `NOT` in query strings (e.g., `'error AND timeout NOT warning'`). |
| 1.5 | **Phrase queries** | Support quoted phrases (e.g., `'"out of memory"'`). |
| 1.6 | **Incremental indexing** | New writes (INSERT, UPDATE, DELETE) are reflected in the FTS index in near real-time. |
| 1.7 | **Index lifecycle** | `CREATE`, `DROP`, and index-build status tracking (reuse existing `system.indexing_status` patterns from Vector Search). |
| 1.8 | **Basic monitoring** | Expose index size, document count, and query latency metrics via Prometheus and Grafana. |
| 1.9 | **Index introspection** | `DESCRIBE INDEX` surfaces FTS index configuration (analyzer, options) so users can inspect existing index definitions. |

**Exit criteria**: A user can create a table with a `text` column, build a full-text index, insert data, run relevance-ranked keyword searches, and inspect index configuration via `DESCRIBE INDEX` - all via CQL.

**Timeline**: To be included in ScyllaDB 2026.3.

#### Milestone 2 - Hybrid Search (Vector + Full-Text)

This milestone delivers the highest-impact differentiating feature: combining semantic and keyword search. Users with existing Vector Search deployments gain immediate value.

| # | Feature | Description |
|---|---|---|
| 2.1 | **Hybrid query syntax** | A single CQL query that combines `ANN OF` (vector similarity) and `MATCH()` (full-text) with a fusion strategy. |
| 2.2 | **Reciprocal Rank Fusion (RRF)** | Default fusion strategy that merges ranked lists from vector and FTS results without requiring score normalization. |
| 2.3 | **Weighted linear combination** | Optional fusion strategy allowing users to set relative weights for vector vs. text relevance. |
| 2.4 | **Post-filtering with FTS** | Apply full-text constraints after vector ANN retrieval for exact keyword enforcement. |
| 2.5 | **Score function** | Expose an `FTS_SCORE()` function that returns the BM25 relevance score as a column in results. |
| 2.6 | **Pagination** | Support paging through FTS and hybrid query results using CQL paging state tokens, consistent with standard CQL paging behavior. |

**Exit criteria**: A user can run a single CQL query that returns results ranked by a combination of semantic similarity and keyword relevance, page through result sets, and retrieve BM25 relevance scores.

**Timeline**: To be included in ScyllaDB 2026.4.

#### Milestone 3 - Configurable Analysis & Multi-column Indexes

This milestone deepens the FTS feature for production-grade, multi-language deployments.

| # | Feature | Description |
|---|---|---|
| 3.1 | **Language-specific analyzers** | Built-in analyzers with stemming for major languages: English, German, French, Spanish, Italian, Portuguese, Russian, and more (17+ Latin-script languages). |
| 3.2 | **Custom analyzer configuration** | Allow specifying tokenizer and token filters in `WITH OPTIONS` (e.g., `'analyzer': '{"tokenizer": "standard", "filters": ["lowercase", "stemmer_en"]}'`). |
| 3.3 | **Multi-column full-text index** | Index across multiple `text` columns in a single index with per-field boosting (assigning higher weight to specific columns so that matches in those columns rank higher in results). |
| 3.4 | **CJK tokenization** | Built-in tokenizers for Chinese, Japanese, and Korean text segmentation. |
| 3.5 | **Fuzzy matching** | Support for typo-tolerant queries with configurable edit distance. |
| 3.6 | **Wildcard queries** | Prefix and wildcard term matching (e.g., `'scylla*'`). |
| 3.7 | **Range queries on indexed fields** | Support for range predicates on numeric and date fields within the full-text index. |

**Exit criteria**: A user can configure language-specific analysis, index multiple text columns with boosting, and use fuzzy and wildcard queries.

**Timeline**: To be included in ScyllaDB 2027.1.

#### Milestone 4 - Advanced Features & Operational Maturity

This milestone covers features that enhance the search experience and operational tooling beyond core FTS and hybrid search. These are lower-priority items that extend the platform for analytics-oriented use cases, improve operational workflows, and address edge-case scenarios deferred from earlier milestones.

| # | Feature | Description |
|---|---|---|
| 4.1 | **Faceted search** | Return term-frequency buckets (facets) alongside search results (e.g., count of results per category). |
| 4.2 | **Highlighting / snippets** | Return text fragments surrounding matched terms for UI display. |
| 4.3 | **Aggregations** | Histogram, range bucket, avg, min, max, stats over indexed numeric and date fields. |
| 4.4 | **Index-only queries** | Return stored fields directly from the FTS index without reading the base table, for low-latency analytics. |
| 4.5 | **Disk-based index storage** | Optionally persist FTS index segments to local NVMe SSD instead of holding them entirely in RAM, removing the memory-size constraint on index capacity. See the [Index Storage Model](#index-storage-model) section for full details. |
| 4.6 | **Auto-suggest / completion** | Edge-ngram or prefix-based suggestion support for type-ahead UX. |
| 4.7 | **Per-segment merge policies** | Configurable merge policies (log merge, tiered) for index segment management. |
| 4.8 | **Pre-filtering with FTS** | Use full-text match as a filter before vector ANN search to narrow the candidate set, improving recall precision. Deferred from Milestone 2 due to performance considerations. |

**Exit criteria**: A user can use faceted search, highlighting, and aggregations on FTS results; optionally persist FTS indexes to disk with compressed storage; and use auto-suggest, pre-filtering with FTS, and configurable merge policies.

**Timeline**: To be prioritized later.

### Out Of Scope

- **External data sources**: FTS only indexes data stored in ScyllaDB tables. Indexing data from external systems (Kafka, S3, etc.) is not supported.
- **Distributed query planning / joins**: FTS does not introduce distributed joins or sub-queries. Results come from a single table.
- **Natural Language Processing (NLP) pipelines**: Entity extraction, summarization, or LLM-based query rewriting are not part of this feature. The application layer is responsible for these.
- **Real-time streaming / change feeds**: FTS does not expose a change-data-capture stream. It consumes ScyllaDB's existing CDC internally to keep the index current.
- **Full Lucene query syntax**: While the query language is inspired by Lucene syntax, 100% Lucene query compatibility is not a goal.
- **Multi-keyspace / cross-table indexes**: A full-text index is scoped to a single table.
- **Dedicated FTS nodes / independent sharding**: In the initial release, FTS indexes are colocated with vector search indexes on the same index node. Dedicated FTS node topology and independent index sharding across nodes will be addressed in a follow-up PRD.
- **Alternator (DynamoDB-compatible API) integration**: Exposing FTS capabilities through Alternator - enabling DynamoDB-compatible clients to create full-text indexes and execute `MATCH()` queries - is a desired extension but out of scope for this PRD. It will be addressed in a separate PRD that defines the Alternator-specific API surface, query translation layer, and compatibility constraints.
- **Geospatial search**: Geo queries (geo_point, geo_shape) are not included.

## Interfaces

### CQL Extensions

All CQL extensions follow the `CREATE CUSTOM INDEX ... USING` pattern established by Vector Search for consistency and backward compatibility.

#### Create a Full-Text Index

```sql
-- Basic full-text index with default analyzer (standard)
CREATE CUSTOM INDEX IF NOT EXISTS idx_comment_fts
ON myapp.comments (comment)
USING 'fulltext_index';

-- Full-text index with explicit options
CREATE CUSTOM INDEX IF NOT EXISTS idx_en_comment_fts
ON myapp.en_comments (comment)
USING 'fulltext_index'
WITH OPTIONS = {
  'analyzer': 'english',
  'similarity_function': 'BM25'
};

-- Multi-column full-text index (Milestone 3)
CREATE CUSTOM INDEX IF NOT EXISTS idx_en_article_fts
ON myapp.en_articles ((title, body))
USING 'fulltext_index'
WITH OPTIONS = {
  'analyzer': 'english',
  'field_boosts': '{"title": 2.0, "body": 1.0}'
};
```

**Parameters for `WITH OPTIONS`:**

| Option | Type | Default | Description |
|---|---|---|---|
| `analyzer` | `text` | `'standard'` | The name of the built-in analyzer to use. Options: `'standard'`, `'english'`, `'german'`, `'french'`, `'spanish'`, `'italian'`, `'portuguese'`, `'russian'`, `'chinese'`, `'japanese'`, `'korean'`, `'simple'`, `'whitespace'` |
| `custom_analyzer` | `text (JSON)` | - | JSON object specifying tokenizer and filters (e.g., `'{"tokenizer": "unicode", "filters": ["lowercase", "stemmer_en", "stop_en"]}'`). Overrides `analyzer` when provided. |
| `similarity_function` | `text` | `'BM25'` | Scoring function. Currently only `'BM25'` is supported. Reserved for future expansion. |
| `field_boosts` | `text (JSON)` | - | JSON object mapping column names to boost factors for multi-column indexes. |
| `positions` | `boolean` | `true` | Whether to store term positions (required for phrase queries). Set to `false` to save space when phrase queries are not needed. |

#### Drop a Full-Text Index

```sql
DROP INDEX IF EXISTS idx_comment_fts;
```

#### Alter a Full-Text Index

`ALTER INDEX` is not currently supported by CQL and therefore cannot be used with FTS indexes. To change index configuration (e.g., switching the analyzer), the index must be dropped and recreated. When `ALTER INDEX` support is added to CQL, selected parameters that can be changed without a full index rebuild will become alterable in place.

#### Full-Text Search Query (MATCH)

```sql
-- Basic keyword search ordered by relevance
-- MATCH(column, query_string): column is the indexed text column,
-- query_string contains space-separated terms combined with implicit OR
SELECT id, commenter, comment
FROM myapp.comments
WHERE MATCH(comment, 'vector search scalability')
LIMIT 10;

-- Boolean query with AND/OR/NOT (multi-column variant requires Milestone 3)
-- MATCH((col1, col2), query_string): a tuple of columns searches across
-- a multi-column FTS index; the query_string supports AND/OR/NOT operators
-- and quoted phrases
SELECT id, title, body
FROM myapp.en_articles
WHERE MATCH((title, body), 'ScyllaDB AND "full text search" NOT deprecated')
LIMIT 20;

-- Retrieve relevance score (Milestone 2)
-- In Milestone 1, results are ordered by BM25 relevance but the score
-- is not exposed as a selectable column.
-- FTS_SCORE() also used in hybrid queries
-- MATCH(column, query_string): single-column variant
SELECT id, title, FTS_SCORE() AS score
FROM myapp.en_articles
WHERE MATCH(title, 'database performance')
LIMIT 10;
```

**MATCH function specification:**

```
MATCH( <column_or_columns>, <query_string> )
```

- `<column_or_columns>`: A single indexed column name or a tuple of column names covered by a multi-column FTS index (Milestone 3).
- `<query_string>`: A text literal containing terms, operators, and phrases.
- **`LIMIT` is required** on all FTS queries. The server rejects `MATCH()` queries without an explicit `LIMIT` clause.

**Query language within `<query_string>`:**

| Syntax | Example | Description |
|---|---|---|
| `term` | `'database'` | Single term match |
| `term1 AND term2` | `'error AND timeout'` | Both terms must appear |
| `term1 OR term2` | `'scylla OR cassandra'` | Either term must appear |
| `NOT term` | `'error NOT warning'` | Exclude term |
| `"phrase"` | `'"out of memory"'` | Exact phrase match |
| `term~N` | `'databse~1'` | Fuzzy match with edit distance N (Milestone 3) |
| `term*` | `'scyll*'` | Prefix/wildcard match (Milestone 3) |
| `(group)` | `'(error OR fault) AND critical'` | Grouping with parentheses |

**Lucene syntax compatibility:**

The query language within `MATCH()` is inspired by [Apache Lucene's query parser syntax](https://lucene.apache.org/core/9_0_0/queryparser/org/apache/lucene/queryparser/classic/package-summary.html) but is **not** a full implementation. The following table summarizes compatibility:

| Lucene syntax | Supported | Notes |
|---|---|---|
| Single term (`database`) | Yes | |
| Boolean operators (`AND`, `OR`, `NOT`) | Yes | |
| Phrase queries (`"out of memory"`) | Yes | |
| Grouping with parentheses | Yes | |
| Fuzzy queries (`term~N`) | Yes | Milestone 3 |
| Prefix/wildcard (`term*`) | Yes | Milestone 3. Only trailing wildcards; leading wildcards (`*term`) are not supported. |
| Field-scoped queries (`title:database`) | No | Column selection is done via `MATCH()` arguments, not within the query string. |
| Range queries (`[a TO z]`) | No | Range predicates use standard CQL `WHERE` clauses on indexed fields. |
| Boosting (`term^2.0`) | No | Use `field_boosts` in `WITH OPTIONS` for per-column boosting. |
| Regular expressions (`/pattern/`) | No | |
| Proximity queries (`"term1 term2"~N`) | No | May be considered in a future milestone. |
| Required/prohibited (`+term`, `-term`) | No | Use `AND` / `NOT` operators instead. |

Users migrating from Elasticsearch or Solr should review their query strings for unsupported syntax and adapt accordingly.

#### Hybrid Search Query (Milestone 2)

```sql
-- Hybrid search: combine vector similarity and full-text relevance via fusion
-- USING FUSION makes MATCH() contribute to scoring alongside ANN
SELECT id, title, body
FROM myapp.en_articles
WHERE MATCH(title, 'full text search')
ORDER BY article_vector ANN OF [0.12, 0.34, ..., 0.05]
LIMIT 10
USING FUSION = { 'strategy': 'RRF', 'k': 60 };

-- Hybrid search with weighted linear combination
SELECT id, title, body
FROM myapp.en_articles
WHERE MATCH(title, 'full text search')
ORDER BY article_vector ANN OF [0.12, 0.34, ..., 0.05]
LIMIT 10
USING FUSION = { 'strategy': 'WEIGHTED', 'vector_weight': 0.7, 'text_weight': 0.3 };

-- FTS as a boolean filter with ANN ranking (no fusion)
-- Without USING FUSION, MATCH() acts purely as a filter
SELECT id, title, body
FROM myapp.en_articles
WHERE MATCH(body, 'machine learning')
ORDER BY article_vector ANN OF [0.12, 0.34, ..., 0.05]
LIMIT 10;
```

**Hybrid query semantics:**

- **With `USING FUSION`**: Both `MATCH()` and `ANN OF` contribute scores that are combined using the specified fusion strategy. The result set is ranked by the fused score.
- **Without `USING FUSION`**: `MATCH()` in the `WHERE` clause acts as a boolean filter — only rows that satisfy the full-text predicate are included, and ranking is determined solely by `ANN OF` vector similarity. This is consistent with how standard `WHERE` clauses filter [vector search queries](https://docs.scylladb.com/manual/branch-2026.1/cql/dml/select.html#vector-queries-scylladb-cloud) in ScyllaDB. In Milestone 2 (feature 2.4), this filtering is applied after ANN retrieval (post-filter). In Milestone 4 (feature 4.8), the query planner may optimize this by applying the FTS filter before ANN search (pre-filter) to narrow the candidate set and improve recall.

**Fusion parameters:**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `strategy` | `text` | `'RRF'` | Fusion strategy: `'RRF'` (Reciprocal Rank Fusion) or `'WEIGHTED'` (weighted linear combination). |
| `k` | `int` | `60` | RRF constant (controls influence of rank position). Only for `'RRF'`. |
| `vector_weight` | `float` | `0.5` | Weight for vector similarity score. Only for `'WEIGHTED'`. |
| `text_weight` | `float` | `0.5` | Weight for FTS relevance score. Only for `'WEIGHTED'`. |

### CQL Compatibility with Cassandra Ecosystem

The following table describes compatibility with other Cassandra-compatible databases:

| Feature | ScyllaDB FTS | Apache Cassandra | DataStax Enterprise (DSE) | Astra DB |
|---|---|---|---|---|
| **Index creation syntax** | `CREATE CUSTOM INDEX ... USING 'fulltext_index'` | `CREATE CUSTOM INDEX ... USING 'org.apache.cassandra.index.sasi.SASIIndex'` (SASI) | `CREATE SEARCH INDEX ON ...` (Solr-based DSE Search) | `CREATE CUSTOM INDEX ... USING 'StorageAttachedIndex'` (SAI) |
| **Query syntax** | `WHERE MATCH(col, 'query')` | `WHERE col LIKE '%term%'` (SASI) | Via Solr JSON/CQL integration | `WHERE col : 'term'` (SAI analyzer) |
| **BM25 scoring** | Native | Not available | Via Solr | BM25 via Data API lexical search (preview) |
| **Hybrid (vector + text)** | Native CQL: `ANN OF` + `MATCH()` + `USING FUSION` | Not available | Not available | Via Data API `findAndRerank` (preview, not CQL) |
| **Phrase queries** | Yes | Limited (SASI) | Yes (Solr) | No |
| **Language analyzers** | Yes (17+ languages + CJK) | No | Yes (Solr) | Yes (SAI, 30+ languages via Lucene analyzers) |
| **Boolean operators** | AND, OR, NOT | No | Yes (Solr) | Limited (AND via CQL `WHERE` clause) |

> **Note on Astra DB**: Astra DB's SAI-based analyzer and hybrid search capabilities are based on publicly available documentation as of March 2026. Astra DB's hybrid search with BM25 lexical scoring and reranking is in public preview (Data API only, limited to AWS us-east-2). These capabilities may change as features progress toward GA.

**Compatibility notes:**

- **CQL wire protocol**: FTS queries use standard CQL protocol v5 (same as Vector Search). Existing CQL drivers can send FTS queries as prepared statements without modification. The `MATCH()` function and `USING FUSION` clause are server-side extensions that degrade gracefully - drivers that do not recognize them simply pass the CQL string through.
- **Migration from DSE Search**: Users migrating from DSE Search (Solr-based) will need to adapt their query syntax from Solr JSON to ScyllaDB's `MATCH()` function. A migration guide will be provided.
- **Migration from SASI**: SASI's `LIKE` operator is a subset of FTS capabilities. Replacing `WHERE col LIKE '%term%'` with `WHERE MATCH(col, 'term')` provides better performance (SASI `LIKE '%term%'` performs a full index scan with no early termination, while `MATCH()` uses an inverted index for O(1) term lookup) and ranking (SASI returns unranked results, while `MATCH()` orders by BM25 relevance score).
- **Future interoperability**: The `MATCH()` function name and semantics are designed to be portable. If Apache Cassandra or Astra adopt a similar function in the future, ScyllaDB's syntax is positioned to be compatible or easily adapted.

### Compatibility with OpenSearch

OpenSearch compatibility is a multi-layered topic. The following table summarizes each layer, its scope, and the recommended approach:

| Layer | Description | Recommendation |
|---|---|---|
| **A) Elasticsearch vs. OpenSearch** | Elasticsearch (re-licensed as open source) and OpenSearch have significantly diverged since the 2021 fork. Most AWS customers use OpenSearch. | Target OpenSearch semantics. This decision is relevant only if layers B and C are pursued. |
| **B) Driver compatibility** | Amazon maintains OpenSearch client libraries (per language) that handle node discovery, high availability, and load balancing. | Not planned. See rationale below. |
| **C) REST API compatibility** | OpenSearch exposes a JSON-based REST API that is fundamentally different from CQL. A translation layer could be implemented but would require a new compatibility layer in ScyllaDB with a dedicated port. | Not planned. See rationale below. |
| **D) Query parser syntax** | OpenSearch translates JSON queries to Apache Lucene syntax internally. This layer is largely invisible to end users. | No action required. The choice of internal query parser does not affect the user-facing API. |
| **E) Indexing and scoring engine** | Tokenization, term weighting (BM25), and hybrid query fusion algorithms. | Addressed natively. ScyllaDB FTS should use a Lucene-compatible library for BM25 scoring and hybrid fusion, providing equivalent functionality through the CQL interface. |

**Rationale for not pursuing layers B and C:**

- Implementing full REST API and driver compatibility presents the same challenges encountered with the Alternator (DynamoDB-compatible API): a large engineering investment across multiple language-specific drivers, a new network protocol layer, and significant ongoing maintenance.
- Experience with Alternator has shown that convincing customers to adopt a compatibility API instead of the native query language (CQL) is a prolonged process with limited adoption.
- Users migrating from OpenSearch will need to adapt their application code to use CQL and the `MATCH()` function. However, the underlying search logic (BM25 ranking, boolean queries, hybrid fusion) remains functionally equivalent, and the query translation is straightforward.

> **Note**: An alternative approach — deploying OpenSearch as a proxy or forwarding queries to AWS OpenSearch — is not recommended. It introduces significant performance overhead (additional network hops, JVM garbage collection tuning) and does not resolve driver compatibility challenges.

### CLI

No new CLI tools. Index management uses existing `cqlsh` and the CQL interface.

## Reliability and Availability

### Consistency Model

FTS provides **eventual consistency** — the same model used by Vector Search today. Read-after-write consistency is not provided. The data path from write to searchability is:

1. Client writes to ScyllaDB (CQL INSERT/UPDATE/DELETE).
2. ScyllaDB records the mutation in its CDC log.
3. The index node asynchronously consumes CDC events.
4. Events are batched (`VECTOR_STORE_FTS_INDEXING_BATCH_SIZE`, default 1000).
5. Batches are committed at intervals (`VECTOR_STORE_FTS_INDEXING_COMMIT_INTERVAL_MS`, default 1000ms).
6. Only after commit are new documents visible to `MATCH()` queries.

Steps 2–5 are asynchronous. A client that writes a row and immediately issues a `MATCH()` query **may not see that row**. The `fts_indexing_lag_seconds` metric tracks this delay, with a <3s target under normal load. Each index node replica independently consumes the CDC stream, so different replicas may be at slightly different positions.

**Why read-after-write consistency is not provided:** It would require the coordinator to either (a) block the write until all index node replicas have indexed the row, adding seconds of latency to every write, or (b) implement a read-your-writes session guarantee by routing FTS queries to a specific replica and waiting for its CDC position to catch up, adding complexity and latency. Neither trade-off is acceptable for the target use cases.

### High Availability

- **Same HA model as Vector Search**: FTS index nodes are deployed per availability zone. The coordinator routes queries to healthy index node replicas. If an index node fails, the coordinator falls back to other replicas that hold the same data.
- **Replication**: Each FTS index is replicated on every index node.
- **No single point of failure**: Loss of a single FTS node does not cause data unavailability.

### Failure Modes and Mitigations

| Failure | Impact | Mitigation |
|---|---|---|
| FTS index node process crash | Index is lost (in-memory only in Milestones 1–3); queries fall back to other replicas | Automatic restart by systemd/supervisor; index is rebuilt from the base table on startup. Coordinator retries against other replicas during recovery. Disk-based persistence (Milestone 4) will eliminate rebuild time. |
| Index corruption | Stale or missing results | Recovery requires dropping and recreating the index. |
| ScyllaDB node failure | FTS index reading CDC from that node stops receiving updates | Driver should automatically switch to other ScyllaDB node |
| Indexing lag (index node behind ScyllaDB writes) | Recently written data not yet searchable | Expose `indexing_lag_seconds` metric; alert when lag exceeds threshold |
| Network partition between ScyllaDB and index node | Queries fail; indexing stalls | Index node reconnect logic with exponential backoff; after reconnection the index node continues serving queries from its last-known index state rather than failing, accepting that results may not reflect the most recent writes) |

### Upgrade Considerations

- **Before FTS is available**: No impact. Existing clusters operate normally.
- **After FTS is available**: FTS is an additive feature. Existing tables and queries are unaffected. FTS indexes are opt-in.
- **Upgrade path**: Index node binary upgrades follow the same rolling-upgrade pattern as Vector Search.
- **Backward compatibility**: Dropping FTS support from a cluster (removing index nodes) causes FTS queries to fail but does not affect the base table data.

## Supportability

### Tracing

- FTS query execution will integrate with ScyllaDB's existing CQL tracing (`TRACING ON`).
- For hybrid queries, the trace will show both the vector and FTS legs with individual timings.

### Monitoring

Prometheus metrics exposed by the index node:

| Metric | Type | Description |
|---|---|---|
| `fts_query_latency_seconds` | histogram | Query latency broken down by index name |
| `fts_query_count_total` | counter | Total number of FTS queries processed |
| `fts_index_doc_count` | gauge | Number of documents indexed per index |
| `fts_index_size_bytes` | gauge | Memory size of the FTS index per index |
| `fts_indexing_lag_seconds` | gauge | Delay between a mutation being written to ScyllaDB and becoming searchable in the FTS index |
| `fts_indexing_rate` | gauge | Documents indexed per second |
| `fts_segment_count` | gauge | Number of index segments per index |
| `fts_merge_duration_seconds` | histogram | Duration of segment merge operations |
| `hybrid_query_latency_seconds` | histogram | End-to-end latency of hybrid queries |

Grafana dashboard templates will be provided alongside existing Vector Search dashboards.

### Configuration

All FTS configuration is done via environment variables on the index node, consistent with the existing Vector Search configuration model. The service supports `.env` files. Both are live-updateable using `SIGHUP`.

The following new environment variables are introduced for FTS. They coexist with the existing `VECTOR_STORE_*` variables on the same index node process:

- `VECTOR_STORE_FTS_THREADS`: `unsigned integer`, number of threads from the total `VECTOR_STORE_THREADS` pool to dedicate to FTS operations (query execution, indexing, and segment merging). The remaining threads in the `VECTOR_STORE_THREADS` pool are used for vector search. For example, on a node with `VECTOR_STORE_THREADS=8` and `VECTOR_STORE_FTS_THREADS=3`, 3 threads handle FTS and 5 handle vector search. Default `0` (FTS disabled). **Validation**: if `VECTOR_STORE_FTS_THREADS` is set to a value greater than `VECTOR_STORE_THREADS`, the index node will fail to start with a configuration error. Setting `VECTOR_STORE_FTS_THREADS` equal to `VECTOR_STORE_THREADS` is permitted for nodes that serve only FTS indexes (no vector indexes); a warning is logged in this case since no threads remain for vector search. Additionally, `VECTOR_STORE_THREADS` must not exceed the number of available CPU cores — over-subscription is not allowed to prevent unpredictable tail latency under load. The index node will fail to start if `VECTOR_STORE_THREADS` exceeds the detected core count.
- `VECTOR_STORE_FTS_MAX_INDEX_SIZE_MB`: `unsigned integer`, maximum RAM in MB dedicated to the FTS index memory pool per node. This pool is exclusively for FTS data - the vector index memory pool and OS/system memory are allocated separately and are not subtracted from this value. The operator must ensure that the sum of the FTS pool, the vector index pool, and OS overhead fits within the node's total RAM. Default: 20% of total system RAM as detected at startup (before subtracting vector index allocation or OS overhead). The operator must ensure that the sum of this value, the vector index memory pool, and OS/process overhead does not exceed the node's total RAM.
- `VECTOR_STORE_FTS_MAX_CLAUSES_PER_QUERY`: `unsigned integer`, maximum number of boolean clauses in a single FTS query. Default `1024`.
- `VECTOR_STORE_FTS_INDEXING_BATCH_SIZE`: `unsigned integer`, number of mutations batched before committing to the FTS index. Default `1000`.
- `VECTOR_STORE_FTS_INDEXING_COMMIT_INTERVAL_MS`: `unsigned integer`, maximum time in milliseconds between index commits. Controls searchability latency - the delay between a write being read by CDC and becoming visible to FTS queries. Default `1000`.
- `VECTOR_STORE_FTS_MAX_WILDCARD_EXPANSIONS`: `unsigned integer`, maximum number of terms a single wildcard clause (e.g., `a*`) may expand to before the query is rejected. Default `65536`.

**Resource protection:**

- **Query timeout**: FTS queries respect the standard CQL query timeout (`USING TIMEOUT`). If no explicit timeout is set, the server-side default timeout applies, consistent with other CQL operations.
- **Max concurrent FTS queries**: FTS query concurrency is bounded by the same mechanism used for Vector Search queries.
- **Expensive wildcard query protection**: Wildcard queries that would expand to an excessive number of terms (e.g., `a*` matching millions of terms) are automatically limited by a maximum term expansion threshold (default: 65,536 terms per wildcard clause). Queries exceeding this limit return an error indicating the wildcard pattern is too broad, prompting the user to provide a more specific prefix. This threshold is configurable via `VECTOR_STORE_FTS_MAX_WILDCARD_EXPANSIONS`.

## Affected Components

- [x] Drivers - No code changes required; drivers pass CQL strings through. Documentation updates only.
- [x] Scylla Manager - Scylla Manager must back up FTS index metadata (index definitions, analyzer configuration) and recreate indexes on restore, following the same approach used for vector search indexes.
- [x] Cloud - Cloud API endpoints for FTS node lifecycle. Cloud UI for enabling/disabling FTS.
- [ ] Operator - Not initially - similarly to Vector Search.
- [x] Other: Vector Store Index Node - Core implementation of FTS indexing and query processing.
- [x] Other: ScyllaDB Core - Routing FTS and hybrid queries to the index node.

## Performance

### Expected Performance Characteristics

The targets below are initial estimates based on the following assumptions:

- **Hardware baseline**: A single index node running on an `r8g.xlarge`-class instance (4 vCPUs, 32 GB RAM, local NVMe SSD) - the same instance type used for Vector Search benchmarks.
- **Dataset profile**: 10 million documents with an average size of 1 KB, using the English `standard` analyzer (tokenizer + lowercase + stop-word removal). Documents contain natural-language text with typical term distribution ([Zipf's law](https://en.wikipedia.org/wiki/Zipf%27s_law)).
- **In-memory index**: In Milestones 1–3 the entire FTS index resides in RAM. The index size (typically 30–70% of raw text size) plus the vector index must fit within the node's available memory. For the reference dataset (10M × 1 KB = 10 GB raw text), the FTS index is expected to consume ~3–7 GB of RAM.
- **Single-node measurement**: Latency targets are measured on a single index node. Coordinator-level fan-out across multiple nodes adds network overhead but benefits from parallelism; end-to-end latency is expected to remain within the same order of magnitude.
- **Tantivy baseline**: Indexing throughput and query latency estimates are informed by published Tantivy benchmarks on comparable hardware, adjusted for the overhead of CQL integration, CDC-based ingestion, and index-node-to-coordinator communication.

These targets will be validated and refined during the benchmark phase (see Performance Validation below). Details will be designed in Technical Design Document.

| Metric | Target | Notes |
|---|---|---|
| **Indexing throughput** | ≥ 10,000 docs/sec per vCPU | For average document size of 1KB |
| **Query latency (p99)** | < 10ms for simple term queries | Single-node, in-memory index |
| **Query latency (p99)** | < 50ms for complex boolean/phrase queries | Single-node, in-memory index |
| **Hybrid query latency (p99)** | < 50ms | Combined FTS + ANN, single coordinator |
| **Indexing lag** | < 3 seconds | Time from ScyllaDB write to FTS searchability |
| **Index size overhead** | 30-70% of raw text data size | RAM consumed by the in-memory index. Depends on analyzer and stored fields. |

### Performance Validation

- **Load generator**: [Latte](https://github.com/scylladb/latte) - a Rust-based CQL load generator with scripting support. FTS-specific workload scripts will be authored to exercise indexing throughput, query latency by query type, and hybrid query benchmarks.
- **Benchmarking framework**: [Scylla-Cluster-Tests (SCT)](https://github.com/scylladb/scylla-cluster-tests) - the standard ScyllaDB performance and longevity testing framework. SCT will orchestrate multi-node cluster provisioning, Latte workload execution, metric collection, and regression detection.
- **Regression tests**: Ensure that adding an FTS index on a table does not degrade CQL read/write latency for non-FTS queries by more than 5%. Workload should be kept isolated.
- **Load tests**: Sustained mixed workloads (writes + FTS queries + ANN queries + hybrid queries) at target throughput levels, executed via SCT longevity jobs.

### Scalability

**Initial deployment model (Milestones 1–3):** The FTS index is colocated with the vector search index on the same index node and held entirely in memory. Although both index types share a single index node process, they operate with **isolated resource pools**: a configurable portion of `VECTOR_STORE_THREADS` is dedicated to FTS via `VECTOR_STORE_FTS_THREADS`, with the remainder used for vector search. Memory pools are also dedicated per index type. This ensures that a heavy FTS workload does not starve vector search (and vice versa), and provides predictable resource consumption for capacity planning.

**Horizontal scaling via index node replicas:** Additional index nodes can be added independently of ScyllaDB nodes. Each index node replica holds a full copy of all indexes (both FTS and vector search) for the cluster it serves. The coordinator distributes queries across available index node replicas, so adding more replicas increases query throughput and reduces per-node load without requiring additional ScyllaDB nodes. This is the primary horizontal scaling mechanism in the initial model.

| Constraint | Implication |
|---|---|
| **Shared resources** | FTS and vector search are colocated on the same index node but use **dedicated thread allocations** (controlled by `VECTOR_STORE_FTS_THREADS` out of the total `VECTOR_STORE_THREADS` pool) and **dedicated memory pools**. Each index type has a configured CPU and RAM budget, preventing one workload from starving the other. Sizing must account for the combined resource allocation of both index types. |
| **Horizontal scaling** | Additional index nodes (each hosting all FTS + vector indexes) can be added without adding ScyllaDB nodes. The coordinator load-balances queries across replicas. This scales read throughput linearly with the number of index node replicas. |
| **Full-index replication** | Every index node replica stores the complete set of indexes (FTS and vector) for all tablets. This simplifies query routing (any replica can serve any query type) but means each new index node requires storage and memory for both index types. |
| **Indexing throughput** | CDC-based ingestion feeds both index types through **dedicated thread allocations** (`VECTOR_STORE_FTS_THREADS` threads for FTS, remaining `VECTOR_STORE_THREADS` for vector search). This isolation prevents a high FTS write rate from blocking vector index updates. Each index node replica independently consumes the CDC stream and builds its own index copy. |

**Future scalability enhancements (post-Milestone 4):**

- **Disk-based index storage** (Milestone 4): Optionally persist FTS index segments to disk, removing the RAM-size constraint. See the [Index Storage Model](#index-storage-model) section for full details.
- **Dedicated FTS nodes**: Deploy FTS indexes on separate index nodes that do not host vector indexes, allowing independent resource allocation and scaling per index type.
- **Index sharding**: Split a single FTS index across multiple index nodes, enabling horizontal scale-out for very large indexes that exceed single-node capacity.
- **Tiered storage**: Move cold FTS segments to lower-cost storage (e.g., EBS, S3) while keeping hot segments on local NVMe.

These enhancements will be scoped in a follow-up PRD once the colocated model has been validated in production.

### Index Storage Model

**Milestones 1–3 - In-memory index:** The entire FTS index (inverted index, term dictionary, stored fields, position data) is held in the index node process's memory. This provides the lowest possible query latency and simplifies the initial implementation, but limits the maximum FTS index size to the node's available RAM and increases TCO since RAM is significantly more expensive than disk storage.

| Aspect | Requirement |
|---|---|
| **Primary storage** | RAM. The full index resides in the index node process heap. There is no disk persistence for index data in the initial release. |
| **Durability** | The index is **not** durable across index node restarts. On restart, the index node rebuilds the FTS index from the base ScyllaDB table via a full scan. Rebuild time depends on dataset size and available vCPUs. Using the indexing throughput baseline of 10,000 docs/sec per vCPU (see [Expected Performance Characteristics](#expected-performance-characteristics)), a 4-vCPU node rebuilds the 10M-document reference dataset in ~250 seconds (~4 minutes). This is an optimistic estimate — actual rebuild time depends on ScyllaDB's full-scan read throughput and network bandwidth between ScyllaDB and the index node, which may be the bottleneck rather than indexing speed. During rebuild, the index node does not serve FTS queries - other replicas handle traffic. |
| **Sizing constraint** | The FTS index plus the vector index must fit within the node's RAM. Operators must set `VECTOR_STORE_FTS_MAX_INDEX_SIZE_MB` to cap FTS memory usage and leave headroom for the vector index, OS, and index node overhead. |
| **Write path** | New documents are indexed in-place into the in-memory index. When a row's text column is updated (`UPDATE`), the FTS index atomically replaces the old tokens with new tokens derived from the updated text — there is no window where duplicate or stale entries are visible to queries. Deletes remove the corresponding tokens from the index. Commits (`VECTOR_STORE_FTS_INDEXING_COMMIT_INTERVAL_MS`) make new data visible to queries but do not involve disk I/O. |
| **Segment merges** | Merge operations run entirely in memory. They compact segments to reduce their count and reclaim space from deleted documents. Merge concurrency is managed within the FTS thread allocation (`VECTOR_STORE_FTS_THREADS`). |
| **Maximum index size** | Bounded by `VECTOR_STORE_FTS_MAX_INDEX_SIZE_MB`. When the limit is reached, new indexing operations are rejected and an alert is raised via the `fts_index_size_bytes` metric. |

**Milestone 4 - Disk-based index storage (optional):** Milestone 4 introduces the option to persist FTS index segments to local NVMe SSD, using the OS page cache for read acceleration (the same model used by Tantivy and Apache Lucene). This removes the RAM-size constraint on index capacity, enables indexes larger than available memory, and provides durability across index node restarts (no rebuild needed). Query latency degrades gracefully when the working set exceeds page cache, proportional to disk IOPS rather than failing. Compressed storage (LZ4, Zstd) will be supported as a sub-option. This mode is opt-in as query performance will be significantly worse than the in-memory index, but it reduces TCO since disk storage is much cheaper than RAM.

## High-Level Testing Approach

### Unit Tests

- Query parser correctness for all supported query syntax.
- Fusion strategies (RRF, weighted) produce correct merged rankings.

**Note:** Analyzer/tokenizer correctness and BM25 scoring accuracy are not tested at this level. These are core capabilities of the underlying search engine library (Tantivy or some alternative) and are validated as part of its own QA process. Our unit tests focus on the integration logic we build on top of the library.

### Integration Tests (Validator)

Extend `crates/validator-vector-store` with FTS test suites:

- **Index lifecycle**: Create, populate, query, drop FTS index.
- **CRUD consistency**: Inserts, updates, and deletes are reflected in FTS results.
- **Multi-column indexing**: Queries across multiple indexed columns.
- **Hybrid search**: Combined ANN + FTS queries return correctly fused results.
- **Language analyzers**: Language-specific stemming and tokenization produce expected results.

### HA / Fault Tolerance Tests

- FTS index node failure during indexing - verify no data loss, index self-heals.
- FTS index node failure during query - verify coordinator retries and returns results.
- Simultaneous ScyllaDB + FTS node failure - verify recovery.
- Rolling upgrade of FTS index node - verify zero-downtime.

### Performance Tests

- Indexing throughput benchmarks at various document sizes.
- Query latency benchmarks across query types (term, phrase, boolean, wildcard, hybrid).
- Index size benchmarks across analyzer configurations.

### Compatibility Tests

- Verify FTS queries work from all supported CQL drivers (Python, Java, Go, Rust, Node.js).
- Verify FTS queries work from `cqlsh`.
- Verify that `MATCH()` syntax does not break compatibility with existing non-FTS queries.

## Enablement

- [x] Experimental feature - N/A. Milestone 1 ships directly as GA.
- [x] Preview - N/A. Milestone 1 ships directly as GA.
- [x] Production - Milestone 1 ships as GA (Production) in ScyllaDB Cloud. Follow-up milestones (2, 3, 4) are delivered incrementally in subsequent releases.
- [x] Enabled/disabled via a feature switch - FTS requires explicit deployment of FTS index nodes (same as Vector Search). No impact on clusters without FTS enabled.

## Security

- **Authentication and authorization**: FTS queries are subject to the same CQL authentication and RBAC rules as any other CQL query. If a user does not have `SELECT` permission on a table, they cannot run FTS queries on that table's indexes. It will be integrated with the current authentication and authorization mechanism used by Vector Store.
- **Data at rest**: FTS index files on disk contain indexed terms and stored fields derived from table data. They must be covered by Encryption at Rest (EaR) when enabled. The index node will use the same encrypted volume configuration as Vector Search. This will be added whenever the disk storage is implemented.
- **Data in transit**: Communication between ScyllaDB coordinator and the FTS index node uses the same TLS-protected internal protocol used by Vector Search.
- **Audit logging**: FTS queries will appear in CQL audit logs with the same detail as any other CQL statement.
- **No new sensitive data**: FTS indexes are derived from existing table data. No new user credentials or PII are introduced.

## Relevant Documents

|   |   |
|---|---|
| Test Plan | _To be created in_ `crates/validator-vector-store/src/` _following existing test patterns_ |
| Design Document | _To be created - will cover: index storage format, index node integration, query routing, segment merge strategy_ |
| Vector Search Reference | [ScyllaDB Vector Search Documentation](https://cloud.docs.scylladb.com/stable/vector-search/index.html) |
| Benchmark Plan | _Latte workload scripts + SCT test definitions for FTS performance and longevity testing_ |

---

# Internal Training

### SA / Support Enablement

- [x] Internal technical webinar - FTS architecture, CQL syntax, troubleshooting common issues (analyzer misconfiguration, indexing lag).
- [x] Demo - Live demo of FTS + Hybrid Search on ScyllaDB Cloud.
- [x] Documentation: How to support, config, etc - Runbook for FTS index node operations, common failure modes, and resolution steps.

### Sales Enablement

- [x] Internal webinar: Functionality, Why it is important? How will it help sell Scylla? Who is it relevant for? - Focus on: eliminating Elasticsearch dependency, hybrid search as AI differentiator, competitive positioning vs. DSE Search and pgvector+pg_trgm.

### R&D Knowledge Sharing

- [x] Internal technical session - Deep dive into FTS engine internals, analyzer pipeline, hybrid fusion algorithms.
- [x] Demo - End-to-end walkthrough: index creation → data ingestion → FTS query → hybrid query.
- [x] Documentation: How to use, config, etc - Developer guide in `docs/` covering all CQL extensions, analyzer options, and performance tuning.
