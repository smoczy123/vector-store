# LangChain Cassandra Example

This example uses LangChain's Cassandra vector store connector against a ScyllaDB deployment. It creates a demo keyspace if needed, lets LangChain/CassIO create the vector table and indexes automatically, inserts a small document set through LangChain, and prints documents returned by `similarity_search_with_score`.

The example uses a local FastEmbed embedding model, so it does not require a paid embedding API subscription. The model is downloaded on first use.

The ScyllaDB- and LangChain-specific integration code lives in `scylla_langchain.py`; `langchain_rag.py` is the command-line entry point that only parses arguments, loads `.env`, and reports missing dependencies.

## Requirements

This example relies on LangChain/CassIO automatic schema setup, which creates a Storage Attached Index (SAI) on the metadata map column. ScyllaDB supports this CassIO pattern starting with **2026.2** when the `enable_cassio_compatibility` flag is enabled; ScyllaDB then rewrites the CassIO metadata SAI statement into a regular secondary index. Enable the flag on every node, for example by passing `--enable-cassio-compatibility 1` on the command line or setting `enable_cassio_compatibility: true` in `scylla.yaml`. The flag is also live-updatable without a restart via `UPDATE system.config SET value = 'true' WHERE name = 'enable_cassio_compatibility';`.

The ANN search itself is served by the ScyllaDB Vector Store service, which must be configured for the cluster.

## Setup

Create and activate a Python virtual environment, then install the example dependencies:

```sh
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
```

Copy the environment template and fill in your ScyllaDB connection details:

```sh
cp .env.example .env
```

The script reads `.env` from this directory. The path is intentionally not configurable.

```dotenv
SCYLLA_CONTACT_POINTS=127.0.0.1
SCYLLA_USERNAME=cassandra
SCYLLA_PASSWORD=cassandra
```

This minimal example does not configure TLS or CA bundles. Use it only with localhost or a trusted private network; production deployments should configure TLS.

The script always uses:

- CQL port `9042`
- keyspace `langchain_demo`
- table `langchain_rag_demo`
- embedding model `BAAI/bge-small-en-v1.5`
- embedding dimension `384`

## Usage

Show the CLI help without connecting to ScyllaDB or loading LangChain dependencies:

```sh
python langchain_rag.py --help
```

Run the example:

```sh
python langchain_rag.py --query "How does ScyllaDB support vector search?"
```

Start from a clean demo table:

```sh
python langchain_rag.py --reset --query "How does ScyllaDB support vector search?" --k 3
```

`--reset` drops the demo table if it exists. Dropping the table also removes its vector index.

## Notes

- The script uses username/password authentication only and does not configure TLS or CA bundles.
- LangChain/CassIO creates the table and indexes automatically (`SetupMode.SYNC`). This requires a ScyllaDB version with `enable_cassio_compatibility` enabled (see Requirements).
- The Vector Store service builds the ANN index asynchronously, so the script waits and retries the first search until the index is ready.
- Do not commit `.env` with real credentials.
- The example prints retrieved documents with scores; it does not run text generation.
