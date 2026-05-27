"""ScyllaDB + LangChain integration for the vector search example.

This module holds the LangChain-specific code: building the LangChain Cassandra
vector store, inserting documents, and running an approximate nearest-neighbor
search. It works on an already-connected ScyllaDB session (provided by the
caller); the LangChain/CassIO connector creates the vector table and indexes
automatically.
"""
from __future__ import annotations

import time

from cassandra.cluster import Session
from langchain_community.embeddings.fastembed import FastEmbedEmbeddings
from langchain_community.utilities.cassandra import SetupMode
from langchain_community.vectorstores import Cassandra
from langchain_core.documents import Document


KEYSPACE = "langchain_demo"
TABLE_NAME = "langchain_rag_demo"
# A small local embedding model from FastEmbed. It runs on CPU and downloads
# automatically on first use, so the example needs no paid embedding API.
# Its 384-dimensional output determines the size of the stored vectors.
EMBEDDING_MODEL = "BAAI/bge-small-en-v1.5"
# The Vector Store service builds the ANN index asynchronously, so the first
# search right after table creation can briefly fail while the index is built.
# We poll up to INDEX_READY_ATTEMPTS times, sleeping INDEX_READY_DELAY_SECONDS
# between attempts (i.e. up to ~60 seconds total).
INDEX_READY_ATTEMPTS = 30
INDEX_READY_DELAY_SECONDS = 2

# The demo corpus inserted into ScyllaDB. Each entry is (document id, text).
# The text is what gets embedded and searched; the id is used as the primary
# key so re-runs upsert instead of duplicating rows.
DEMO_DOCUMENTS = (
    (
        "vector-store-indexing",
        "ScyllaDB Vector Store indexes vector columns stored in ScyllaDB tables "
        "and serves approximate nearest-neighbor search for application queries.",
    ),
    (
        "langchain-cassandra-connector",
        "LangChain's Cassandra vector store connector uses the Cassandra Query "
        "Language protocol and can exercise ScyllaDB compatibility through CassIO.",
    ),
    (
        "scylla-cloud-auth",
        "The demo connects to a Scylla deployment with CQL contact points and "
        "username/password authentication loaded from a local .env file.",
    ),
    (
        "ann-search",
        "Approximate nearest-neighbor queries compare an embedded question with "
        "stored document embeddings and return the closest matching documents.",
    ),
    (
        "local-embeddings",
        "The example uses a local FastEmbed embedding model so it does "
        "not require a paid embedding API subscription.",
    ),
)


def create_vector_store(session: Session) -> Cassandra:
    """Build a LangChain Cassandra vector store backed by the ScyllaDB session.

    This "vector store" is LangChain's client-side abstraction, not to be
    confused with the ScyllaDB Vector Store component (this repository).

    SetupMode.SYNC tells LangChain/CassIO to create the table and its vector
    index up front (synchronously) if they do not already exist, so the caller
    does not have to write any CQL DDL.
    """
    # FastEmbed loads the embedding model locally; the same model must be used
    # for both inserts and queries so the vectors live in the same space.
    embeddings = FastEmbedEmbeddings(model_name=EMBEDDING_MODEL)
    return Cassandra(
        embedding=embeddings,
        session=session,
        keyspace=KEYSPACE,
        table_name=TABLE_NAME,
        setup_mode=SetupMode.SYNC,
    )


def insert_documents(vector_store: Cassandra) -> int:
    """Embed and upsert the demo documents, returning how many were written."""
    # LangChain embeds each document's page_content and stores the vector. The
    # metadata 'id' is surfaced later when printing results.
    documents = [
        Document(page_content=text, metadata={"id": document_id})
        for document_id, text in DEMO_DOCUMENTS
    ]
    # Passing explicit ids makes inserts idempotent: re-running the example
    # updates the same rows instead of creating duplicates.
    ids = [document_id for document_id, _ in DEMO_DOCUMENTS]
    vector_store.add_documents(documents, ids=ids)
    return len(documents)


def is_index_still_building_error(error: Exception) -> bool:
    """Return True if the error means the ANN index is still being built.

    Any other error (auth, schema, connectivity) should propagate instead of
    being retried.
    """
    # While the ANN index is not ready, the Vector Store service answers the
    # search with HTTP 404 (index not created yet) or HTTP 503 (index still
    # being constructed). ScyllaDB surfaces that status in the CQL error
    # message, and matching the status code is more stable than the prose text.
    message = str(error)
    return "HTTP status 404" in message or "HTTP status 503" in message


def wait_for_index(vector_store: Cassandra, query: str) -> None:
    """Block until the ANN index is queryable, then return.

    Because the index is built asynchronously, we probe once at the start of the
    workflow. Once this returns, later searches can run directly without any
    retry logic, which keeps the common search path simple.
    """
    for attempt in range(INDEX_READY_ATTEMPTS):
        try:
            # A single-result probe is enough to tell whether the index is
            # ready; discard it and let the caller run the "real" search.
            vector_store.similarity_search_with_score(query, k=1)
            return
        except Exception as error:
            # Only swallow the "index not ready yet" error; re-raise anything
            # else so genuine failures are not hidden.
            if not is_index_still_building_error(error):
                raise
            if attempt == 0:
                print("Waiting for the Vector Store to build the ANN index...")
            time.sleep(INDEX_READY_DELAY_SECONDS)
    raise RuntimeError("Vector Store did not finish building the ANN index in time")


def populate_and_search(
    session: Session, query: str, k: int
) -> list[tuple[Document, float]]:
    """Run the full demo flow and return the top-k (document, score) matches.

    Steps: build the vector store (creating the table/index on demand), insert
    the demo documents, wait for the ANN index to be ready, then run the
    similarity search. Higher scores mean closer (more similar) matches.
    """
    print(f"Loading embedding model {EMBEDDING_MODEL}...")
    vector_store = create_vector_store(session)

    count = insert_documents(vector_store)
    print(f"Inserted or updated {count} demo documents.")

    # Pay the one-time wait for the async index, then search without retries.
    wait_for_index(vector_store, query)
    return vector_store.similarity_search_with_score(query, k=k)
