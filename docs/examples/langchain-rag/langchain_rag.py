#!/usr/bin/env python3
"""Command-line entry point for the ScyllaDB + LangChain vector search example.

This file handles only argument parsing, loading connection settings from
`.env`, and reporting missing dependencies with a friendly message. The actual
ScyllaDB/LangChain integration lives in `scylla_langchain.py`.
"""
from __future__ import annotations

import argparse
import importlib
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any


DEFAULT_QUERY = "How does ScyllaDB support vector search?"
DEFAULT_PORT = 9042
DEFAULT_CONNECT_TIMEOUT_SECONDS = 10
SCRIPT_DIR = Path(__file__).resolve().parent


@dataclass(frozen=True)
class Config:
    contact_points: tuple[str, ...]
    username: str
    password: str


def parse_positive_int(value: str) -> int:
    parsed_value = int(value)
    if parsed_value <= 0:
        raise argparse.ArgumentTypeError("value must be greater than zero")
    return parsed_value


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run a minimal LangChain Cassandra vector search example against "
            "ScyllaDB. Configuration is read from .env next to this script."
        )
    )
    parser.add_argument(
        "--query",
        default=DEFAULT_QUERY,
        help=f"question to search for (default: {DEFAULT_QUERY!r})",
    )
    parser.add_argument(
        "--k",
        default=3,
        type=parse_positive_int,
        help="number of documents to retrieve (default: 3)",
    )
    parser.add_argument(
        "--reset",
        action="store_true",
        help="drop and recreate the demo table before inserting documents",
    )
    return parser.parse_args(argv)


def import_dependency(module_name: str, package_name: str) -> Any:
    try:
        return importlib.import_module(module_name)
    except ImportError as error:
        raise RuntimeError(
            f"Missing dependency: {package_name}. "
            "Run `pip install -r requirements.txt`."
        ) from error


def load_config(env_path: Path) -> Config:
    if not env_path.exists():
        raise RuntimeError(
            f"Missing {env_path}. Copy .env.example to .env and fill in the "
            "ScyllaDB connection values."
        )

    dotenv = import_dependency("dotenv", "python-dotenv")
    dotenv.load_dotenv(env_path)

    return Config(
        contact_points=parse_contact_points(require_env("SCYLLA_CONTACT_POINTS")),
        username=require_env("SCYLLA_USERNAME"),
        password=require_env("SCYLLA_PASSWORD"),
    )


def require_env(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def parse_contact_points(value: str) -> tuple[str, ...]:
    contact_points = tuple(item.strip() for item in value.split(",") if item.strip())
    if not contact_points:
        raise RuntimeError("SCYLLA_CONTACT_POINTS must contain at least one host")
    return contact_points


def connect_to_scylla(config: Config) -> tuple[Any, Any]:
    auth_module = import_dependency("cassandra.auth", "cassandra-driver")
    cluster_module = import_dependency("cassandra.cluster", "cassandra-driver")

    auth_provider = auth_module.PlainTextAuthProvider(
        username=config.username,
        password=config.password,
    )
    cluster = cluster_module.Cluster(
        contact_points=list(config.contact_points),
        port=DEFAULT_PORT,
        auth_provider=auth_provider,
        connect_timeout=DEFAULT_CONNECT_TIMEOUT_SECONDS,
    )
    return cluster, cluster.connect()


def create_keyspace(session: Any, keyspace: str) -> None:
    # DDL identifiers are fixed constants; do not pass user-controlled values here.
    session.execute(
        f"""
        CREATE KEYSPACE IF NOT EXISTS {keyspace}
        WITH replication = {{
            'class': 'NetworkTopologyStrategy', 'replication_factor': 1
        }}
        """
    )
    session.set_keyspace(keyspace)


def drop_table(session: Any, keyspace: str, table_name: str) -> None:
    # DDL identifiers are fixed constants; do not pass user-controlled values here.
    session.execute(f"DROP TABLE IF EXISTS {keyspace}.{table_name}")


def print_results(query: str, results: list[tuple[Any, float]]) -> None:
    print(f"\nQuery: {query}")
    if not results:
        print("No documents returned.")
        return

    print("\nRetrieved documents:")
    for index, (document, score) in enumerate(results, start=1):
        document_id = document.metadata.get("id", "unknown")
        print(f"\n{index}. id={document_id} score={score:.6f}")
        print(document.page_content)


def run(args: argparse.Namespace) -> None:
    if not args.query.strip():
        raise RuntimeError("--query must not be empty")

    config = load_config(SCRIPT_DIR / ".env")
    rag = import_dependency("scylla_langchain", "langchain-community")

    cluster = None
    try:
        print("Connecting to ScyllaDB...")
        cluster, session = connect_to_scylla(config)
        create_keyspace(session, rag.KEYSPACE)

        if args.reset:
            print(
                f"Dropping demo table {rag.KEYSPACE}.{rag.TABLE_NAME} if it exists..."
            )
            drop_table(session, rag.KEYSPACE, rag.TABLE_NAME)

        results = rag.populate_and_search(session, args.query, args.k)
        print_results(args.query, results)
    finally:
        if cluster is not None:
            cluster.shutdown()


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        run(args)
    except KeyboardInterrupt:
        print("interrupted", file=sys.stderr)
        return 130
    except Exception as error:
        print(f"error: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
