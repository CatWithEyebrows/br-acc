import logging
from typing import Any

from neo4j import Driver

logger = logging.getLogger(__name__)


class Neo4jBatchLoader:
    """Bulk loader using UNWIND for efficient Neo4j writes."""

    def __init__(self, driver: Driver, batch_size: int = 10_000) -> None:
        self.driver = driver
        self.batch_size = batch_size
        self._total_written = 0

    def _run_batches(self, query: str, rows: list[dict[str, Any]]) -> int:
        total = 0
        for i in range(0, len(rows), self.batch_size):
            batch = rows[i : i + self.batch_size]
            with self.driver.session() as session:
                session.run(query, {"rows": batch})
            total += len(batch)
            self._total_written += len(batch)
        if total >= 10_000:
            logger.info("  Batch written: %d rows (cumulative: %d)", total, self._total_written)
        return total

    def load_nodes(
        self,
        label: str,
        rows: list[dict[str, Any]],
        key_field: str,
    ) -> int:
        rows = [r for r in rows if r.get(key_field)]
        all_keys: set[str] = set()
        for r in rows:
            all_keys.update(r.keys())
        all_keys.discard(key_field)
        props = ", ".join(
            f"n.{k} = row.{k}" for k in sorted(all_keys)
        ) if rows else ""
        set_clause = f"SET {props}" if props else ""
        query = (
            f"UNWIND $rows AS row "
            f"MERGE (n:{label} {{{key_field}: row.{key_field}}}) "
            f"{set_clause}"
        )
        return self._run_batches(query, rows)

    def load_relationships(
        self,
        rel_type: str,
        rows: list[dict[str, Any]],
        source_label: str,
        source_key: str,
        target_label: str,
        target_key: str,
        properties: list[str] | None = None,
    ) -> int:
        rows = [r for r in rows if r.get("source_key") and r.get("target_key")]
        props = ""
        if properties:
            prop_str = ", ".join(f"r.{p} = row.{p}" for p in properties)
            props = f"SET {prop_str}"
        query = (
            f"UNWIND $rows AS row "
            f"MATCH (a:{source_label} {{{source_key}: row.source_key}}) "
            f"MATCH (b:{target_label} {{{target_key}: row.target_key}}) "
            f"MERGE (a)-[r:{rel_type}]->(b) "
            f"{props}"
        )
        return self._run_batches(query, rows)

    def run_query(self, query: str, rows: list[dict[str, Any]]) -> int:
        return self._run_batches(query, rows)
