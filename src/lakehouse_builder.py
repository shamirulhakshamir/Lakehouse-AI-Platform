"""
Lakehouse Builder — Medallion Architecture (Bronze / Silver / Gold)

Simulates a Databricks-style lakehouse pipeline that processes data through
three layers: raw ingestion (Bronze), cleansed/standardized (Silver), and
business-aggregated (Gold).
"""

from __future__ import annotations

import copy
import hashlib
import re
from datetime import datetime, timezone
from typing import Any


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# ---------------------------------------------------------------------------
# Bronze Layer — Raw Ingestion
# ---------------------------------------------------------------------------

class BronzeLayer:
    """Ingests raw records, tags them with metadata, and detects schema."""

    def __init__(self) -> None:
        self.records: list[dict[str, Any]] = []
        self.schema: dict[str, str] = {}
        self.ingestion_log: list[dict[str, Any]] = []

    def ingest(self, raw_records: list[dict[str, Any]], source: str = "unknown") -> int:
        """Ingest a batch of raw records. Returns count ingested."""
        if not raw_records:
            return 0

        tagged: list[dict[str, Any]] = []
        for record in raw_records:
            enriched = {
                "_bronze_id": hashlib.md5(str(record).encode()).hexdigest(),
                "_ingested_at": _now_iso(),
                "_source": source,
                **record,
            }
            tagged.append(enriched)

        self.records.extend(tagged)
        self._detect_schema(raw_records[0])

        self.ingestion_log.append({
            "source": source,
            "count": len(tagged),
            "timestamp": _now_iso(),
        })
        return len(tagged)

    def _detect_schema(self, sample: dict[str, Any]) -> None:
        """Infer column types from a sample record."""
        for key, value in sample.items():
            self.schema[key] = type(value).__name__

    def get_records(self) -> list[dict[str, Any]]:
        return list(self.records)

    def get_schema(self) -> dict[str, str]:
        return dict(self.schema)


# ---------------------------------------------------------------------------
# Silver Layer — Cleansing & Standardization
# ---------------------------------------------------------------------------

class SilverLayer:
    """Cleanses, deduplicates, and standardizes Bronze data."""

    def __init__(self) -> None:
        self.records: list[dict[str, Any]] = []
        self.rejected: list[dict[str, Any]] = []

    def process(
        self,
        bronze_records: list[dict[str, Any]],
        dedup_key: str | None = None,
        required_fields: list[str] | None = None,
        type_casts: dict[str, type] | None = None,
    ) -> int:
        """Process bronze records into silver. Returns count accepted."""
        cleaned: list[dict[str, Any]] = []
        seen_keys: set[str] = set()

        for record in bronze_records:
            rec = copy.deepcopy(record)

            # Null / required-field check
            if required_fields:
                missing = [f for f in required_fields if f not in rec or rec[f] is None]
                if missing:
                    rec["_rejection_reason"] = f"missing fields: {missing}"
                    self.rejected.append(rec)
                    continue

            # Deduplication
            if dedup_key and dedup_key in rec:
                key_val = str(rec[dedup_key])
                if key_val in seen_keys:
                    rec["_rejection_reason"] = f"duplicate on {dedup_key}"
                    self.rejected.append(rec)
                    continue
                seen_keys.add(key_val)

            # Type casting
            if type_casts:
                for field, target_type in type_casts.items():
                    if field in rec and rec[field] is not None:
                        try:
                            rec[field] = target_type(rec[field])
                        except (ValueError, TypeError):
                            pass  # keep original

            # Strip whitespace from strings
            for k, v in rec.items():
                if isinstance(v, str) and not k.startswith("_"):
                    rec[k] = v.strip()

            rec["_silver_processed_at"] = _now_iso()
            cleaned.append(rec)

        self.records.extend(cleaned)
        return len(cleaned)

    def get_records(self) -> list[dict[str, Any]]:
        return list(self.records)

    def get_rejected(self) -> list[dict[str, Any]]:
        return list(self.rejected)


# ---------------------------------------------------------------------------
# Gold Layer — Business Aggregations
# ---------------------------------------------------------------------------

class GoldLayer:
    """Produces business-level aggregations from Silver data."""

    def __init__(self) -> None:
        self.aggregations: dict[str, list[dict[str, Any]]] = {}

    def aggregate(
        self,
        silver_records: list[dict[str, Any]],
        group_by: str,
        metric_field: str,
        agg_name: str = "default",
    ) -> dict[str, dict[str, Any]]:
        """Group records by a field and compute sum/count/avg on a metric."""
        groups: dict[str, list[float]] = {}

        for rec in silver_records:
            key = str(rec.get(group_by, "unknown"))
            val = rec.get(metric_field)
            if val is not None:
                try:
                    val = float(val)
                except (ValueError, TypeError):
                    continue
                groups.setdefault(key, []).append(val)

        result: dict[str, dict[str, Any]] = {}
        for key, values in groups.items():
            result[key] = {
                "count": len(values),
                "sum": round(sum(values), 2),
                "avg": round(sum(values) / len(values), 2) if values else 0,
                "min": round(min(values), 2),
                "max": round(max(values), 2),
            }

        self.aggregations[agg_name] = [
            {group_by: k, **v} for k, v in result.items()
        ]
        return result

    def get_aggregation(self, agg_name: str = "default") -> list[dict[str, Any]]:
        return self.aggregations.get(agg_name, [])


# ---------------------------------------------------------------------------
# Pipeline Orchestrator
# ---------------------------------------------------------------------------

class LakehousePipeline:
    """End-to-end Bronze -> Silver -> Gold pipeline."""

    def __init__(self) -> None:
        self.bronze = BronzeLayer()
        self.silver = SilverLayer()
        self.gold = GoldLayer()

    def run(
        self,
        raw_data: list[dict[str, Any]],
        source: str = "batch",
        dedup_key: str | None = None,
        required_fields: list[str] | None = None,
        type_casts: dict[str, type] | None = None,
        group_by: str | None = None,
        metric_field: str | None = None,
    ) -> dict[str, Any]:
        """Execute the full pipeline and return summary stats."""
        bronze_count = self.bronze.ingest(raw_data, source=source)
        silver_count = self.silver.process(
            self.bronze.get_records(),
            dedup_key=dedup_key,
            required_fields=required_fields,
            type_casts=type_casts,
        )

        gold_result = {}
        if group_by and metric_field:
            gold_result = self.gold.aggregate(
                self.silver.get_records(),
                group_by=group_by,
                metric_field=metric_field,
            )

        return {
            "bronze_ingested": bronze_count,
            "silver_accepted": silver_count,
            "silver_rejected": len(self.silver.get_rejected()),
            "gold_groups": len(gold_result),
        }
