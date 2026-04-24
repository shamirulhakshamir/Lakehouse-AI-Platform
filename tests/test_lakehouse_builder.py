"""Tests for lakehouse_builder.py"""

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.lakehouse_builder import BronzeLayer, SilverLayer, GoldLayer, LakehousePipeline


# ---------------------------------------------------------------------------
# Bronze Layer Tests
# ---------------------------------------------------------------------------

class TestBronzeLayer:
    def test_ingest_records(self):
        bronze = BronzeLayer()
        data = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
        count = bronze.ingest(data, source="test")
        assert count == 2
        assert len(bronze.get_records()) == 2

    def test_ingest_empty(self):
        bronze = BronzeLayer()
        count = bronze.ingest([], source="test")
        assert count == 0

    def test_metadata_tagging(self):
        bronze = BronzeLayer()
        bronze.ingest([{"id": 1}], source="api")
        rec = bronze.get_records()[0]
        assert "_bronze_id" in rec
        assert "_ingested_at" in rec
        assert rec["_source"] == "api"

    def test_schema_detection(self):
        bronze = BronzeLayer()
        bronze.ingest([{"id": 1, "name": "Alice", "amount": 9.99}])
        schema = bronze.get_schema()
        assert schema["id"] == "int"
        assert schema["name"] == "str"
        assert schema["amount"] == "float"

    def test_ingestion_log(self):
        bronze = BronzeLayer()
        bronze.ingest([{"id": 1}], source="batch_1")
        bronze.ingest([{"id": 2}, {"id": 3}], source="batch_2")
        assert len(bronze.ingestion_log) == 2
        assert bronze.ingestion_log[0]["count"] == 1
        assert bronze.ingestion_log[1]["count"] == 2


# ---------------------------------------------------------------------------
# Silver Layer Tests
# ---------------------------------------------------------------------------

class TestSilverLayer:
    def _bronze_records(self):
        bronze = BronzeLayer()
        bronze.ingest([
            {"id": 1, "name": "Alice", "amount": "100"},
            {"id": 2, "name": "Bob", "amount": "200"},
            {"id": 1, "name": "Alice", "amount": "100"},  # duplicate
        ], source="test")
        return bronze.get_records()

    def test_deduplication(self):
        silver = SilverLayer()
        records = self._bronze_records()
        count = silver.process(records, dedup_key="id")
        assert count == 2
        assert len(silver.get_rejected()) == 1

    def test_required_fields(self):
        silver = SilverLayer()
        records = [{"name": "Alice"}, {"name": "Bob", "email": "bob@test.com"}]
        count = silver.process(records, required_fields=["name", "email"])
        assert count == 1
        assert len(silver.get_rejected()) == 1

    def test_type_casting(self):
        silver = SilverLayer()
        records = [{"id": "1", "amount": "99.5"}]
        silver.process(records, type_casts={"id": int, "amount": float})
        rec = silver.get_records()[0]
        assert rec["id"] == 1
        assert rec["amount"] == 99.5

    def test_whitespace_stripping(self):
        silver = SilverLayer()
        records = [{"name": "  Alice  ", "city": " Amsterdam "}]
        silver.process(records)
        rec = silver.get_records()[0]
        assert rec["name"] == "Alice"
        assert rec["city"] == "Amsterdam"

    def test_silver_timestamp(self):
        silver = SilverLayer()
        silver.process([{"id": 1}])
        assert "_silver_processed_at" in silver.get_records()[0]


# ---------------------------------------------------------------------------
# Gold Layer Tests
# ---------------------------------------------------------------------------

class TestGoldLayer:
    def test_aggregation(self):
        gold = GoldLayer()
        records = [
            {"region": "EU", "revenue": 100},
            {"region": "EU", "revenue": 200},
            {"region": "US", "revenue": 150},
        ]
        result = gold.aggregate(records, group_by="region", metric_field="revenue")
        assert "EU" in result
        assert result["EU"]["sum"] == 300
        assert result["EU"]["avg"] == 150
        assert result["EU"]["count"] == 2
        assert result["US"]["sum"] == 150

    def test_named_aggregation(self):
        gold = GoldLayer()
        records = [{"cat": "A", "val": 10}, {"cat": "A", "val": 20}]
        gold.aggregate(records, group_by="cat", metric_field="val", agg_name="test_agg")
        agg = gold.get_aggregation("test_agg")
        assert len(agg) == 1
        assert agg[0]["cat"] == "A"

    def test_non_numeric_skipped(self):
        gold = GoldLayer()
        records = [
            {"cat": "A", "val": 10},
            {"cat": "A", "val": "not_a_number"},
        ]
        result = gold.aggregate(records, group_by="cat", metric_field="val")
        assert result["A"]["count"] == 1


# ---------------------------------------------------------------------------
# Pipeline Tests
# ---------------------------------------------------------------------------

class TestLakehousePipeline:
    def test_full_pipeline(self):
        pipeline = LakehousePipeline()
        data = [
            {"merchant_id": "M001", "region": "EU", "amount": "500"},
            {"merchant_id": "M002", "region": "EU", "amount": "300"},
            {"merchant_id": "M003", "region": "US", "amount": "700"},
            {"merchant_id": "M001", "region": "EU", "amount": "500"},  # dup
        ]
        summary = pipeline.run(
            raw_data=data,
            source="transactions",
            dedup_key="merchant_id",
            type_casts={"amount": float},
            group_by="region",
            metric_field="amount",
        )
        assert summary["bronze_ingested"] == 4
        assert summary["silver_accepted"] == 3
        assert summary["silver_rejected"] == 1
        assert summary["gold_groups"] == 2

    def test_pipeline_no_gold(self):
        pipeline = LakehousePipeline()
        data = [{"id": 1, "val": 10}]
        summary = pipeline.run(raw_data=data, source="test")
        assert summary["bronze_ingested"] == 1
        assert summary["gold_groups"] == 0
