"""Tests for governance_engine.py"""

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.governance_engine import (
    LineageGraph,
    LineageNode,
    QualityEngine,
    QualityRule,
    QualityRuleType,
    AccessPolicy,
    GovernanceEngine,
)


# ---------------------------------------------------------------------------
# Lineage Graph Tests
# ---------------------------------------------------------------------------

class TestLineageGraph:
    def test_add_nodes_and_edges(self):
        g = LineageGraph()
        src = g.add_node("raw_txn", "amount", "bronze")
        tgt = g.add_node("clean_txn", "amount_eur", "silver")
        g.add_edge(src, tgt, transformation="currency_convert")

        lineage = g.get_full_lineage()
        assert len(lineage["nodes"]) == 2
        assert len(lineage["edges"]) == 1
        assert lineage["edges"][0]["transformation"] == "currency_convert"

    def test_upstream_lineage(self):
        g = LineageGraph()
        bronze = g.add_node("raw", "col_a", "bronze")
        silver = g.add_node("clean", "col_a", "silver")
        gold = g.add_node("agg", "col_a_sum", "gold")
        g.add_edge(bronze, silver, "clean")
        g.add_edge(silver, gold, "sum")

        upstream = g.get_upstream("gold.agg.col_a_sum")
        assert len(upstream) == 2
        sources = {e["source"] for e in upstream}
        assert "silver.clean.col_a" in sources
        assert "bronze.raw.col_a" in sources

    def test_downstream_lineage(self):
        g = LineageGraph()
        bronze = g.add_node("raw", "col_a", "bronze")
        silver = g.add_node("clean", "col_a", "silver")
        gold = g.add_node("agg", "col_a_sum", "gold")
        g.add_edge(bronze, silver, "clean")
        g.add_edge(silver, gold, "sum")

        downstream = g.get_downstream("bronze.raw.col_a")
        assert len(downstream) == 2
        targets = {e["target"] for e in downstream}
        assert "silver.clean.col_a" in targets
        assert "gold.agg.col_a_sum" in targets

    def test_node_equality(self):
        n1 = LineageNode("tbl", "col", "bronze")
        n2 = LineageNode("tbl", "col", "bronze")
        assert n1 == n2
        assert hash(n1) == hash(n2)


# ---------------------------------------------------------------------------
# Quality Engine Tests
# ---------------------------------------------------------------------------

class TestQualityEngine:
    def test_not_null_rule(self):
        engine = QualityEngine()
        engine.add_rule(QualityRule("email_required", "email", QualityRuleType.NOT_NULL))
        records = [
            {"email": "a@test.com"},
            {"email": None},
            {"email": "  "},
            {"email": "b@test.com"},
        ]
        result = engine.validate(records)
        assert not result["passed"]
        detail = result["details"][0]
        assert detail["violation_count"] == 2

    def test_range_rule(self):
        engine = QualityEngine()
        engine.add_rule(QualityRule("amount_range", "amount", QualityRuleType.RANGE,
                                     params={"min": 0, "max": 1000}))
        records = [
            {"amount": 500},
            {"amount": -10},
            {"amount": 1500},
            {"amount": 0},
        ]
        result = engine.validate(records)
        assert not result["passed"]
        detail = result["details"][0]
        assert detail["violation_count"] == 2

    def test_regex_rule(self):
        engine = QualityEngine()
        engine.add_rule(QualityRule("email_format", "email", QualityRuleType.REGEX,
                                     params={"pattern": r"^[\w.+-]+@[\w-]+\.[\w.]+$"}))
        records = [
            {"email": "valid@test.com"},
            {"email": "invalid"},
            {"email": "also.valid@foo.co.uk"},
        ]
        result = engine.validate(records)
        assert not result["passed"]
        detail = result["details"][0]
        assert detail["violation_count"] == 1

    def test_unique_rule(self):
        engine = QualityEngine()
        engine.add_rule(QualityRule("unique_id", "id", QualityRuleType.UNIQUE))
        records = [{"id": 1}, {"id": 2}, {"id": 1}, {"id": 3}]
        result = engine.validate(records)
        assert not result["passed"]
        detail = result["details"][0]
        assert detail["violation_count"] == 1

    def test_all_rules_pass(self):
        engine = QualityEngine()
        engine.add_rule(QualityRule("nn", "name", QualityRuleType.NOT_NULL))
        engine.add_rule(QualityRule("uniq", "id", QualityRuleType.UNIQUE))
        records = [{"id": 1, "name": "A"}, {"id": 2, "name": "B"}]
        result = engine.validate(records)
        assert result["passed"]
        assert result["rules_checked"] == 2


# ---------------------------------------------------------------------------
# Access Policy Tests
# ---------------------------------------------------------------------------

class TestAccessPolicy:
    def test_grant_and_check(self):
        ap = AccessPolicy()
        ap.grant("gold.revenue", "analyst", ["SELECT"])
        assert ap.check_access("gold.revenue", "analyst", "SELECT")
        assert not ap.check_access("gold.revenue", "analyst", "DELETE")

    def test_revoke(self):
        ap = AccessPolicy()
        ap.grant("gold.revenue", "analyst", ["SELECT", "INSERT"])
        ap.revoke("gold.revenue", "analyst", ["INSERT"])
        assert ap.check_access("gold.revenue", "analyst", "SELECT")
        assert not ap.check_access("gold.revenue", "analyst", "INSERT")

    def test_get_policies(self):
        ap = AccessPolicy()
        ap.grant("silver.txn", "engineer", ["SELECT", "INSERT"])
        ap.grant("silver.txn", "analyst", ["SELECT"])
        policies = ap.get_policies("silver.txn")
        assert "engineer" in policies
        assert "analyst" in policies
        assert "INSERT" in policies["engineer"]
        assert "INSERT" not in policies["analyst"]

    def test_audit_trail(self):
        ap = AccessPolicy()
        ap.grant("tbl", "role", ["SELECT"])
        ap.revoke("tbl", "role", ["SELECT"])
        trail = ap.get_audit_trail()
        assert len(trail) == 2
        assert trail[0]["action"] == "GRANT"
        assert trail[1]["action"] == "REVOKE"

    def test_no_duplicate_permissions(self):
        ap = AccessPolicy()
        ap.grant("tbl", "role", ["SELECT"])
        ap.grant("tbl", "role", ["SELECT"])
        policies = ap.get_policies("tbl")
        assert policies["role"].count("SELECT") == 1


# ---------------------------------------------------------------------------
# Governance Engine (Unified) Tests
# ---------------------------------------------------------------------------

class TestGovernanceEngine:
    def test_register_transformation(self):
        ge = GovernanceEngine()
        ge.register_transformation(
            "raw_txn", "amount", "bronze",
            "clean_txn", "amount_eur", "silver",
            "currency_convert",
        )
        lineage = ge.lineage.get_full_lineage()
        assert len(lineage["nodes"]) == 2
        assert len(lineage["edges"]) == 1

    def test_quality_validation(self):
        ge = GovernanceEngine()
        ge.add_quality_rule("nn_check", "name", QualityRuleType.NOT_NULL)
        result = ge.validate_data([{"name": "Alice"}, {"name": None}])
        assert not result["passed"]

    def test_access_control(self):
        ge = GovernanceEngine()
        ge.grant_access("gold.dashboard", "viewer", ["SELECT"])
        assert ge.check_access("gold.dashboard", "viewer", "SELECT")
        assert not ge.check_access("gold.dashboard", "viewer", "DELETE")
