"""
Governance Engine — Data Governance, Lineage Tracking, and Access Control

Provides column-level lineage graphs, data quality rule validation,
role-based access policies, and a full audit trail for governance actions.
"""

from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# ---------------------------------------------------------------------------
# Column-Level Lineage
# ---------------------------------------------------------------------------

class LineageNode:
    """Represents a column in the lineage graph."""

    def __init__(self, table: str, column: str, layer: str = "bronze") -> None:
        self.table = table
        self.column = column
        self.layer = layer
        self.node_id = f"{layer}.{table}.{column}"

    def __repr__(self) -> str:
        return self.node_id

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, LineageNode):
            return NotImplemented
        return self.node_id == other.node_id

    def __hash__(self) -> int:
        return hash(self.node_id)


class LineageGraph:
    """Tracks column-level lineage across Bronze/Silver/Gold layers."""

    def __init__(self) -> None:
        self.nodes: dict[str, LineageNode] = {}
        self.edges: list[dict[str, str]] = []  # source_id -> target_id + transform

    def add_node(self, table: str, column: str, layer: str = "bronze") -> LineageNode:
        node = LineageNode(table, column, layer)
        self.nodes[node.node_id] = node
        return node

    def add_edge(
        self,
        source: LineageNode,
        target: LineageNode,
        transformation: str = "direct",
    ) -> None:
        """Record a lineage relationship between two columns."""
        # Ensure nodes exist
        if source.node_id not in self.nodes:
            self.nodes[source.node_id] = source
        if target.node_id not in self.nodes:
            self.nodes[target.node_id] = target

        self.edges.append({
            "source": source.node_id,
            "target": target.node_id,
            "transformation": transformation,
            "created_at": _now_iso(),
        })

    def get_upstream(self, node_id: str) -> list[dict[str, str]]:
        """Get all upstream lineage for a given node."""
        upstream = []
        visited: set[str] = set()
        stack = [node_id]

        while stack:
            current = stack.pop()
            for edge in self.edges:
                if edge["target"] == current and edge["source"] not in visited:
                    upstream.append(edge)
                    visited.add(edge["source"])
                    stack.append(edge["source"])

        return upstream

    def get_downstream(self, node_id: str) -> list[dict[str, str]]:
        """Get all downstream lineage for a given node."""
        downstream = []
        visited: set[str] = set()
        stack = [node_id]

        while stack:
            current = stack.pop()
            for edge in self.edges:
                if edge["source"] == current and edge["target"] not in visited:
                    downstream.append(edge)
                    visited.add(edge["target"])
                    stack.append(edge["target"])

        return downstream

    def get_full_lineage(self) -> dict[str, Any]:
        """Return the complete lineage graph as a dict."""
        return {
            "nodes": [
                {"id": n.node_id, "table": n.table, "column": n.column, "layer": n.layer}
                for n in self.nodes.values()
            ],
            "edges": list(self.edges),
        }


# ---------------------------------------------------------------------------
# Data Quality Rule Engine
# ---------------------------------------------------------------------------

class QualityRuleType:
    NOT_NULL = "not_null"
    RANGE = "range"
    REGEX = "regex"
    UNIQUE = "unique"
    CUSTOM = "custom"


class QualityRule:
    """A single data quality validation rule."""

    def __init__(
        self,
        name: str,
        column: str,
        rule_type: str,
        params: dict[str, Any] | None = None,
    ) -> None:
        self.name = name
        self.column = column
        self.rule_type = rule_type
        self.params = params or {}


class QualityEngine:
    """Validates data records against configured quality rules."""

    def __init__(self) -> None:
        self.rules: list[QualityRule] = []

    def add_rule(self, rule: QualityRule) -> None:
        self.rules.append(rule)

    def validate(self, records: list[dict[str, Any]]) -> dict[str, Any]:
        """Run all rules against the records. Returns validation report."""
        results: list[dict[str, Any]] = []

        for rule in self.rules:
            violations = self._check_rule(rule, records)
            results.append({
                "rule": rule.name,
                "column": rule.column,
                "type": rule.rule_type,
                "total_records": len(records),
                "violations": violations,
                "violation_count": len(violations),
                "pass_rate": round(
                    (len(records) - len(violations)) / max(len(records), 1) * 100, 2
                ),
            })

        passed = all(r["violation_count"] == 0 for r in results)
        return {
            "passed": passed,
            "rules_checked": len(results),
            "details": results,
        }

    def _check_rule(
        self, rule: QualityRule, records: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        """Check a single rule and return list of violations."""
        violations: list[dict[str, Any]] = []

        if rule.rule_type == QualityRuleType.NOT_NULL:
            for i, rec in enumerate(records):
                val = rec.get(rule.column)
                if val is None or (isinstance(val, str) and val.strip() == ""):
                    violations.append({"record_index": i, "value": val})

        elif rule.rule_type == QualityRuleType.RANGE:
            min_val = rule.params.get("min")
            max_val = rule.params.get("max")
            for i, rec in enumerate(records):
                val = rec.get(rule.column)
                if val is not None:
                    try:
                        num = float(val)
                        if (min_val is not None and num < min_val) or (
                            max_val is not None and num > max_val
                        ):
                            violations.append({"record_index": i, "value": val})
                    except (ValueError, TypeError):
                        violations.append({"record_index": i, "value": val})

        elif rule.rule_type == QualityRuleType.REGEX:
            pattern = rule.params.get("pattern", "")
            compiled = re.compile(pattern)
            for i, rec in enumerate(records):
                val = rec.get(rule.column)
                if val is not None and not compiled.match(str(val)):
                    violations.append({"record_index": i, "value": val})

        elif rule.rule_type == QualityRuleType.UNIQUE:
            seen: dict[Any, int] = {}
            for i, rec in enumerate(records):
                val = rec.get(rule.column)
                if val in seen:
                    violations.append({"record_index": i, "value": val, "duplicate_of": seen[val]})
                else:
                    seen[val] = i

        return violations


# ---------------------------------------------------------------------------
# Access Policy Management
# ---------------------------------------------------------------------------

class AccessPolicy:
    """Role-based access policy for data assets."""

    def __init__(self) -> None:
        self.policies: dict[str, dict[str, list[str]]] = {}  # asset -> role -> permissions
        self.audit_trail: list[dict[str, Any]] = []

    def grant(self, asset: str, role: str, permissions: list[str]) -> None:
        """Grant permissions on an asset to a role."""
        if asset not in self.policies:
            self.policies[asset] = {}
        if role not in self.policies[asset]:
            self.policies[asset][role] = []

        for perm in permissions:
            if perm not in self.policies[asset][role]:
                self.policies[asset][role].append(perm)

        self._audit("GRANT", asset, role, permissions)

    def revoke(self, asset: str, role: str, permissions: list[str]) -> None:
        """Revoke permissions on an asset from a role."""
        if asset in self.policies and role in self.policies[asset]:
            self.policies[asset][role] = [
                p for p in self.policies[asset][role] if p not in permissions
            ]
        self._audit("REVOKE", asset, role, permissions)

    def check_access(self, asset: str, role: str, permission: str) -> bool:
        """Check if a role has a specific permission on an asset."""
        return permission in self.policies.get(asset, {}).get(role, [])

    def get_policies(self, asset: str) -> dict[str, list[str]]:
        """Get all role-permission mappings for an asset."""
        return dict(self.policies.get(asset, {}))

    def _audit(self, action: str, asset: str, role: str, permissions: list[str]) -> None:
        self.audit_trail.append({
            "action": action,
            "asset": asset,
            "role": role,
            "permissions": permissions,
            "timestamp": _now_iso(),
        })

    def get_audit_trail(self) -> list[dict[str, Any]]:
        return list(self.audit_trail)


# ---------------------------------------------------------------------------
# Governance Engine — Unified Interface
# ---------------------------------------------------------------------------

class GovernanceEngine:
    """Unified governance interface combining lineage, quality, and access control."""

    def __init__(self) -> None:
        self.lineage = LineageGraph()
        self.quality = QualityEngine()
        self.access = AccessPolicy()

    def register_transformation(
        self,
        source_table: str,
        source_column: str,
        source_layer: str,
        target_table: str,
        target_column: str,
        target_layer: str,
        transformation: str = "direct",
    ) -> None:
        """Register a column-level transformation in the lineage graph."""
        src = self.lineage.add_node(source_table, source_column, source_layer)
        tgt = self.lineage.add_node(target_table, target_column, target_layer)
        self.lineage.add_edge(src, tgt, transformation)

    def add_quality_rule(
        self,
        name: str,
        column: str,
        rule_type: str,
        params: dict[str, Any] | None = None,
    ) -> None:
        """Add a data quality rule."""
        self.quality.add_rule(QualityRule(name, column, rule_type, params))

    def validate_data(self, records: list[dict[str, Any]]) -> dict[str, Any]:
        """Run quality validation on records."""
        return self.quality.validate(records)

    def grant_access(self, asset: str, role: str, permissions: list[str]) -> None:
        self.access.grant(asset, role, permissions)

    def check_access(self, asset: str, role: str, permission: str) -> bool:
        return self.access.check_access(asset, role, permission)
