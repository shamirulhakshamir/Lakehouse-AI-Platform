"""
Notebook Runner — DAG-based Notebook Orchestration Engine

Simulates Databricks notebook workflow orchestration with dependency
resolution, parallel execution support, retry logic, and audit logging.
"""

from __future__ import annotations

import time
import logging
from collections import deque
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable

logger = logging.getLogger(__name__)


class NotebookStatus(Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"


class Notebook:
    """Represents a single notebook in the execution DAG."""

    def __init__(
        self,
        name: str,
        execute_fn: Callable[..., Any] | None = None,
        depends_on: list[str] | None = None,
        max_retries: int = 0,
        timeout_seconds: float = 60.0,
    ) -> None:
        self.name = name
        self.execute_fn = execute_fn or (lambda: {"status": "ok"})
        self.depends_on = depends_on or []
        self.max_retries = max_retries
        self.timeout_seconds = timeout_seconds
        self.status = NotebookStatus.PENDING
        self.result: Any = None
        self.error: str | None = None
        self.attempts = 0
        self.started_at: str | None = None
        self.completed_at: str | None = None
        self.duration_ms: float | None = None


class DAGValidationError(Exception):
    """Raised when the DAG has cycles or missing dependencies."""
    pass


class NotebookRunner:
    """Orchestrates notebook execution with DAG resolution."""

    def __init__(self) -> None:
        self.notebooks: dict[str, Notebook] = {}
        self.execution_log: list[dict[str, Any]] = []

    def register(self, notebook: Notebook) -> None:
        """Register a notebook in the DAG."""
        self.notebooks[notebook.name] = notebook

    def validate_dag(self) -> list[str]:
        """Validate the DAG: check for cycles and missing deps. Returns topo order."""
        # Check for missing dependencies
        for name, nb in self.notebooks.items():
            for dep in nb.depends_on:
                if dep not in self.notebooks:
                    raise DAGValidationError(
                        f"Notebook '{name}' depends on '{dep}' which is not registered"
                    )

        # Topological sort (Kahn's algorithm)
        in_degree: dict[str, int] = {n: 0 for n in self.notebooks}
        adjacency: dict[str, list[str]] = {n: [] for n in self.notebooks}

        for name, nb in self.notebooks.items():
            for dep in nb.depends_on:
                adjacency[dep].append(name)
                in_degree[name] += 1

        queue: deque[str] = deque(
            [n for n, d in in_degree.items() if d == 0]
        )
        topo_order: list[str] = []

        while queue:
            node = queue.popleft()
            topo_order.append(node)
            for neighbor in adjacency[node]:
                in_degree[neighbor] -= 1
                if in_degree[neighbor] == 0:
                    queue.append(neighbor)

        if len(topo_order) != len(self.notebooks):
            raise DAGValidationError("Cycle detected in notebook DAG")

        return topo_order

    def _execute_notebook(self, notebook: Notebook) -> bool:
        """Execute a single notebook with retry logic. Returns True on success."""
        notebook.status = NotebookStatus.RUNNING
        notebook.started_at = datetime.now(timezone.utc).isoformat()

        for attempt in range(notebook.max_retries + 1):
            notebook.attempts = attempt + 1
            try:
                start = time.monotonic()
                notebook.result = notebook.execute_fn()
                elapsed = (time.monotonic() - start) * 1000
                notebook.duration_ms = round(elapsed, 2)
                notebook.status = NotebookStatus.SUCCESS
                notebook.completed_at = datetime.now(timezone.utc).isoformat()
                notebook.error = None

                self._log(notebook, "SUCCESS", attempt + 1)
                return True

            except Exception as e:
                notebook.error = str(e)
                backoff = min(2 ** attempt * 0.01, 1.0)  # small backoff for tests
                if attempt < notebook.max_retries:
                    logger.info(
                        f"Notebook '{notebook.name}' attempt {attempt + 1} failed, "
                        f"retrying in {backoff:.2f}s: {e}"
                    )
                    time.sleep(backoff)

        notebook.status = NotebookStatus.FAILED
        notebook.completed_at = datetime.now(timezone.utc).isoformat()
        self._log(notebook, "FAILED", notebook.attempts)
        return False

    def run_all(self, skip_on_upstream_failure: bool = True) -> dict[str, NotebookStatus]:
        """Execute all notebooks in topological order. Returns status map."""
        topo_order = self.validate_dag()
        results: dict[str, NotebookStatus] = {}

        for name in topo_order:
            nb = self.notebooks[name]

            # Check upstream status
            if skip_on_upstream_failure:
                upstream_failed = any(
                    results.get(dep) in (NotebookStatus.FAILED, NotebookStatus.SKIPPED)
                    for dep in nb.depends_on
                )
                if upstream_failed:
                    nb.status = NotebookStatus.SKIPPED
                    nb.error = "Skipped due to upstream failure"
                    self._log(nb, "SKIPPED", 0)
                    results[name] = NotebookStatus.SKIPPED
                    continue

            success = self._execute_notebook(nb)
            results[name] = nb.status

        return results

    def get_execution_summary(self) -> dict[str, Any]:
        """Return a summary of the last execution."""
        statuses = {nb.name: nb.status.value for nb in self.notebooks.values()}
        return {
            "total": len(self.notebooks),
            "succeeded": sum(1 for s in statuses.values() if s == "SUCCESS"),
            "failed": sum(1 for s in statuses.values() if s == "FAILED"),
            "skipped": sum(1 for s in statuses.values() if s == "SKIPPED"),
            "pending": sum(1 for s in statuses.values() if s == "PENDING"),
            "notebooks": statuses,
        }

    def _log(self, nb: Notebook, status: str, attempts: int) -> None:
        self.execution_log.append({
            "notebook": nb.name,
            "status": status,
            "attempts": attempts,
            "error": nb.error,
            "started_at": nb.started_at,
            "completed_at": nb.completed_at,
            "duration_ms": nb.duration_ms,
        })

    def get_execution_log(self) -> list[dict[str, Any]]:
        return list(self.execution_log)

    def get_ready_notebooks(self) -> list[str]:
        """Return notebooks whose dependencies are all satisfied (SUCCESS)."""
        ready = []
        for name, nb in self.notebooks.items():
            if nb.status != NotebookStatus.PENDING:
                continue
            deps_met = all(
                self.notebooks[dep].status == NotebookStatus.SUCCESS
                for dep in nb.depends_on
            )
            if deps_met:
                ready.append(name)
        return ready
