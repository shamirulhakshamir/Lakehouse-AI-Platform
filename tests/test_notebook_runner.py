"""Tests for notebook_runner.py"""

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.notebook_runner import Notebook, NotebookRunner, NotebookStatus, DAGValidationError
import pytest


class TestNotebookBasic:
    def test_notebook_defaults(self):
        nb = Notebook(name="test_nb")
        assert nb.name == "test_nb"
        assert nb.status == NotebookStatus.PENDING
        assert nb.depends_on == []
        assert nb.max_retries == 0

    def test_notebook_execution(self):
        nb = Notebook(name="calc", execute_fn=lambda: 42)
        runner = NotebookRunner()
        runner.register(nb)
        results = runner.run_all()
        assert results["calc"] == NotebookStatus.SUCCESS
        assert nb.result == 42


class TestDAGValidation:
    def test_valid_dag(self):
        runner = NotebookRunner()
        runner.register(Notebook("A"))
        runner.register(Notebook("B", depends_on=["A"]))
        runner.register(Notebook("C", depends_on=["A", "B"]))
        order = runner.validate_dag()
        assert order.index("A") < order.index("B")
        assert order.index("B") < order.index("C")

    def test_missing_dependency(self):
        runner = NotebookRunner()
        runner.register(Notebook("A", depends_on=["missing"]))
        with pytest.raises(DAGValidationError, match="not registered"):
            runner.validate_dag()

    def test_cycle_detection(self):
        runner = NotebookRunner()
        runner.register(Notebook("A", depends_on=["B"]))
        runner.register(Notebook("B", depends_on=["A"]))
        with pytest.raises(DAGValidationError, match="Cycle"):
            runner.validate_dag()


class TestExecutionFlow:
    def test_linear_pipeline(self):
        results_tracker = []

        runner = NotebookRunner()
        runner.register(Notebook("ingest", execute_fn=lambda: results_tracker.append("ingest")))
        runner.register(Notebook("transform", depends_on=["ingest"],
                                 execute_fn=lambda: results_tracker.append("transform")))
        runner.register(Notebook("publish", depends_on=["transform"],
                                 execute_fn=lambda: results_tracker.append("publish")))

        statuses = runner.run_all()
        assert all(s == NotebookStatus.SUCCESS for s in statuses.values())
        assert results_tracker == ["ingest", "transform", "publish"]

    def test_skip_on_upstream_failure(self):
        runner = NotebookRunner()

        def fail():
            raise RuntimeError("Boom")

        runner.register(Notebook("A", execute_fn=fail))
        runner.register(Notebook("B", depends_on=["A"]))

        statuses = runner.run_all(skip_on_upstream_failure=True)
        assert statuses["A"] == NotebookStatus.FAILED
        assert statuses["B"] == NotebookStatus.SKIPPED

    def test_no_skip_on_upstream_failure(self):
        runner = NotebookRunner()

        def fail():
            raise RuntimeError("Boom")

        runner.register(Notebook("A", execute_fn=fail))
        runner.register(Notebook("B", depends_on=["A"]))

        statuses = runner.run_all(skip_on_upstream_failure=False)
        assert statuses["A"] == NotebookStatus.FAILED
        assert statuses["B"] == NotebookStatus.SUCCESS


class TestRetryLogic:
    def test_retry_succeeds_on_second_attempt(self):
        call_count = {"n": 0}

        def flaky():
            call_count["n"] += 1
            if call_count["n"] < 2:
                raise RuntimeError("Transient error")
            return "ok"

        runner = NotebookRunner()
        runner.register(Notebook("flaky", execute_fn=flaky, max_retries=2))
        statuses = runner.run_all()
        assert statuses["flaky"] == NotebookStatus.SUCCESS
        assert runner.notebooks["flaky"].attempts == 2

    def test_retry_exhausted(self):
        def always_fail():
            raise RuntimeError("Permanent error")

        runner = NotebookRunner()
        runner.register(Notebook("doomed", execute_fn=always_fail, max_retries=1))
        statuses = runner.run_all()
        assert statuses["doomed"] == NotebookStatus.FAILED
        assert runner.notebooks["doomed"].attempts == 2


class TestExecutionSummary:
    def test_summary(self):
        runner = NotebookRunner()

        def fail():
            raise RuntimeError("err")

        runner.register(Notebook("A"))
        runner.register(Notebook("B", execute_fn=fail))
        runner.register(Notebook("C", depends_on=["B"]))

        runner.run_all()
        summary = runner.get_execution_summary()
        assert summary["total"] == 3
        assert summary["succeeded"] == 1
        assert summary["failed"] == 1
        assert summary["skipped"] == 1

    def test_execution_log(self):
        runner = NotebookRunner()
        runner.register(Notebook("A"))
        runner.run_all()
        log = runner.get_execution_log()
        assert len(log) == 1
        assert log[0]["notebook"] == "A"
        assert log[0]["status"] == "SUCCESS"


class TestReadyNotebooks:
    def test_get_ready(self):
        runner = NotebookRunner()
        runner.register(Notebook("A"))
        runner.register(Notebook("B", depends_on=["A"]))
        runner.register(Notebook("C"))

        ready = runner.get_ready_notebooks()
        assert "A" in ready
        assert "C" in ready
        assert "B" not in ready
