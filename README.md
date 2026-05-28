# Lakehouse-AI-Platform

A personal data platform project implementing medallion architecture (Bronze/Silver/Gold), column-level lineage tracking, data governance engine, and DAG-based notebook orchestration built to demonstrate production-grade data platform engineering.

## Architecture

This project implements three core components of a modern data platform:

### 1. Lakehouse Builder (`src/lakehouse_builder.py`)
Implements the medallion architecture (Bronze/Silver/Gold) for structured data processing:
- **Bronze layer**: Raw data ingestion with schema detection and metadata tagging
- **Silver layer**: Data cleansing, deduplication, type casting, and standardization
- **Gold layer**: Business-level aggregations and analytics-ready datasets

### 2. Notebook Runner (`src/notebook_runner.py`)
Orchestration engine for managing notebook execution workflows:
- DAG-based dependency resolution with topological sorting
- Parallel execution support with configurable concurrency
- Retry logic with exponential backoff for transient failures
- Execution audit logging and status tracking

### 3. Governance Engine (`src/governance_engine.py`)
Data governance and lineage tracking system:
- Column-level lineage graph tracking transformations across layers
- Data quality rule engine with configurable validation checks (nulls, ranges, regex, uniqueness)
- Access policy management with role-based permissions
- Audit trail for all governance actions

## Project Structure

```
lakehouse-ai-platform/
├── README.md
├── requirements.txt
├── src/
│   ├── lakehouse_builder.py
│   ├── notebook_runner.py
│   └── governance_engine.py
└── tests/
    ├── test_lakehouse_builder.py
    ├── test_notebook_runner.py
    └── test_governance_engine.py
```

## Setup

```bash
pip install -r requirements.txt
```

## Running Tests

```bash
pytest tests/ -v
```

## Technologies

- Python 3.9+
- Designed for Databricks / Apache Spark (runs standalone for demonstration)
- Delta Lake medallion architecture patterns
- DAG-based workflow orchestration
- pytest (comprehensive unit test coverage)

## Author

Shamirul Hak Surbudeen
[GitHub](https://github.com/shamirulhakshamir)
