# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

### Linting and Formatting
```bash
# Run linting with automatic fixes
ruff check --fix .

# Format code
ruff format .

# Check code without fixing
ruff check .
```

### Type Checking
```bash
# Run type checking
mypy redash_pandas/
```

### Development Environment
```bash
# Install development dependencies
pip install -e ".[dev]"
```

## Architecture Overview

This is a Python library that provides a simple wrapper around the Redash API for data querying and retrieval. The project follows modern Python packaging practices using pyproject.toml.

### Core Components

1. **Main Class: `Redash`** (redash_pandas/redash.py)
   - Handles API authentication via credentials file or direct API key
   - Uses httpx for HTTP requests (not requests library)
   - Implements three query methods:
     - `query()`: Standard query execution with retry logic
     - `safe_query()`: Paginated queries for large datasets using offset/limit
     - `period_limited_query()`: Time-chunked queries for period-based data
   - Job monitoring with status enum (PENDING, STARTED, SUCCESS, FAILURE, CANCELLED)
   - Returns pandas DataFrames

2. **Configuration Methods**:
   - JSON credentials file: `{"endpoint": "https://...", "apikey": "..."}`
   - Direct instantiation: `Redash(apikey="...", endpoint="...")`

### Key Development Notes

- Python 3.12+ required
- Dependencies: httpx (>=0.28.1), pandas (>=2.3.0)
- Extensive Ruff configuration with many rules enabled
- No test suite currently exists in the project
- Example usage provided in example.py (Japanese comments)
- Clean code practices: proper error handling, logging, and type hints