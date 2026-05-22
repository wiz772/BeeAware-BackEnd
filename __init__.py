"""Backend package initializer for BeeAware.

This file ensures `backend` is importable as a Python package when
starting the API from the project root or using uvicorn.
"""

__all__ = ["API", "DB", "stream_ingestion"]
