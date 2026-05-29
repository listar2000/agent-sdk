"""Service layer for the API server.

Thin FastAPI routers in ``api.server`` delegate business logic here so the
HTTP edge stays a parse-validate-delegate shell. Extracted incrementally
from the former ``server.py`` god module — see the refactor plan.
"""
