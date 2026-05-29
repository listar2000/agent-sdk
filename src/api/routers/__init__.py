"""FastAPI ``APIRouter`` modules.

Routes are being moved out of the ``api.server`` monolith into focused
routers here, included via ``app.include_router`` (refactor slice 6+).
Routers that need shared business logic import it from ``api.services`` so
there is no ``server`` <-> ``routers`` import cycle.
"""
