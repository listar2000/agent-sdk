"""Agent CRUD routes (config only, no sandbox).

Imports shared helpers from ``api.deps`` / ``api.services.config_parse`` and
data access from ``api.db`` / ``api.models`` — never ``api.server`` — so this
router is cycle-free and includes cleanly via ``app.include_router``. Behavior
identical to the former inline ``@app.*("/agents*")`` handlers.
"""
from __future__ import annotations

import uuid

from fastapi import APIRouter, HTTPException, Request

from api.db import delete_agent, list_agents, upsert_agent
from api.deps import _json_body, _require_agent
from api.models import AgentConfig, AgentRecord
from api.services.config_parse import _AGENT_REJECTED_KEYS, _merge_top_level_config

router = APIRouter()


@router.post("/agents")
async def create_agent(request: Request):
    data = await _json_body(request)
    agent_id = str(uuid.uuid4())
    name = data.get("name")
    config_data = data.get("config", {})
    _merge_top_level_config(data, config_data)
    # cwd / env / dockerfile / shared_mounts moved off AgentConfig — reject
    # them at the boundary so stale clients get a clear 400 instead of
    # silently-discarded fields.
    for k in _AGENT_REJECTED_KEYS:
        if k in data or k in config_data:
            raise HTTPException(
                400,
                f"'{k}' no longer belongs to agent config. "
                "cwd → session; env → session; dockerfile + shared_mounts → sandbox. "
                "Set these on POST /sessions or /sessions instead.",
            )
    config = AgentConfig.from_dict(config_data)
    await upsert_agent(AgentRecord(id=agent_id, name=name, config=config))
    return {"id": agent_id, "name": name, "config": config.to_dict()}


@router.get("/agents")
async def list_agents_route():
    agents = await list_agents()
    return [{"id": a.id, "name": a.name, "config": a.config.to_dict()} for a in agents]


@router.get("/agents/{agent_id}")
async def get_agent_route(agent_id: str):
    record = await _require_agent(agent_id)
    return {"id": record.id, "name": record.name, "config": record.config.to_dict()}


@router.delete("/agents/{agent_id}")
async def delete_agent_route(agent_id: str):
    await _require_agent(agent_id)
    await delete_agent(agent_id)
    return {"status": "deleted"}
