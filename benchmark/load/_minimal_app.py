"""Tiny FastAPI app for uvicorn benchmarking with different loop+http combos."""
from fastapi import FastAPI

app = FastAPI()

@app.get("/ping")
async def ping():
    return {"ok": True}


@app.get("/work")
async def work():
    # tiny async work to simulate an awaiting handler
    import asyncio
    await asyncio.sleep(0)
    return {"x": list(range(20))}
