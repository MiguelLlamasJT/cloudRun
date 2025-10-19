from fastapi import FastAPI, Request, BackgroundTasks
import os
from app.slack_events import handler

app = FastAPI()


@app.post("/slack/events")
async def slack_events(req: Request, background_tasks: BackgroundTasks):
    body = await req.json()
    background_tasks.add_task(handler, body)
    return {"ok": True}



