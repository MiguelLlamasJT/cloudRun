from fastapi import FastAPI, Request, BackgroundTasks
import os
from app.slack_events import handler

app = FastAPI()


@app.post("/slack/events")
async def slack_events(req: Request, background_tasks: BackgroundTasks):
    body = await req.json()
    if body.get("type") == "url_verification":
        print("Verification request from Slack")
        return {"challenge": body["challenge"]}
    background_tasks.add_task(handler, body)
    return {"ok": True}



