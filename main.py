from fastapi import FastAPI, Request
from fastapi.responses import PlainTextResponse

app = FastAPI()

@app.post("/slack/events")
async def slack_events(req: Request):
    body = await req.json()
    if body.get("type") == "url_verification":
        return PlainTextResponse(body["challenge"])
    return {"ok": True}
