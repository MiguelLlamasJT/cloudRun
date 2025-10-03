from fastapi import FastAPI, Request

app = FastAPI()

@app.post("/slack/events")
async def slack_events(req: Request):
    body = await req.json()

    if body.get("type") == "url_verification":
        return body["challenge"]

    event = body.get("event", {})

    if event.get("type") == "message":
        if event.get("thread_ts"):
            print("Thread:", event)
        else:
            print("Message:", event)

    elif event.get("type") == "reaction_added":
        print("Reaction:", event)

    return {"ok": True}

