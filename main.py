from fastapi import FastAPI, Request, BackgroundTasks
import os
import requests
from analysis import process_question 

app = FastAPI()

SLACK_BOT_TOKEN = os.environ.get("SLACK_BOT_TOKEN")
AUTHORIZED_USERS = ["U06BW8J6MRU", "U031RNA3J86", "U01BECSBLJ1", "U02CYBAR4JY"] #miguel, gon, gato, dani
processed_events = set()


@app.post("/slack/events")
async def slack_events(req: Request, background_tasks: BackgroundTasks):
    body = await req.json()

    if body.get("type") == "url_verification":
        return body["challenge"]

    event = body.get("event", {})
    event_id = body.get("event_id")
    if event_id in processed_events:
        print(f"evento ya siendo procesado: {event_id}")
        return {"ok": True}

    processed_events.add(event_id)

    if event.get("type") != "message" or event.get("bot_id"):
        return {"ok": True}

    print(event)
    user = event.get("user")
    channel = event.get("channel")
    text = event.get("text")
    thread_ts = event.get("thread_ts") or event.get("ts")

    #send_message(channel, "Under maintenance.", thread_ts)
    #return {"ok": True}

    if not text:
        return {"ok": True}
    text = get_thread_history(channel, thread_ts)
    if (user not in AUTHORIZED_USERS):
        print("usuario no autorizado")
        background_tasks.add_task(send_message, channel,"Usuario no autorizado.", thread_ts)
        return {"ok": True}
    print("mensaje valido")
    background_tasks.add_task(process_and_reply, channel, text, thread_ts)
    return {"ok": True}

def process_and_reply(channel, text, ts):
    result_text = process_question(text)
    send_message(channel, result_text, thread_ts=ts)

def send_message(channel, text, thread_ts=None):
    payload = {
        "channel": channel,
        "text": text,
    }
    if thread_ts:
        payload["thread_ts"] = thread_ts

    response = requests.post(
        "https://slack.com/api/chat.postMessage",
        headers={"Authorization": f"Bearer {SLACK_BOT_TOKEN}"},
        data=payload
    )

    # Para depuración
    print("Slack API response:", response.json())


def get_thread_history(channel_id, thread_ts):
    headers = {"Authorization": f"Bearer {SLACK_BOT_TOKEN}"}
    payload = {"channel": channel_id, "ts": thread_ts, "limit": 200}

    resp = requests.get(
        "https://slack.com/api/conversations.replies", 
        headers=headers,
        params=payload)
    
    data = resp.json()
    if not data.get("ok"):
        print(f"Slack API error: {data.get('error')}")
        return []
    print(data["messages"])
    return data["messages"]