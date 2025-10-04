from fastapi import FastAPI, Request
import os
import requests
from analysis import process_question 

app = FastAPI()

SLACK_BOT_TOKEN = os.environ.get("SLACK_BOT_TOKEN")

@app.post("/slack/events")
async def slack_events(req: Request):
    body = await req.json()

    # Verificación inicial de Slack
    if body.get("type") == "url_verification":
        return body["challenge"]

    event = body.get("event", {})

    # Solo manejar mensajes de usuarios (ignora mensajes de bots)
    if event.get("type") == "message" and not event.get("bot_id"):
        channel = event.get("channel")
        text = event.get("text")
        thread_ts = event.get("thread_ts")
        ts = event.get("ts")

        if thread_ts:
            # Mensaje en un thread → responder en el thread
            result_text = process_question(text)
            send_message(channel, result_text, thread_ts=thread_ts)
        else:
            # Mensaje nuevo → responder debajo de ese mensaje (thread nuevo)
            result_text = process_question(text)
            send_message(channel, result_text, thread_ts=ts)

    return {"ok": True}


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
