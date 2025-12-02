import logging
import sys
import anthropic
import os
from dotenv import load_dotenv
from google.cloud import bigquery, firestore
from datetime import datetime, timedelta, timezone


load_dotenv()

# === CONFIG GLOBAL DE LOGGING ===
LOG_LEVEL = logging.DEBUG

FORMATTER = "[%(levelname)s] %(name)s.%(funcName)s():Line %(lineno)d → %(message)s"

root_logger = logging.getLogger()
root_logger.handlers.clear()
root_logger.setLevel(LOG_LEVEL)


console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(logging.Formatter(FORMATTER))
console_handler.setLevel(LOG_LEVEL)

root_logger.addHandler(console_handler)
logger = logging.getLogger("FPA")

for noisy_logger in [
    "anthropic",
    "httpx",
    "httpcore",
    "urllib3",
    "google",
    "google.cloud",
    "uvicorn",
    "fastapi"
]:
    logging.getLogger(noisy_logger).setLevel(logging.WARNING)


ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")
PROMPTS_PATH = "/app/app/prompts/"
claude = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
#AUTHORIZED_USERS = ["U06BW8J6MRU", "U031RNA3J86", "U01BECSBLJ1", "U02CYBAR4JY", "U0CGEEKJT"] #miguel, gon, gato, dani, Juan
AUTHORIZED_USERSS = [
    "miguel.llamas@jobandtalent.com",
    "javier.gato@jobandtalent.com",
    "daniel.martin@jobandtalent.com",
    "ivan.camacho@jobandtalent.com",
    "ramon.bernad@jobandtalent.com",
    "pamela.gonzalez@jobandtalent.com",
    "maria.gonzalbez@jobandtalent.com",
    "juan.urdiales@jobandtalent.com",
    "gonzalo.rubio@jobandtalent.com",
    "victoria.delpozo@jobandtalent.com"
    ]
bq_client = bigquery.Client()
db = firestore.Client()


class Thread:
    def __init__(self, event: dict):
        thread_id = event.get("thread_ts") or event.get("ts")
        self.ref = db.collections("slack-bot").document(thread_id)
        doc = self.ref.get()
        if doc.exists:
            data = doc.to_dict()
            self.user_id = data.get("user_id")
            self.thread_id = data.get("thread_id")
            self.channel_id = data.get("channel_id")
            self.messages = data.get("messages",[])
            self.messages.append(event.get("text"))
            self.claude_file_ids = data.get("file_ids", [])
            self.expireAt = data.get("expireAt")
        else:
            self.user_id = event.get("user")
            self.channel_id = event.get("channel")
            self.messages = [event.get("text")]
            self.thread_id = thread_id
            self.claude_file_ids = []
            self.expireAt = datetime.now(timezone.utc) + timedelta(days=7)
