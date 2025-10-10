import logging
import sys
import json
import anthropic
import os
from dotenv import load_dotenv
from google.cloud import bigquery


load_dotenv()
logging.basicConfig(
    level=logging.INFO,  # Nivel mínimo de log mostrado
    format="[%(levelname)s] %(name)s.%(funcName)s():Line %(lineno)d → %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]  # Enviar todo a stdout
)
logger = logging.getLogger(__name__)

class GCPJsonFormatter(logging.Formatter):
    def format(self, record):
        log = {
            "severity": record.levelname,
            "message": record.getMessage(),
            "logger": record.name,
        }
        return json.dumps(log)

handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(GCPJsonFormatter())
root = logging.getLogger()
root.setLevel(logging.INFO)
root.addHandler(handler)


ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")
claude = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
AUTHORIZED_USERS = ["U06BW8J6MRU"] #, "U031RNA3J86", "U01BECSBLJ1", "U02CYBAR4JY"] #miguel, gon, gato, dani
bq_client = bigquery.Client()