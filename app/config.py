import logging
import sys
import anthropic
import os
from dotenv import load_dotenv
from google.cloud import bigquery


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

""" CONFIGURACION CON SEVERITY, serviria PARA PRODUCCION
logging.basicConfig(
    level=logging.DEBUG,  # Nivel mínimo de log mostrado
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

"""


ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")
PROMPTS_PATH = "/app/app/prompts/"
claude = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
AUTHORIZED_USERS = ["U06BW8J6MRU", "U031RNA3J86", "U01BECSBLJ1", "U02CYBAR4JY", "U0CGEEKJT"] #miguel, gon, gato, dani, Juan
bq_client = bigquery.Client()