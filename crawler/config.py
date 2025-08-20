from dotenv import load_dotenv
from pathlib import Path
import os

# 載入 .env 檔案
load_dotenv()


RABBITMQ_HOST = os.environ.get("RABBITMQ_HOST", "rabbitmq")
RABBITMQ_PORT = os.environ.get("RABBITMQ_PORT", 5672)
WORKER_ACCOUNT = os.environ.get("WORKER_ACCOUNT", "worker")
WORKER_PASSWORD = os.environ.get("WORKER_PASSWORD", "worker")
