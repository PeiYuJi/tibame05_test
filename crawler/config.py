import os

WORKER_ACCOUNT = os.environ.get("WORKER_ACCOUNT", "worker")
WORKER_PASSWORD = os.environ.get("WORKER_PASSWORD", "worker")

RABBITMQ_HOST = os.environ.get("RABBITMQ_HOST", "35.209.80.196")
RABBITMQ_PORT = int(os.environ.get("RABBITMQ_PORT", 5672))

MYSQL_HOST = os.environ.get("MYSQL_HOST", "35.209.80.196")
MYSQL_PORT = int(os.environ.get("MYSQL_PORT", 3306))
MYSQL_ACCOUNT = os.environ.get("MYSQL_ACCOUNT", "root")
MYSQL_PASSWORD = os.environ.get("MYSQL_PASSWORD", "ppWgnb_mfGe2m_")
