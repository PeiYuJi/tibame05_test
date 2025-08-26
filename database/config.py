from dotenv import load_dotenv
from pathlib import Path
import os


# 載入 .env 檔案
load_dotenv()

MYSQL_HOST = os.getenv("MYSQL_HOST", "35.208.165.47")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", 3306))
MYSQL_ACCOUNT = os.getenv("MYSQL_ACCOUNT", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "pppWgnb_mfGe2m_")


if __name__ == "__main__":
    print("MYSQL_HOST:", MYSQL_HOST)
    print("MYSQL_PORT:", MYSQL_PORT)
    print("MYSQL_ACCOUNT:", MYSQL_ACCOUNT)
    print("MYSQL_PASSWORD:", MYSQL_PASSWORD)