from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup

import pandas as pd

from crawler.worker import app

import pandas as pd
from sqlalchemy import (
    MetaData,
    Table,
    Column,
    ForeignKey,
    Date,
    Float,
    VARCHAR,
    DECIMAL,
    BIGINT,
    create_engine,
    text,
)
from sqlalchemy.dialects.mysql import (
    insert  # 專用於 MySQL 的 insert 語法，可支援 on_duplicate_key_update
)

from crawler.config import MYSQL_ACCOUNT, MYSQL_HOST, MYSQL_PASSWORD, MYSQL_PORT

# 建立連接到 MySQL 的資料庫引擎，不指定資料庫
engine_no_db = create_engine(
    f"mysql+pymysql://{MYSQL_ACCOUNT}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/",
    connect_args={"charset": "utf8mb4"},
)

# 連線，建立 etf 資料庫（如果不存在）
with engine_no_db.connect() as conn:
    conn.execute(
        text(
            "CREATE DATABASE IF NOT EXISTS etf CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"
        )
    )

# 指定連到 etf 資料庫
engine = create_engine(
    f"mysql+pymysql://{MYSQL_ACCOUNT}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/etf",
    # echo=True,  # 所有 SQL 指令都印出來（debug 用）
    pool_pre_ping=True,  # 連線前先 ping 一下，確保連線有效
)

# 建立 etfs、etf_daily_price 資料表（如果不存在）
metadata = MetaData()
# ETF 基本資料表
etfs_table = Table(
    "etfs",
    metadata,
    Column("etf_id", VARCHAR(20), primary_key=True),  # ETF 代碼
    Column("etf_name", VARCHAR(100)),  # ETF 名稱
    Column("region", VARCHAR(10)),  # 地區
    Column("currency", VARCHAR(10)),  # 幣別
)



# 註冊 task, 有註冊的 task 才可以變成任務發送給 rabbitmq
@app.task()
def etf_list_us(url):

    
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")

    driver = webdriver.Chrome(options=options)
    driver.get(url)

    # 等待表格載入
    WebDriverWait(driver, 15).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, "table tbody tr"))
    )

    html = driver.page_source
    soup = BeautifulSoup(html, "html.parser")

    etf_data = []

    # 逐列抓取
    rows = soup.select("table tbody tr")
    for row in rows:
        code_tag = row.select_one('a[href^="/symbols/"]')
        name_tag = row.select_one("sup")
        
        if code_tag and name_tag:
            code = code_tag.get_text(strip=True)
            name = name_tag.get_text(strip=True)
            region = "US"  # 手動補上國別
            currency = "USD"  # 手動補上幣別
            etf_data.append((code, name,region,currency))

    driver.quit()

    df = pd.DataFrame(etf_data, columns=['id', 'name','region','currency'])
    
    return df