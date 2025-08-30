import requests
from bs4 import BeautifulSoup
import pandas as pd
from database.main import write_etfs_to_db
from crawler.worker import app

# 註冊 task, 有註冊的 task 才可以變成任務發送給 rabbitmq
@app.task()
def etf_list_us(url):

    # 建立請求（可加 headers 避免被擋）
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
    }
    resp = requests.get(url, headers=headers)
    resp.encoding = resp.apparent_encoding

    # 解析 HTML
    soup = BeautifulSoup(resp.text, "html.parser")
    table = soup.find("table")

    # 擷取資料
    etf_list = []
    if table:
        rows = table.find_all("tr")
        for row in rows[1:]:  # 跳過表頭
            cols = row.find_all("td")
            if len(cols) >= 2:
                code = cols[0].get_text(strip=True)
                name = cols[1].get_text(strip=True)
                region = "US"  # 手動補上國別
                currency = "USD"  # 手動補上幣別
                etf_list.append((code, name,region,currency))

    # 建立 DataFrame
    df = pd.DataFrame(etf_list, columns=["etf_id", "etf_name", "region", "currency"])
    write_etfs_to_db(df)

    # return df
