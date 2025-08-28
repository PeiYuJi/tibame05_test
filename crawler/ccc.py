import pandas as pd
import yfinance as yf
from bs4 import BeautifulSoup
import requests


url="https://www.moneydj.com/etf/ea/et081001.djhtm"

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
all_dividends = []
if table:
    rows = table.find_all("tr")
    for row in rows[1:]:  # 跳過表頭
        cols = row.find_all("td")
        if len(cols) >= 2:
            code = cols[0].get_text(strip=True)
            name = cols[1].get_text(strip=True)
            etf_list.append((code, name))

etf_codes = [code for code, _ in etf_list]

for ticker in etf_codes:
        # 抓取配息資料
    dividends = yf.Ticker(ticker).dividends
    if not dividends.empty:
        dividends_df = dividends.reset_index()
        dividends_df.columns = ["date", "dividend_per_unit"]    # 調整欄位名稱
        dividends_df["date"] = dividends_df["date"].dt.date  # 只保留年月日
        dividends_df.insert(0, "etf_id", ticker)  # 新增股票代碼欄位，放第一欄
        dividends_df.insert(3, "currency", "USD")  # 新增欄位，放第一欄
        all_dividends.append(dividends_df)  # 加入到 list 中
    else:
        print(f"{ticker} 沒有配息資料")
if all_dividends:
    result_dividends = pd.concat(all_dividends, ignore_index=True)
    print(result_dividends)  # 印前幾筆就好
else:
    print("沒有任何 ETF 有配息資料。")

result_dividends.to_csv("etf_dividends.csv", index=False, encoding="utf-8-sig")