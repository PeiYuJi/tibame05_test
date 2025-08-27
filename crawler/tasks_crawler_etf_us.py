import pandas as pd
import yfinance as yf
from bs4 import BeautifulSoup
import requests
from crawler.worker import app
from database.main import write_etf_daily_price_to_db
# 註冊 task, 有註冊的 task 才可以變成任務發送給 rabbitmq
@app.task()
def crawler_etf_us(url):

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
                etf_list.append((code, name))

    etf_codes = [code for code, _ in etf_list]
        
    start_date = '2015-05-01'
    end_date = pd.Timestamp.today().strftime('%Y-%m-%d')

    failed_tickers = []
    all_dfs = []
    for r in etf_codes:
        print(f"正在下載：{r}")
        try:
            df = yf.download(r, start=start_date, end=end_date, auto_adjust=False)
            df = df[df["Volume"] > 0].ffill()
            df.reset_index(inplace=True)
            df.rename(columns={
                "Date": "date",
                "Adj Close": "adj_close",
                "Close": "close",
                "High": "high",
                "Low": "low",
                "Open": "open",
                "Volume": "volume"
            }, inplace=True)
            if df.empty:
                raise ValueError("下載結果為空")
        except Exception as e:
            print(f"[⚠️ 錯誤] {r} 下載失敗：{e}")
            failed_tickers.append(r)
            continue
        df.insert(0, "etf_id", r)  # 新增一欄「etf_id」
        all_dfs.append(df)

        #df.columns = ["etf_id","date", "dividend_per_unit"]    # 調整欄位名稱
    columns_order = ['etf_id', 'date', 'adj_close','close','high', 'low', 'open','volume']
    result_df = pd.concat(all_dfs, ignore_index=True)
    result_df = result_df[columns_order]
    write_etf_daily_price_to_db(result_df)
    # return df