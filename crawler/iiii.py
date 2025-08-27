import pandas as pd
import yfinance as yf
from bs4 import BeautifulSoup
import requests

# Step 1: 抓 ETF 名單
url = "https://www.moneydj.com/etf/ea/et081001.djhtm"
headers = {"User-Agent": "Mozilla/5.0"}
resp = requests.get(url, headers=headers)
resp.encoding = resp.apparent_encoding
soup = BeautifulSoup(resp.text, "html.parser")
table = soup.find("table")

etf_list = []
if table:
    rows = table.find_all("tr")
    for row in rows[1:]:
        cols = row.find_all("td")
        if len(cols) >= 2:
            code = cols[0].get_text(strip=True)
            name = cols[1].get_text(strip=True)
            region = "US"
            currency = "USD"
            etf_list.append((code, name, region, currency))

etf_codes = [code.replace('.', '-') for code, _ in etf_list]

# Step 2: 下載歷史資料
start_date = '2015-05-01'
end_date = pd.Timestamp.today().strftime('%Y-%m-%d')
failed_tickers = []
all_etf_data = []

for r in etf_codes:
    print(f"正在下載：{r}")
    try:
        df = yf.download(r, start=start_date, end=end_date, auto_adjust=False)
        if df.empty:
            raise ValueError("下載結果為空")

        df = df[df["Volume"] > 0].ffill()
        df.reset_index(inplace=True)

        required_cols = ["Date", "Adj Close", "Close", "High", "Low", "Open", "Volume"]
        if not all(col in df.columns for col in required_cols):
            raise ValueError("資料欄位不完整")

        df.rename(columns={
            "Date": "date",
            "Adj Close": "adj_close",
            "Close": "close",
            "High": "high",
            "Low": "low",
            "Open": "open",
            "Volume": "volume"
        }, inplace=True)

        df.insert(0, "etf_id", r)
        df = df[['etf_id', 'date', 'adj_close', 'close', 'high', 'low', 'open', 'volume']]
        all_etf_data.append(df)

    except Exception as e:
        print(f"[⚠️ 錯誤] {r} 下載失敗：{e}")
        failed_tickers.append(r)

# Step 3: 合併所有 ETF 資料
if all_etf_data:
    final_df = pd.concat(all_etf_data, ignore_index=True)
    print(f"成功下載 {len(all_etf_data)} 檔 ETF 的歷史資料。")
else:
    print("無資料下載成功。")
