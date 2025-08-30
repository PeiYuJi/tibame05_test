import requests
from bs4 import BeautifulSoup
import pandas as pd

# 設定網址
url = "https://tw.tradingview.com/markets/etfs/funds-usa/"

# 發送 HTTP 請求獲取網站內容
response = requests.get(url)
response.encoding = 'utf-8'  # 確保中文編碼正確

# 解析 HTML
soup = BeautifulSoup(response.text, 'html.parser')

# 假設 ETF 代碼都在 href 中，並且以 'symbol' 開頭
etf_codes = []

# 解析表格數據
rows = soup.select("table tbody tr")
for row in rows:
    code_tag = row.select_one('a[href^="/symbols/"]')
    name_tag = row.select_one("sup")
    
    if code_tag and name_tag:
        code = code_tag.get_text(strip=True)
        name = name_tag.get_text(strip=True)
        region = "US"  # 手動補上國別
        currency = "USD"  # 手動補上幣別
        etf_codes.append((code, name, region, currency))

# 將資料放入 DataFrame
df = pd.DataFrame(etf_codes, columns=["etf_id", "etf_name", "region", "currency"])

# 將 ETF 代碼寫入 CSV 文件
csv_filename = "etf_codes.csv"
df.to_csv(csv_filename, index=False, encoding='utf-8')

print(f"ETF 代碼已經成功保存到 {csv_filename} 文件中。")