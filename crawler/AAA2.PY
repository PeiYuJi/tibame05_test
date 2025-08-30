import requests
from bs4 import BeautifulSoup
import csv

# 目標網站
url = "https://tw.tradingview.com/markets/etfs/funds-usa"

# 發送HTTP GET請求
response = requests.get(url)
response.encoding = 'utf-8'  # 確保處理UTF-8編碼

# 檢查是否成功獲取網頁
if response.status_code == 200:
    # 使用BeautifulSoup來解析HTML
    soup = BeautifulSoup(response.text, 'html.parser')
    
    # 根據網站的HTML結構找到ETF代碼的位置
    # 這裡假設ETF代碼位於特定的標籤中，根據實際情況修改
    etf_elements = soup.find_all('a', {'class': 'tv-widget-symbol'})  # 根據class名稱修改
    
    # 提取ETF代碼並存入列表
    etf_codes = []
    for element in etf_elements:
        etf_codes.append(element.get_text())

    # 輸出結果到CSV
    with open('etf_codes.csv', mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(["ETF Code"])  # CSV文件的標題行
        for code in etf_codes:
            writer.writerow([code])  # 將每個ETF代碼寫入CSV文件

    print(f"成功將{len(etf_codes)}個ETF代碼保存到 'etf_codes.csv'")
else:
    print("無法獲取網頁內容，狀態碼：", response.status_code)