import requests
from bs4 import BeautifulSoup
import csv

# Yahoo Finance 美股ETF列表頁面
url = "https://finance.yahoo.com/etf"

# 發送HTTP GET請求
response = requests.get(url)
response.encoding = 'utf-8'

# 檢查請求是否成功
if response.status_code == 200:
    # 使用BeautifulSoup來解析HTML
    soup = BeautifulSoup(response.text, 'html.parser')
    
    # 假設ETF代碼在 class='simpTblRow' 標籤中，你可以根據實際情況修改
    etf_rows = soup.find_all('tr', class_='simpTblRow')

    # 提取ETF代碼並存入列表
    etf_codes = []
    for row in etf_rows:
        # 假設ETF代碼在第二個<td>元素中
        columns = row.find_all('td')
        if len(columns) > 1:
            etf_code = columns[0].get_text(strip=True)
            etf_codes.append(etf_code)

    # 將ETF代碼寫入CSV文件
    with open('all_etf_codes.csv', mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(["ETF Code"])  # CSV文件標題
        for code in etf_codes:
            writer.writerow([code])

    print(f"成功將{len(etf_codes)}個ETF代碼保存到 'all_etf_codes.csv'")
else:
    print("無法獲取網頁內容，狀態碼：", response.status_code)