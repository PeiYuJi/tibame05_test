import time
import requests
from bs4 import BeautifulSoup

# 設置網站URL
url = "https://finance.yahoo.com/etf"  # 請將這裡的URL替換為你要爬取的網站

# 設置自訂的User-Agent來模擬瀏覽器
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36'
}

# 重試機制，最多重試5次
def fetch_page(url, retries=5, delay=5):
    for attempt in range(retries):
        # 發送請求
        response = requests.get(url, headers=headers)
        
        # 如果狀態碼是200，表示請求成功
        if response.status_code == 200:
            return response.text
        elif response.status_code == 429:
            # 如果是429錯誤（請求過多），等待一段時間再重試
            print(f"達到請求限制，正在重試... 嘗試次數: {attempt + 1}/{retries}")
            time.sleep(delay)
        else:
            # 其他錯誤
            print(f"發生錯誤，狀態碼：{response.status_code}")
            break
    
    # 如果所有重試都失敗，返回None
    return None

# 嘗試抓取網頁內容
page_content = fetch_page(url)

# 如果成功獲取網頁，則進行解析
if page_content:
    # 使用BeautifulSoup解析網頁
    soup = BeautifulSoup(page_content, 'html.parser')
    
    # 你可以在這裡解析網站內容，這只是示例代碼
    print(soup.prettify())  # 輸出解析後的HTML
else:
    print("無法獲取網頁內容")