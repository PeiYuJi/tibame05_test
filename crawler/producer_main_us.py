from crawler.tasks_etf_list_us import etf_list_us  

crawler_url = "https://tw.tradingview.com/markets/etfs/funds-usa/"

print("ETF 清單")
etf_list_us = etf_list_us.s(crawler_url="https://tw.tradingview.com/markets/etfs/funds-usa/")
etf_list_us.apply_async(queue="etfus") 

