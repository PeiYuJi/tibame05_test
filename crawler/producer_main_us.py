from crawler.tasks_etf_list_us import etf_list_us  

crawler_url = "https://tw.tradingview.com/markets/etfs/funds-usa/"

print("ETF 清單")
etf_list_us.s(crawler_url=crawler_url).apply_async(queue="etfus"
    ).get()  # 使用 apply_async 發送任務到 RabbitMQ
    # etf_list_us = etf_list_us(us_etf_url)  # 直接呼叫函式