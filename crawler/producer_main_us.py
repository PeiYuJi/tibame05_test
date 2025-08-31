from crawler.tasks_etf_list_us import etf_list_us  
from crawler.tasks_crawler_etf_us import crawler_etf_us
from crawler.tasks_backtest_utils_us import backtest_utils_us    
from crawler.tasks_crawler_etf_dps_us import crawler_etf_dps_us      
from celery import chain, group



if __name__ == "__main__":
    crawler_url = "https://tw.tradingview.com/markets/etfs/funds-usa/"

    workflow = chain(
        # 指定 etf_list_us 送到 etfus 隊列
        etf_list_us.s(crawler_url=crawler_url).set(queue='etfus'),

        group(
            # 以下每個子任務指定不同隊列
            crawler_etf_us.s().set(queue='etfus_price'),         # 價格資料
            backtest_utils_us.s().set(queue='etfus_utils'),      # 技術指標和績效分析
            crawler_etf_dps_us.s().set(queue='etfus_dps')        # 配息資料
        )
    )

    # 發送任務到 RabbitMQ
    result = workflow.apply_async()
    print("任務已發送，正在處理中...")

    # 阻塞等待所有任務完成，這行可根據需求選擇要不要加
    result.get()