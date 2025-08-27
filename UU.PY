from crawler.tasks_crawler_etf_us import crawler_etf_us

test_url = "https://www.moneydj.com/etf/ea/et081001.djhtm"
crawler_etf_us.delay(test_url)
