import pandas as pd
import yfinance as yf
from bs4 import BeautifulSoup
import requests
from crawler.worker import app
import pandas_ta as ta
import numpy as np
from database.main import write_etf_backtest_results_to_db, write_etf_daily_price_to_db


# 註冊 task, 有註冊的 task 才可以變成任務發送給 rabbitmq
@app.task()
def backtest_utils_us(url):
    
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
    summary_df = [] 
    for r in etf_codes:
        print(f"正在下載：{r}")
        try:
            df = yf.download(r, start=start_date, end=end_date, auto_adjust=False)
            df = df[df["Volume"] > 0].ffill()

            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.droplevel(1)
            df.columns.name = None

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

        
        # RSI (14) (相對強弱指標)
        df["rsi"] = ta.rsi(df["close"], length=14)

        # MA5、MA20（移動平均線）（也可以使用 df['close'].rolling(5).mean())）
        df["ma5"] = ta.sma(df["close"], length=5)
        df["ma20"] = ta.sma(df["close"], length=20)

        # MACD（移動平均收斂背離指標）
        macd = ta.macd(df["close"], fast=12, slow=26, signal=9)
        df["macd_line"] = macd["MACD_12_26_9"]
        df["macd_signal"] = macd["MACDs_12_26_9"]
        df["macd_hist"] = macd["MACDh_12_26_9"]

        # KD 指標（STOCH: 隨機震盪指標）
        stoch = ta.stoch(df["high"], df["low"], df["close"], k=14, d=3, smooth_k=3)
        df["pct_k"] = stoch["STOCHk_14_3_3"]
        df["pct_d"] = stoch["STOCHd_14_3_3"]

        # 增加該日報酬率與累積報酬指數
        df['daily_return'] = df['adj_close'].pct_change()
        df['cumulative_return'] = (1 + df['daily_return']).cumprod()
        df.insert(0, "etf_id", r)  # 新增一欄「etf_id」
        print (df)
        #df.columns = ["etf_id","date", "dividend_per_unit"]    # 調整欄位名稱
        columns_order = ['etf_id', 'date', 'adj_close','close','high', 'low', 'open','volume',
                        'rsi', 'ma5', 'ma20', 'macd_line', 'macd_signal', 'macd_hist',
                        'pct_k', 'pct_d', 'daily_return', 'cumulative_return']
        df = df[columns_order]

        write_etf_daily_price_to_db(df)

        # 儲存技術指標結果
        print("開始 2️⃣ 進行技術指標計算與績效分析")



        # 確保 date 欄位為 datetime
        if not pd.api.types.is_datetime64_any_dtype(df["date"]):
            df["date"] = pd.to_datetime(df["date"])

        # 回測期間
        backtest_start = df["date"].iloc[0].strftime("%Y-%m-%d")
        backtest_end = df["date"].iloc[-1].strftime("%Y-%m-%d")
        print(backtest_start, backtest_end)
        # 總報酬率（Total Return）
        # 增加該日報酬率與累積報酬指數
        df['daily_return'] = df['adj_close'].pct_change()
        df['cumulative_return'] = (1 + df['daily_return']).cumprod()
        total_return = df['cumulative_return'].iloc[-1] - 1

        # 年化報酬率（CAGR）
        days = (df["date"].iloc[-1] - df["date"].iloc[0]).days

        if days <= 0 or df['cumulative_return'].iloc[-1] <= 0:
            cagr = np.nan
        else:
            cagr = df['cumulative_return'].iloc[-1] ** (365 / days) - 1

        # 最大回撤（Max Drawdown）
        roll_max = df['cumulative_return'].cummax()
        drawdown = df['cumulative_return'] / roll_max - 1
        max_drawdown = drawdown.min()

        # 夏普比率（Sharpe Ratio）
        std_return = df['daily_return'].std()
        sharpe = np.sqrt(252) * df['daily_return'].mean() / std_return if std_return and std_return != 0 else np.nan
        
        
        summary_data = {
            "etf_id": r,
            "backtest_start": backtest_start,
            "backtest_end": backtest_end,
            "total_return": total_return,
            "cagr": cagr,
            "max_drawdown": max_drawdown,
            "sharpe_ratio": sharpe
            }

        summary_df.append(summary_data)

    summary_df = pd.DataFrame(summary_df)
    # 指定欄位輸出順序
    desired_order = ["etf_id", "backtest_start", "backtest_end", "total_return", "cagr", "max_drawdown", "sharpe_ratio"]
    summary_df = summary_df[desired_order]

    write_etf_backtest_results_to_db(summary_df)

    # return summary_df
