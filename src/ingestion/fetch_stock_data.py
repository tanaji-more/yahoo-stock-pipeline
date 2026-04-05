import yfinance as yf
import pandas as pd
import os
from datetime import datetime
import time
import logging

# Logging configuration
logging.basicConfig(
    filename = "stock_pipeline.log",
    level = logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Configuration
TICKERS = ["RELIANCE.NS", "TCS.NS", "HDFCBANK.NS", 
           "ICICIBANK.NS", "INFY.NS", "SBIN.NS", 
           "AXISBANK.NS", "BAJFINANCE.NS"]  # List of stock tickers to fetch

BASE_PATH = "data/raw"  # Base path for storing raw data

# Fetch stock data for a given ticker
def fetch_stock_data(ticker, interval="1d", period="1y"):
    try:
        logging.info(f"Fetching data for {ticker}")

        df = yf.download(ticker, interval= interval, period= period)

        if df.empty:
            logging.warning(f"No data for {ticker}")
            return None

        df.reset_index(inplace = True)

        #standardize column names
        df.columns = [col[0].lower() if isinstance(col, tuple) else col.lower() for col in df.columns]

        # add symbol column
        df["symbol"] = ticker

        return df

    except Exception as e:
        logging.error(f"Error fetching {ticker} : {e}")
    

# Cleaning function to handle missing values and outliers
def clean_data(df):

    # remove duplicates
    df = df.drop_duplicates(subset=["datetime"])

    # handle nulls
    df = df.dropna()

    # Ensure price columns are positive
    df = df[df["close"] > 0]

    return df

# Save function to save data in parquet format
def save_partitioned_data(df):
    df["date"] = df["datetime"].dt.date

    for (symbol, date), group in df.groupby(["symbol", "date"]):
        folder_path = f"{BASE_PATH}/symbol = {symbol}/date={date}"
        os.makedirs(folder_path, exist_ok= True)

        file_path = f"{folder_path}/data.parquet"
        group.to_parquet(file_path, engine="pyarrow", index = False)
    
# Main pipleline function to run the entire process

def run_pipeline():
    for ticker in TICKERS:
        try:
            # Histroical data
            hist_df = fetch_stock_data(ticker, interval="1d", period="2y")

            #intraday data
            intraday_df = fetch_stock_data(ticker, interval="5m", period= "5d")

            if hist_df is not None:
                hist_df.rename(columns={"date" : "datetime"}, inplace = True)
                hist_df = clean_data(hist_df)
                save_partitioned_data(hist_df)

            if intraday_df is not None:
                intraday_df.rename(columns={"datetime" : "datetime"}, inplace = True)
                intraday_df = clean_data(intraday_df)
                save_partitioned_data(intraday_df)
            
        
            
            # API rate limit handling
            time.sleep(2)
        
        except Exception as e:
            logging.error(f"Pipline failed for {ticker} : {e}")

# run the pipeline for all tickers

if __name__ == "__main__":
    run_pipeline()