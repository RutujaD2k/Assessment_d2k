import requests
import pandas as pd
from datetime import datetime
import io
from minio import Minio
from dagster import job, op

# Alpha Vantage API configuration
ALPHA_VANTAGE_API_KEY = "9QC2UH40C1UGZFIY"  
BASE_URL = "https://www.alphavantage.co/query"

@op
def fetch_stock_data(symbol="IBM", interval="1min"):
    """Fetch stock data from Alpha Vantage API."""
    params = {
        "function": "TIME_SERIES_INTRADAY",
        "symbol": symbol,
        "interval": interval,
        "apikey": ALPHA_VANTAGE_API_KEY,
        "outputsize": "compact"
    }
    response = requests.get(BASE_URL, params=params)
    data = response.json()

    if f"Time Series ({interval})" not in data:
        raise Exception(f"Error fetching data: {data.get('Note', 'Unknown error')}")
    
    time_series_key = f"Time Series ({interval})"
    df = pd.DataFrame.from_dict(data[time_series_key], orient="index")
    df.index = pd.to_datetime(df.index)
    df.columns = ["open", "high", "low", "close", "volume"]
    df = df.astype(float)
    df['symbol'] = symbol
    df['processed_at'] = datetime.now()
    
    return df

@op
def save_data_to_minio(df, bucket_name="stock-data", minio_client=None):
    """Save the DataFrame to a MinIO bucket."""
    if minio_client is None:
        minio_client = Minio(
            "minio-server:9000",
            access_key="cefPpbAmIoxxXfxP2Z8c",
            secret_key="xn7pQrbpX5yUB9P53fZjbrrZfLnMQ8C2zTPjS5IR",
            secure=False  
        )
    
    filename = f"processed_stock_data_{datetime.now().strftime('%Y%m%d')}.csv"
    
    buffer = io.StringIO()
    df.to_csv(buffer, index=False)
    buffer.seek(0)
    
    minio_client.put_object(bucket_name, filename, buffer.getvalue(), len(buffer.getvalue()))
    
    print(f"Data saved to MinIO bucket '{bucket_name}' with filename '{filename}'")

    
