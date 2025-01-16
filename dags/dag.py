from datetime import datetime, timedelta
import os
from airflow import DAG, task
from dotenv import load_dotenv
import pandas as pd
import requests

load_dotenv()

with DAG(
    dag_id="market_etl",
    start_date=datetime(2024, 1, 1, 9),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 3, "retry_delay": timedelta(minutes=5)},
) as dag:

    @task
    def hit_polygon_api(**context):
        """Extract stock market data from Polygon API."""

        stock_ticker = "AMZN"
        polygon_api_key = os.environ["POLYGON_API_KEY"]
        ds = context.get("ds")

        url = f"<https://api.polygon.io/v1/open-close/{stock_ticker}/{ds}?adjusted=true&apiKey={polygon_api_key}>"  # noqa
        response = requests.get(url)
        # Return the raw data
        return response.json()

    @task
    def flatten_market_data(polygon_response, **context):
        # Create a list of headers and a list to store
        # the normalized data in
        columns = {
            "status": "closed",
            "from": context.get("ds"),
            "symbol": "AMZN",
            "open": None,
            "high": None,
            "low": None,
            "close": None,
            "volume": None,
        }

        flattened_record = []
        for header_name, default_value in columns.items():
            flattened_record.append(
                polygon_response.get(
                    header_name,
                    default_value,
                )
            )
        flattened_dataframe = pd.DataFrame(
            [flattened_record],
            columns=columns.keys(),
        )
        return flattened_dataframe

    raw_market_data = hit_polygon_api()
    flatten_market_data(raw_market_data)
