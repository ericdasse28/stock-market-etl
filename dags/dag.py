from datetime import datetime, timedelta
import os
from airflow import DAG, task
from dotenv import load_dotenv
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

    hit_polygon_api()
