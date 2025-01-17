from airflow.models.dagbag import DagBag
from datetime import datetime

import pytz


def test_market_etl_config():
    market_etl_dag = DagBag().get_dag("market_etl")

    assert market_etl_dag.start_date == datetime(2024, 1, 1, tzinfo=pytz.UTC)
    assert market_etl_dag.schedule_interval == "@daily"
    assert market_etl_dag.catchup
