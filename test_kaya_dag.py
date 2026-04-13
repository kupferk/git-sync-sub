import logging
from datetime import datetime, timedelta

from airflow.sdk import dag, task
from airflow.utils.trigger_rule import TriggerRule

app_name = "test_kaya"
app_version = "1.0.0"
k8s_namespace = "stackable-hadoop"
dag_id = app_name + "_dag"


@dag(
    dag_id=dag_id,
    schedule="0 21 * * *",
    start_date=datetime(2000, 1, 1),
    catchup=False,
    dagrun_timeout=timedelta(minutes=120),
    tags=[app_name]
)
def test_kaya_flow():
    @task
    def parse_input_params(
            logical_date: datetime,
            run_id: str = None) -> dict:

        log = logging.getLogger(app_name)

        # Access context variables
        log.info(f"Logical date: {logical_date}")
        log.info(f"Run ID: {run_id}")

        from_date = (logical_date - timedelta(days=1)).strftime('%Y-%m-%d')

        return {
            "from_date": from_date
        }

    parse_input_params()

test_kaya_flow()

