import uuid
from datetime import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator


default_args = {
    "owner": "airscholar",
    "start_date": datetime(2026, 10, 4, 10, 0),
}


def get_data():
    import requests

    res = requests.get("https://randomuser.me/api/")
    res.raise_for_status()

    res = res.json()
    return res["results"][0]


def format_data(res):
    data = {}

    location = res["location"]

    data["id"] = str(uuid.uuid4())
    data["first_name"] = res["name"]["first"]
    data["last_name"] = res["name"]["last"]
    data["gender"] = res["gender"]

    data["address"] = (
        f"{location['street']['number']} "
        f"{location['street']['name']}, "
        f"{location['city']}, "
        f"{location['state']}, "
        f"{location['country']}"
    )

    data["post_code"] = location["postcode"]
    data["email"] = res["email"]
    data["username"] = res["login"]["username"]
    data["dob"] = res["dob"]["date"]
    data["registered_date"] = res["registered"]["date"]
    data["phone"] = res["phone"]
    data["picture"] = res["picture"]["medium"]

    return data


def stream_data():
    import json
    import logging
    import time

    from kafka import KafkaProducer

    producer = KafkaProducer(
        bootstrap_servers=["kafka:9092"],
        max_block_ms=5000,
    )

    curr_time = time.time()

    while time.time() < curr_time + 10:
        try:
            res = get_data()
            res = format_data(res)

            producer.send(
                "users_created",
                value = json.dumps(res).encode("utf-8"),
            )

            partition = (partition + 1) % 3


        except Exception as e:
            logging.error(f"An error occurred: {e}")

    producer.flush()
    producer.close()



with DAG(
    dag_id="user_automation",
    default_args=default_args,
    schedule="@daily",
    catchup=False,
) as dag:

    streaming_task = PythonOperator(
        task_id="stream_data_from_api",
        python_callable=stream_data,
    )