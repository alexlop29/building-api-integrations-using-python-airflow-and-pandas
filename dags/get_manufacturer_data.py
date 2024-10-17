from airflow import DAG
from airflow.operators.python import task
from requests import get
import re
import logging
from datetime import datetime

with DAG(
    dag_id="get_vehicle_data",
    start_date=datetime(2024, 10, 17),
    catchup=False
) as dag:
    logger = logging.getLogger(__name__)

    @task()
    def get_manufacturer_data():
        req = get("https://vpic.nhtsa.dot.gov/api/vehicles/GetAllManufacturers?format=csv", allow_redirects=True)
        logger.info(f"Response Status Code: {req.status_code}")
        data = req.headers['content-disposition']
        file = re.findall("filename=(.+)", data)[0]
        logger.info(f"Downloaded File: {file}")
        open(file, 'wb').write(req.content)
        logger.info(req.content)

    @task()
    def transform_manufacturer_data():
        pass

    @task()
    def load_manufacturer_data():
        pass

    get_data = get_manufacturer_data()
    transform_data = transform_manufacturer_data()
    load_data = load_manufacturer_data()

    get_data >> transform_data >> load_data
