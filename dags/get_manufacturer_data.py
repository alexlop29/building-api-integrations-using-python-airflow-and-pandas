import re
import logging
from datetime import datetime

from airflow import DAG
from airflow.operators.python import task
from airflow.models import taskinstance

from requests import get

from pandas import read_csv
from sqlalchemy import create_engine, text

POSTGRES_CONNECTION_STRING = "postgresql+psycopg2://airflow:airflow@postgres:5432/airflow"

with DAG(
    dag_id="get_vehicle_data",
    start_date=datetime(2024, 10, 17),
    catchup=False
) as dag:
    logger = logging.getLogger(__name__)

    @task(task_id="get_manufacturer_data_task")
    def get_manufacturer_data():
        req = get("https://vpic.nhtsa.dot.gov/api/vehicles/GetAllManufacturers?format=csv", allow_redirects=True)
        logger.info(f"Response Status Code: {req.status_code}")
        data = req.headers['content-disposition']
        file = re.findall("filename=(.+)", data)[0]
        taskinstance.xcom_push(key="fileName", value=file)
        logger.info(f"Downloaded File: {file}")
        open(file, 'wb').write(req.content)
        logger.info(req.content)

    @task(task_id="transform_manufacturer_data_task")
    def transform_manufacturer_data():
        fileName = taskinstance.xcom_pull(key="fileName", task_ids="get_manufacturer_data_task")
        df = read_csv(fileName)

        df['mfr_commonname'].fillna(df['mfr_name'], inplace=True)
        df.drop_duplicates(subset=['mfr_commonname'])
        df.drop(columns=['mfr_id', 'country', 'vehicletypes', 'mfr_commonname'])
        df.rename(columns={"mfr_commonname": "name"})
        df["id"] = df.index
        logger.info(f"Parsed Data Frame {df}")

        transformed_file = "transformed_" + fileName
        df.to_csv(transformed_file, index=False)
        taskinstance.xcom_push(key="transformedFileName", value=transformed_file)
        logger.info(f"Transformed data saved to: {transformed_file}")

    @task(task_id="load_manufacturer_data_task")
    def load_manufacturer_data():
        fileName = taskinstance.xcom_pull(key="transformedFileName", task_ids="transform_manufacturer_data_task")
        df = read_csv(fileName)

        engine = create_engine(POSTGRES_CONNECTION_STRING)
        create_table_query = """
        CREATE TABLE IF NOT EXISTS manufacturers (
            name TEXT,
            PRIMARY_KEY(id)
        );
        """

        with engine.connect() as connection:
            connection.execute(text(create_table_query))
        df.to_sql('manufacturers', engine, if_exists='append', index=False)
        logger.info(f"Data loaded into PostgreSQL table 'manufacturer' from file: {fileName}")


    get_data = get_manufacturer_data()
    transform_data = transform_manufacturer_data()
    load_data = load_manufacturer_data()

    get_data >> transform_data >> load_data
