import os
from airflow import DAG
from airflow.operators.python import PythonOperator
import pendulum
from brew_project.libs.extract.e_brewery_extract import SimpleDagTest, BreweryAPI
from airflow.providers.postgres.operators.postgres import PostgresOperator
from brew_project.libs.transform.Silver import BrewerySilver
from brew_project.libs.transform.Gold import BreweryGold


def brewery_bronze_extract():
    BreweryAPI().extract_data()


def brewery_silver():
    BrewerySilver().execute()


def brewery_gold():
    BreweryGold().execute()


with DAG(
    dag_id="Brew",
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["BreweryMedalionStudy"],
) as dag:

    brew_extract = PythonOperator(
        task_id="brewery_bronze_extract",
        python_callable=brewery_bronze_extract
    )

    brew_silver = PythonOperator(
        task_id="brewery_silver",
        python_callable=brewery_silver
    )

    brew_gold = PythonOperator(
        task_id="brewery_gold",
        python_callable=brewery_gold
    )

    brew_extract >> brew_silver >> brew_gold