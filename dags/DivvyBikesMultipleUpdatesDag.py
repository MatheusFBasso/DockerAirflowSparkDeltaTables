from airflow import DAG
from airflow.operators.python import PythonOperator
from lib.Transformations.DivvyBikes.ClassesCall import DivvyBikesCall
import pendulum
from datetime import timedelta

# ----------------------------------------------------------------------------------------------------------------------
def divvy_get_bike_status():
    DivvyBikesCall('divvy_get_bike_status')

def bronze_load_bike_status():
    DivvyBikesCall('bronze_load_revised', divvy_path='free_bike_status')

def silver_load_bike_status():
    DivvyBikesCall('silver_revised_bike_status')

def gold_bike_status():
    DivvyBikesCall('gold_bike_status')

def divvy_clean_bike_status():
    DivvyBikesCall('clean_raw_data', sub_folder_path='free_bike_status')

def pre_divvy_clean_bike_status():
    DivvyBikesCall('clean_raw_data', sub_folder_path='free_bike_status')
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
def divvy_get_station_status():
    DivvyBikesCall('divvy_get_station_status')

def bronze_load_station_status():
    DivvyBikesCall('bronze_load_revised', divvy_path='station_status')

def silver_load_station_status():
    DivvyBikesCall('silver_revised_station_status')

def gold_station_status():
    DivvyBikesCall('gold_station_status')

def divvy_clean_station_status():
    DivvyBikesCall('clean_raw_data', sub_folder_path='station_status')

def pre_divvy_clean_station_status():
    DivvyBikesCall('clean_raw_data', sub_folder_path='station_status')
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
with DAG(
    dag_id='DivvyBikesDailyUpdatesDag',
    schedule_interval='*/20 * * * *',
    concurrency=5,
    start_date=pendulum.datetime(2024, 2, 10, tz='America/Sao_Paulo'),
    catchup=False,
    orientation='LR',
    max_active_runs=1,
    description='Divvy Bikes DAG for multi-hop architecture in Spark Delta Tables.',
    # owner_links={"airflow": "https://airflow.apache.org"},
    tags=['DivvyBikes', 'DeltaTable', 'Spark', 'Multiple'],
    dagrun_timeout=timedelta(minutes=2.0),
) as dag:
    # ------------------------------------------------------------------------------------------------------------------
    divvy_get_bike_status = PythonOperator(
        task_id='GetBikeStatus',
        python_callable=divvy_get_bike_status,
        task_concurrency=1,
    )
    bronze_load_bike_status = PythonOperator(
        task_id='BronzeLoadStationStatus',
        python_callable=bronze_load_bike_status,
        task_concurrency=1,
        retries=2,
        show_return_value_in_logs=False,
        retry_delay=5,
        priority_weight=1
    )
    silver_load_bike_status = PythonOperator(
        task_id='DivvySilverBikeStatus',
        python_callable=silver_load_bike_status,
        task_concurrency=1,
        retries=2,
        show_return_value_in_logs=False,
        retry_delay=5,
    )
    gold_bike_status = PythonOperator(
        task_id='GoldBikeStatus',
        python_callable=gold_bike_status,
        retries=2,
        show_return_value_in_logs=False,
        retry_delay=5,
        priority_weight=1,
        task_concurrency=1,
    )
    divvy_clean_bike_status = PythonOperator(
        task_id='CleanBikeStatus',
        python_callable=divvy_clean_bike_status,
        priority_weight=1,
        task_concurrency=1,
    )
    pre_divvy_clean_bike_status = PythonOperator(
        task_id='PreCleanBikeStatus',
        python_callable=pre_divvy_clean_bike_status,
        task_concurrency=1,
    )
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    divvy_get_station_status = PythonOperator(
        task_id='GetStationStatus',
        python_callable=divvy_get_station_status,
        task_concurrency=1,
    )
    bronze_load_station_status = PythonOperator(
        task_id='BronzeLoadStatusStatus',
        python_callable=bronze_load_station_status,
        task_concurrency=1,
        retries=2,
        show_return_value_in_logs=False,
        retry_delay=5,
    )
    silver_load_station_status = PythonOperator(
        task_id='SilverLoadStatusStatus',
        python_callable=silver_load_station_status,
        task_concurrency=1,
        retries=2,
        show_return_value_in_logs=False,
    )
    gold_station_status = PythonOperator(
        task_id='GoldStationStatus',
        python_callable=gold_station_status,
        retries=2,
        show_return_value_in_logs=False,
        retry_delay=5,
        priority_weight=2,
        task_concurrency=1,
    )
    divvy_clean_station_status = PythonOperator(
        task_id='CleanStationStatus',
        python_callable=divvy_clean_station_status,
        priority_weight=4,
        task_concurrency=1,
    )
    pre_divvy_clean_station_status = PythonOperator(
        task_id='PreCleanStationStatus',
        python_callable=pre_divvy_clean_station_status,
        task_concurrency=1,
    )
    # ------------------------------------------------------------------------------------------------------------------

pre_divvy_clean_station_status >> divvy_get_station_status >> bronze_load_station_status >> divvy_clean_station_status >> silver_load_station_status >> gold_station_status
pre_divvy_clean_bike_status >> divvy_get_bike_status >> bronze_load_bike_status >> divvy_clean_bike_status >> silver_load_bike_status >> gold_bike_status
