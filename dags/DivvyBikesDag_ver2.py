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
def divvy_get_station_information():
    DivvyBikesCall('divvy_get_station_information')

def bronze_load_station_information():
    DivvyBikesCall('bronze_load_revised', divvy_path='station_information')

def silver_load_station_information():
    DivvyBikesCall('silver_revised_station_information')

def gold_station_information():
    DivvyBikesCall('gold_station_information')

def divvy_clean_station_information():
    DivvyBikesCall('clean_raw_data', sub_folder_path='station_information')

def pre_divvy_clean_station_information():
    DivvyBikesCall('clean_raw_data', sub_folder_path='station_information')
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
def divvy_get_system_pricing():
    DivvyBikesCall('divvy_get_system_pricing')

def bronze_load_system_pricing():
    DivvyBikesCall('bronze_load_revised', divvy_path='system_pricing_plan')

def silver_load_system_pricing():
    DivvyBikesCall('silver_revised_system_pricing')

def gold_system_pricing():
    DivvyBikesCall('gold_system_pricing')

def divvy_clean_system_pricing():
    DivvyBikesCall('clean_raw_data', sub_folder_path='system_pricing_plan')

def pre_divvy_clean_system_pricing():
    DivvyBikesCall('clean_raw_data', sub_folder_path='system_pricing_plan')
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
def divvy_get_vehicle_types():
    DivvyBikesCall('divvy_get_vehicle_types')

def bronze_load_vehicle_types():
    DivvyBikesCall('bronze_load_revised', divvy_path='vehicle_types')

def silver_load_vehicle_types():
    DivvyBikesCall('silver_revised_vehicle_types')

def gold_vehicle_types():
    DivvyBikesCall('gold_vehicle_types')

def divvy_clean_vehicle_types():
    DivvyBikesCall('clean_raw_data', sub_folder_path='vehicle_types')

def pre_divvy_clean_vehicle_types():
    DivvyBikesCall('clean_raw_data', sub_folder_path='vehicle_types')
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
with DAG(
    dag_id='DivvyBikesDag_ver2',
    schedule_interval='*/20 * * * *',
    concurrency=5,
    start_date=pendulum.datetime(2024, 2, 10, tz='America/Sao_Paulo'),
    catchup=False,
    orientation='LR',
    max_active_runs=1,
    description='Divvy Bikes DAG for multi-hop architecture in Spark Delta Tables.',
    # owner_links={"airflow": "https://airflow.apache.org"},
    tags=['DivvyBikes', 'DeltaTable', 'Spark'],
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
    divvy_get_station_information = PythonOperator(
        task_id='GetStationInformation',
        python_callable=divvy_get_station_information,
        task_concurrency=1,
    )
    bronze_load_station_information = PythonOperator(
        task_id='BronzeLoadStationInformation',
        python_callable=bronze_load_station_information,
        task_concurrency=1,
        retries=2,
        show_return_value_in_logs=False,
        retry_delay=5,
    )
    silver_load_station_information = PythonOperator(
        task_id='DivvySilverStationInformation',
        python_callable=silver_load_station_information,
        task_concurrency=1,
        retries=2,
        show_return_value_in_logs=False,
    )
    gold_station_information = PythonOperator(
        task_id='GoldStationInformation',
        python_callable=gold_station_information,
        retries=2,
        show_return_value_in_logs=False,
        retry_delay=5,
        priority_weight=5,
        task_concurrency=1,
    )
    divvy_clean_station_information = PythonOperator(
        task_id='CleanStationInformation',
        python_callable=divvy_clean_station_information,
        priority_weight=3,
        task_concurrency=1,
    )
    pre_divvy_clean_station_information = PythonOperator(
        task_id='PreCleanStationInformation',
        python_callable=pre_divvy_clean_station_information,
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

    # ------------------------------------------------------------------------------------------------------------------
    divvy_get_system_pricing = PythonOperator(
        task_id='GetSystemPricing',
        python_callable=divvy_get_system_pricing,
        task_concurrency=1,
    )
    bronze_load_system_pricing = PythonOperator(
        task_id='BronzeLoadSystemPricing',
        python_callable=bronze_load_system_pricing,
        task_concurrency=1,
        retries=2,
        show_return_value_in_logs=False,
        retry_delay=5,
    )
    silver_load_system_pricing = PythonOperator(
        task_id='SilverLoadSystemPricing',
        python_callable=silver_load_system_pricing,
        task_concurrency=1,
        retries=2,
        show_return_value_in_logs=False,
    )
    gold_system_pricing = PythonOperator(
        task_id='GoldSystemPricing',
        python_callable=gold_system_pricing,
        retries=2,
        show_return_value_in_logs=False,
        retry_delay=5,
        priority_weight=4,
        task_concurrency=1,
    )
    divvy_clean_system_pricing = PythonOperator(
        task_id='CleanSystemPricing',
        python_callable=divvy_clean_system_pricing,
        priority_weight=3,
        task_concurrency=1,
    )
    pre_divvy_clean_system_pricing = PythonOperator(
        task_id='PreCleanSystemPricing',
        python_callable=pre_divvy_clean_system_pricing,
        task_concurrency=1,
    )
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    divvy_get_vehicle_types = PythonOperator(
        task_id='GetVehicleTypes',
        python_callable=divvy_get_vehicle_types,
        task_concurrency=1,
    )
    bronze_load_vehicle_types = PythonOperator(
        task_id='BronzeLoadVehicleTypes',
        python_callable=bronze_load_vehicle_types,
        task_concurrency=1,
        retries=2,
        show_return_value_in_logs=False,
        retry_delay=5,
    )
    silver_load_vehicle_types = PythonOperator(
        task_id='SilverLoadVehicleTypes',
        python_callable=silver_load_vehicle_types,
        task_concurrency=1,
        retries=2,
        show_return_value_in_logs=False,
    )
    gold_vehicle_types = PythonOperator(
        task_id='GoldVehicleTypes',
        python_callable=gold_vehicle_types,
        retries=2,
        show_return_value_in_logs=False,
        retry_delay=5,
        priority_weight=3,
        task_concurrency=1,
    )
    divvy_clean_vehicle_types = PythonOperator(
        task_id='CleanVehicleTypes',
        python_callable=divvy_clean_vehicle_types,
        priority_weight=3,
        task_concurrency=1,
    )
    pre_divvy_clean_vehicle_types = PythonOperator(
        task_id='PreCleanVehicleTypes',
        python_callable=pre_divvy_clean_vehicle_types,
        task_concurrency=1,
    )
    # ------------------------------------------------------------------------------------------------------------------

pre_divvy_clean_system_pricing >> divvy_get_system_pricing >> bronze_load_system_pricing >> divvy_clean_system_pricing >> silver_load_system_pricing >> gold_system_pricing
pre_divvy_clean_station_status >> divvy_get_station_status >> bronze_load_station_status >> divvy_clean_station_status >> silver_load_station_status >> gold_station_status
pre_divvy_clean_station_information >> divvy_get_station_information >> bronze_load_station_information >> divvy_clean_station_information >> silver_load_station_information >> gold_station_information
pre_divvy_clean_bike_status >> divvy_get_bike_status >> bronze_load_bike_status >> divvy_clean_bike_status >> silver_load_bike_status >> gold_bike_status
pre_divvy_clean_vehicle_types >> divvy_get_vehicle_types >> bronze_load_vehicle_types >> divvy_clean_vehicle_types >> silver_load_vehicle_types >> gold_vehicle_types