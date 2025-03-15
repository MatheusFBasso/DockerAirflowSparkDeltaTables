from airflow import DAG
from airflow.operators.python import PythonOperator
from lib.Transformations.DivvyBikes.ClassesCall import DivvyBikesCall
import pendulum
from datetime import timedelta


# ----------------------------------------------------------------------------------------------------------------------
def divvy_set_delta_tables():
    DivvyBikesCall('divvy_set_delta_tables')
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
def divvy_bronze():
    DivvyBikesCall('bronze_raw_data')
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
def divvy_bronze_revised_create():
    DivvyBikesCall('bronze_raw_data_revised_create')
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
def divvy_get_bike_status():
    DivvyBikesCall('divvy_get_bike_status')


def bronze_load_bike_status():
    DivvyBikesCall('bronze_load_revised', divvy_path='free_bike_status')


def silver_bike_status():
    DivvyBikesCall('silver_bike_status')


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


def silver_station_information():
    DivvyBikesCall('silver_station_information')


def gold_station_information():
    DivvyBikesCall('gold_station_information')


def divvy_clean_station_information():
    DivvyBikesCall('clean_raw_data', sub_folder_path='station_information')


def pre_divvy_clean_station_information():
    DivvyBikesCall('clean_raw_data', sub_folder_path='station_information')
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
def divvy_get_station_staus():
    DivvyBikesCall('divvy_get_station_status')


def bronze_load_station_status():
    DivvyBikesCall('bronze_load_revised', divvy_path='station_status')


def silver_station_staus():
    DivvyBikesCall('silver_station_status')


def gold_station_staus():
    DivvyBikesCall('gold_station_status')


def divvy_clean_station_staus():
    DivvyBikesCall('clean_raw_data', sub_folder_path='station_status')

def pre_divvy_clean_station_staus():
    DivvyBikesCall('clean_raw_data', sub_folder_path='station_status')
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
def divvy_get_system_pricing():
    DivvyBikesCall('divvy_get_system_pricing')


def bronze_load_system_pricing():
    DivvyBikesCall('bronze_load_revised', divvy_path='system_pricing_plan')


def silver_system_pricing():
    DivvyBikesCall('silver_system_pricing')


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


def silver_vehicle_types():
    DivvyBikesCall('silver_vehicle_types')


def gold_vehicle_types():
    DivvyBikesCall('gold_vehicle_types')


def divvy_clean_vehicle_types():
    DivvyBikesCall('clean_raw_data', sub_folder_path='vehicle_types')


def pre_divvy_clean_vehicle_types():
    DivvyBikesCall('clean_raw_data', sub_folder_path='vehicle_types')
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
with DAG(
    dag_id='DivvyBikesDag',
    schedule_interval='*/20 * * * *',
    concurrency=5,
    start_date=pendulum.datetime(2024, 2, 10, tz='America/Sao_Paulo'),
    catchup=False,
    orientation='TB',
    max_active_runs=1,
    description='Divvy Bikes DAG for multi-hop architecture in Spark Delta Tables.',
    # owner_links={"airflow": "https://airflow.apache.org"},
    tags=['DivvyBikes', 'DeltaTable', 'Spark'],
    dagrun_timeout=timedelta(minutes=8.0),
) as dag:
    # ------------------------------------------------------------------------------------------------------------------
    divvy_bronze = PythonOperator(
        task_id='divvy_bronze',
        python_callable=divvy_bronze,
        task_concurrency=1,
    )
    # divvy_bronze_revised_create = PythonOperator(
    #     task_id='DIvvyBronzeCreateTable',
    #     python_callable=divvy_bronze_revised_create,
    #     task_concurrency=1,
    # )
    # ------------------------------------------------------------------------------------------------------------------
    divvy_get_bike_status = PythonOperator(
        task_id='GetBikeStatus',
        python_callable=divvy_get_bike_status,
        task_concurrency=1,
    )
    # bronze_load_bike_status = PythonOperator(
    #     task_id='BronzeLoadStationStatus',
    #     python_callable=bronze_load_bike_status,
    #     task_concurrency=1,
    #     retries=2,
    #     show_return_value_in_logs=False,
    #     retry_delay=5,
    #     priority_weight=1
    # )
    silver_bike_status = PythonOperator(
        task_id='SilverBikeStatus',
        python_callable=silver_bike_status,
        retries=2,
        show_return_value_in_logs=False,
        retry_delay=5,
        task_concurrency=1,
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
    # bronze_load_station_information = PythonOperator(
    #     task_id='BronzeLoadStationInformation',
    #     python_callable=bronze_load_station_information,
    #     task_concurrency=1,
    #     retries=2,
    #     show_return_value_in_logs=False,
    #     retry_delay=5,
    # )
    silver_station_information = PythonOperator(
        task_id='SilverStationInformation',
        python_callable=silver_station_information,
        retries=2,
        show_return_value_in_logs=False,
        retry_delay=5,
        priority_weight=5,
        task_concurrency=1,
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
    divvy_get_station_staus = PythonOperator(
        task_id='GetStationStatus',
        python_callable=divvy_get_station_staus,
        task_concurrency=1,
    )
    # bronze_load_station_status = PythonOperator(
    #     task_id='BronzeLoadStatusStatus',
    #     python_callable=bronze_load_station_status,
    #     task_concurrency=1,
    #     retries=2,
    #     show_return_value_in_logs=False,
    #     retry_delay=5,
    # )
    silver_station_staus = PythonOperator(
        task_id='SilverStationStatus',
        python_callable=silver_station_staus,
        retries=2,
        show_return_value_in_logs=False,
        retry_delay=5,
        priority_weight=2,
        task_concurrency=1,
    )
    gold_station_staus = PythonOperator(
        task_id='GoldStationStatus',
        python_callable=gold_station_staus,
        retries=2,
        show_return_value_in_logs=False,
        retry_delay=5,
        priority_weight=2,
        task_concurrency=1,
    )
    divvy_clean_station_staus = PythonOperator(
        task_id='CleanStationStatus',
        python_callable=divvy_clean_station_staus,
        priority_weight=4,
        task_concurrency=1,
    )
    pre_divvy_clean_station_staus = PythonOperator(
        task_id='PreCleanStationStatus',
        python_callable=pre_divvy_clean_station_staus,
        task_concurrency=1,
    )
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    divvy_get_system_pricing = PythonOperator(
        task_id='GetSystemPricing',
        python_callable=divvy_get_system_pricing,
        task_concurrency=1,
    )
    # bronze_load_system_pricing = PythonOperator(
    #     task_id='BronzeLoadSystemPricing',
    #     python_callable=bronze_load_system_pricing,
    #     task_concurrency=1,
    #     retries=2,
    #     show_return_value_in_logs=False,
    #     retry_delay=5,
    # )
    silver_system_pricing = PythonOperator(
        task_id='SilverSystemPricing',
        python_callable=silver_system_pricing,
        retries=2,
        show_return_value_in_logs=False,
        retry_delay=5,
        priority_weight=4,
        task_concurrency=1,
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
    # bronze_load_vehicle_types = PythonOperator(
    #     task_id='BronzeLoadVehicleTypes',
    #     python_callable=bronze_load_vehicle_types,
    #     task_concurrency=1,
    #     retries=2,
    #     show_return_value_in_logs=False,
    #     retry_delay=5,
    # )
    silver_vehicle_types = PythonOperator(
        task_id='SilverVehicleTypes',
        python_callable=silver_vehicle_types,
        retries=2,
        show_return_value_in_logs=False,
        retry_delay=5,
        priority_weight=3,
        task_concurrency=1,
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

    # ------------------------------------------------------------------------------------------------------------------
    divvy_set_delta_tables = PythonOperator(
        task_id='SetDeltaTables',
        python_callable=divvy_set_delta_tables,
        task_concurrency=1,
        priority_weight=5,
    )
    # ------------------------------------------------------------------------------------------------------------------

    divvy_set_delta_tables >> silver_station_information >> gold_station_information
    divvy_set_delta_tables >> silver_bike_status >> gold_bike_status
    divvy_set_delta_tables >> silver_system_pricing >> gold_system_pricing
    divvy_set_delta_tables >> silver_station_staus >> gold_station_staus
    divvy_set_delta_tables >> silver_vehicle_types >> gold_vehicle_types

    divvy_bronze >> divvy_clean_station_information
    divvy_bronze >> divvy_clean_bike_status
    divvy_bronze >> divvy_clean_system_pricing
    divvy_bronze >> divvy_clean_station_staus
    divvy_bronze >> divvy_clean_vehicle_types

    # divvy_get_system_pricing >> divvy_bronze_revised_create >> bronze_load_system_pricing >> divvy_bronze
    # divvy_get_station_staus >> divvy_bronze_revised_create >> bronze_load_station_status >> divvy_bronze
    # divvy_get_station_information >>divvy_bronze_revised_create >> bronze_load_station_information >> divvy_bronze
    # divvy_get_bike_status >> divvy_bronze_revised_create >> bronze_load_bike_status >> divvy_bronze
    # divvy_get_vehicle_types >> divvy_bronze_revised_create >> bronze_load_vehicle_types >> divvy_bronze

    pre_divvy_clean_system_pricing >> divvy_get_system_pricing >> divvy_bronze >> divvy_set_delta_tables
    pre_divvy_clean_station_staus >> divvy_get_station_staus >> divvy_bronze >> divvy_set_delta_tables
    pre_divvy_clean_station_information >> divvy_get_station_information >> divvy_bronze >> divvy_set_delta_tables
    pre_divvy_clean_bike_status >> divvy_get_bike_status >> divvy_bronze >> divvy_set_delta_tables
    pre_divvy_clean_vehicle_types >> divvy_get_vehicle_types >> divvy_bronze >> divvy_set_delta_tables

