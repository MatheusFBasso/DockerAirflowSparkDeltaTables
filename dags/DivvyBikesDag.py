from airflow import DAG
from airflow.operators.python import PythonOperator
from lib.Transformations.DivvyBikes.ClassesCall import DivvyBikesCall
import pendulum
from time import sleep


# ----------------------------------------------------------------------------------------------------------------------
def divvy_set_delta_tables():
    print(f'STARTING'.rjust(120, '.'))
    DivvyBikesCall('divvy_set_delta_tables')
    print(f'FINISHED'.rjust(120, '.'))
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
def divvy_get_bike_status():
    print(f'STARTING'.rjust(120, '.'))
    DivvyBikesCall('divvy_get_bike_status')
    print(f'FINISHED'.rjust(120, '.'))


def silver_bike_status():
    print(f'STARTING'.rjust(120, '.'))
    DivvyBikesCall('silver_bike_status')
    print(f'FINISHED'.rjust(120, '.'))


def gold_bike_status():
    print(f'STARTING'.rjust(120, '.'))
    DivvyBikesCall('gold_bike_status')
    print(f'FINISHED'.rjust(120, '.'))


def divvy_clean_bike_status():
    print(f'STARTING'.rjust(120, '.'))
    DivvyBikesCall('clean_raw_data', sub_folder_path='free_bike_status')
    print(f'FINISHED'.rjust(120, '.'))


def pre_divvy_clean_bike_status():
    print(f'STARTING'.rjust(120, '.'))
    DivvyBikesCall('clean_raw_data', sub_folder_path='free_bike_status')
    print(f'FINISHED'.rjust(120, '.'))
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
def divvy_get_station_information():
    print(f'STARTING'.rjust(120, '.'))
    DivvyBikesCall('divvy_get_station_information')
    print(f'FINISHED'.rjust(120, '.'))


def silver_station_information():
    print(f'STARTING'.rjust(120, '.'))
    DivvyBikesCall('silver_station_information')
    print(f'FINISHED'.rjust(120, '.'))


def gold_station_information():
    print(f'STARTING'.rjust(120, '.'))
    DivvyBikesCall('gold_station_information')
    print(f'FINISHED'.rjust(120, '.'))


def divvy_clean_station_information():
    print(f'STARTING'.rjust(120, '.'))
    DivvyBikesCall('clean_raw_data', sub_folder_path='station_information')
    print(f'FINISHED'.rjust(120, '.'))


def pre_divvy_clean_station_information():
    print(f'STARTING'.rjust(120, '.'))
    DivvyBikesCall('clean_raw_data', sub_folder_path='station_information')
    print(f'FINISHED'.rjust(120, '.'))
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
def divvy_get_station_staus():
    print(f'STARTING'.rjust(120, '.'))
    DivvyBikesCall('divvy_get_station_status')
    print(f'FINISHED'.rjust(120, '.'))


def silver_station_staus():
    print(f'STARTING'.rjust(120, '.'))
    DivvyBikesCall('silver_station_status')
    print(f'FINISHED'.rjust(120, '.'))


def gold_station_staus():
    print(f'STARTING'.rjust(120, '.'))
    DivvyBikesCall('gold_station_status')
    print(f'FINISHED'.rjust(120, '.'))


def divvy_clean_station_staus():
    print(f'STARTING'.rjust(120, '.'))
    DivvyBikesCall('clean_raw_data', sub_folder_path='station_status')
    print(f'FINISHED'.rjust(120, '.'))

def pre_divvy_clean_station_staus():
    print(f'STARTING'.rjust(120, '.'))
    DivvyBikesCall('clean_raw_data', sub_folder_path='station_status')
    print(f'FINISHED'.rjust(120, '.'))
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
def divvy_get_system_pricing():
    print(f'STARTING'.rjust(120, '.'))
    DivvyBikesCall('divvy_get_system_pricing')
    print(f'FINISHED'.rjust(120, '.'))


def silver_system_pricing():
    print(f'STARTING'.rjust(120, '.'))
    DivvyBikesCall('silver_system_pricing')
    print(f'FINISHED'.rjust(120, '.'))


def gold_system_pricing():
    print(f'STARTING'.rjust(120, '.'))
    DivvyBikesCall('gold_system_pricing')
    print(f'FINISHED'.rjust(120, '.'))


def divvy_clean_system_pricing():
    print(f'STARTING'.rjust(120, '.'))
    DivvyBikesCall('clean_raw_data', sub_folder_path='system_pricing_plan')
    print(f'FINISHED'.rjust(120, '.'))


def pre_divvy_clean_system_pricing():
    print(f'STARTING'.rjust(120, '.'))
    DivvyBikesCall('clean_raw_data', sub_folder_path='system_pricing_plan')
    print(f'FINISHED'.rjust(120, '.'))
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
def divvy_get_vehicle_types():
    print(f'STARTING'.rjust(120, '.'))
    DivvyBikesCall('divvy_get_vehicle_types')
    print(f'FINISHED'.rjust(120, '.'))


def silver_vehicle_types():
    print(f'STARTING'.rjust(120, '.'))
    # sleep(10)
    DivvyBikesCall('silver_vehicle_types')
    print(f'FINISHED'.rjust(120, '.'))


def gold_vehicle_types():
    print(f'STARTING'.rjust(120, '.'))
    # sleep(10)
    DivvyBikesCall('gold_vehicle_types')
    print(f'FINISHED'.rjust(120, '.'))


def divvy_clean_vehicle_types():
    print(f'STARTING'.rjust(120, '.'))
    DivvyBikesCall('clean_raw_data', sub_folder_path='vehicle_types')
    print(f'FINISHED'.rjust(120, '.'))


def pre_divvy_clean_vehicle_types():
    print(f'STARTING'.rjust(120, '.'))
    DivvyBikesCall('clean_raw_data', sub_folder_path='vehicle_types')
    print(f'FINISHED'.rjust(120, '.'))
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
with DAG(
    dag_id='DivvyBikesDag',
    schedule_interval='*/10 * * * *',
    concurrency=2,
    start_date=pendulum.datetime(2024, 2, 10, tz='America/Sao_Paulo'),
    catchup=False,
    tags=['DivvyBikes']
) as dag:
    # ------------------------------------------------------------------------------------------------------------------
    divvy_get_bike_status = PythonOperator(
        task_id='GetBikeStatus',
        python_callable=divvy_get_bike_status
    )
    silver_bike_status = PythonOperator(
        task_id='SilverBikeStatus',
        python_callable=silver_bike_status,
        retries=2,
        retry_delay=5,
    )
    gold_bike_status = PythonOperator(
        task_id='GoldBikeStatus',
        python_callable=gold_bike_status,
        retries=2,
        retry_delay=5,
    )
    divvy_clean_bike_status = PythonOperator(
        task_id='CleanBikeStatus',
        python_callable=divvy_clean_bike_status
    )
    pre_divvy_clean_bike_status = PythonOperator(
        task_id='PreCleanBikeStatus',
        python_callable=pre_divvy_clean_bike_status
    )
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    divvy_get_station_information = PythonOperator(
        task_id='GetStationInformation',
        python_callable=divvy_get_station_information
    )
    silver_station_information = PythonOperator(
        task_id='SilverStationInformation',
        python_callable=silver_station_information,
        retries=2,
        retry_delay=5,
    )
    gold_station_information = PythonOperator(
        task_id='GoldStationInformation',
        python_callable=gold_station_information,
        retries=2,
        retry_delay=5,
    )
    divvy_clean_station_information = PythonOperator(
        task_id='CleanStationInformation',
        python_callable=divvy_clean_station_information
    )
    pre_divvy_clean_station_information = PythonOperator(
        task_id='PreCleanStationInformation',
        python_callable=pre_divvy_clean_station_information
    )
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    divvy_get_station_staus = PythonOperator(
        task_id='GetStationStatus',
        python_callable=divvy_get_station_staus
    )
    silver_station_staus = PythonOperator(
        task_id='SilverStationStatus',
        python_callable=silver_station_staus,
        retries=2,
        retry_delay=5,
    )
    gold_station_staus = PythonOperator(
        task_id='GoldStationStatus',
        python_callable=gold_station_staus,
        retries=2,
        retry_delay=5,
    )
    divvy_clean_station_staus = PythonOperator(
        task_id='CleanStationStatus',
        python_callable=divvy_clean_station_staus
    )
    pre_divvy_clean_station_staus = PythonOperator(
        task_id='PreCleanStationStatus',
        python_callable=pre_divvy_clean_station_staus
    )
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    divvy_get_system_pricing = PythonOperator(
        task_id='GetSystemPricing',
        python_callable=divvy_get_system_pricing
    )
    silver_system_pricing = PythonOperator(
        task_id='SilverSystemPricing',
        python_callable=silver_system_pricing,
        retries=2,
        retry_delay=5,
    )
    gold_system_pricing = PythonOperator(
        task_id='GoldSystemPricing',
        python_callable=gold_system_pricing,
        retries=2,
        retry_delay=5,
    )
    divvy_clean_system_pricing = PythonOperator(
        task_id='CleanSystemPricing',
        python_callable=divvy_clean_system_pricing
    )
    pre_divvy_clean_system_pricing = PythonOperator(
        task_id='PreCleanSystemPricing',
        python_callable=pre_divvy_clean_system_pricing
    )
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    divvy_get_vehicle_types = PythonOperator(
        task_id='GetVehicleTypes',
        python_callable=divvy_get_vehicle_types
    )
    silver_vehicle_types = PythonOperator(
        task_id='SilverVehicleTypes',
        python_callable=silver_vehicle_types,
        retries=2,
        retry_delay=5,
    )
    gold_vehicle_types = PythonOperator(
        task_id='GoldVehicleTypes',
        python_callable=gold_vehicle_types,
        retries=2,
        retry_delay=5,
    )
    divvy_clean_vehicle_types = PythonOperator(
        task_id='CleanVehicleTypes',
        python_callable=divvy_clean_vehicle_types
    )
    pre_divvy_clean_vehicle_types = PythonOperator(
        task_id='PreCleanVehicleTypes',
        python_callable=pre_divvy_clean_vehicle_types
    )
    # ------------------------------------------------------------------------------------------------------------------

    # ------------------------------------------------------------------------------------------------------------------
    divvy_set_delta_tables = PythonOperator(
        task_id='SetDeltaTables',
        python_callable=divvy_set_delta_tables,
        task_concurrency=1,
    )
    # ------------------------------------------------------------------------------------------------------------------

    divvy_set_delta_tables >> silver_station_information >> divvy_clean_station_information >> gold_station_information
    divvy_set_delta_tables >> silver_bike_status >> divvy_clean_bike_status >> gold_bike_status
    divvy_set_delta_tables >> silver_system_pricing >> divvy_clean_system_pricing >> gold_system_pricing
    divvy_set_delta_tables >> silver_station_staus >> divvy_clean_station_staus >> gold_station_staus
    divvy_set_delta_tables >> silver_vehicle_types >> divvy_clean_vehicle_types >> gold_vehicle_types

    pre_divvy_clean_system_pricing >> divvy_get_system_pricing >> divvy_set_delta_tables
    pre_divvy_clean_station_staus >> divvy_get_station_staus >> divvy_set_delta_tables
    pre_divvy_clean_station_information >> divvy_get_station_information >> divvy_set_delta_tables
    pre_divvy_clean_bike_status >> divvy_get_bike_status >> divvy_set_delta_tables
    pre_divvy_clean_vehicle_types >> divvy_get_vehicle_types >> divvy_set_delta_tables

