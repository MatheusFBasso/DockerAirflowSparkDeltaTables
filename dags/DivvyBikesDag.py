from airflow import DAG
from airflow.operators.python import PythonOperator
from lib.Transformations.DivvyBikes.ClassesCall import DivvyBikesCall
import pendulum

# ----------------------------------------------------------------------------------------------------------------------
def divvy_get_bike_status():
    print(f'STARTING'.rjust(120, '.'))
    DivvyBikesCall('divvy_get_bike_status')
    print(f'FINISHED'.rjust(120, '.'))
# ----------------------------------------------------------------------------------------------------------------------
def divvy_set_delta_tables():
    print(f'STARTING'.rjust(120, '.'))
    DivvyBikesCall('divvy_set_delta_tables')
    print(f'FINISHED'.rjust(120, '.'))
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
with DAG(
    dag_id='DivvyBikesDag',
    schedule_interval='10 15 * * *',
    concurrency=1,
    start_date=pendulum.datetime(2024, 2, 10, tz='America/Sao_Paulo'),
    catchup=False,
    tags=['DivvyBikes']
) as dag:
    # ------------------------------------------------------------------------------------------------------------------
    divvy_get_bike_status = PythonOperator(
        task_id='GetBikeStatus',
        python_callable=divvy_get_bike_status
    )
    # ------------------------------------------------------------------------------------------------------------------
    divvy_set_delta_tables = PythonOperator(
        task_id='SetDeltaTables',
        python_callable=divvy_set_delta_tables,
        task_concurrency=1,
    )
    # ------------------------------------------------------------------------------------------------------------------

    divvy_set_delta_tables >> divvy_get_bike_status