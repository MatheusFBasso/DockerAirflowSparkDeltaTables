from airflow import DAG
from airflow.operators.python import PythonOperator
from lib.Transformations.Brewery.ClassesCall import BreweryClasses
import pendulum

# ----------------------------------------------------------------------------------------------------------------------
def brew_create_tables():
    print(f'STARTING'.rjust(120, '.'))
    BreweryClasses('brew_set_delta_tables')
    print(f'FINISHED'.rjust(120, '.'))
# ----------------------------------------------------------------------------------------------------------------------
def brew_exctract_api():
    print(f'STARTING'.rjust(120, '.'))
    BreweryClasses('brew_api_extract')
    print(f'FINISHED'.rjust(120, '.'))
# ----------------------------------------------------------------------------------------------------------------------
def brew_clean_raw_data():
    print(f'STARTING'.rjust(120, '.'))
    BreweryClasses('brew_clean_raw_data')
    print(f'FINISHED'.rjust(120, '.'))
# ----------------------------------------------------------------------------------------------------------------------
def brew_silver():
    print(f'STARTING'.rjust(120, '.'))
    BreweryClasses('brew_silver')
    print(f'FINISHED'.rjust(120, '.'))
# ----------------------------------------------------------------------------------------------------------------------
def brew_gold_type_total():
    print(f'STARTING'.rjust(120, '.'))
    BreweryClasses('gold_brewery_types_total')
    print(f'FINISHED'.rjust(120, '.'))
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
with DAG(
    dag_id='BreweryDag',
    schedule_interval='10 15 * * *',
    concurrency=1,
    start_date=pendulum.datetime(2024, 2, 10, tz='America/Sao_Paulo'),
    catchup=False,
    tags=['Brewery']
) as dag:
    # ------------------------------------------------------------------------------------------------------------------
    brew_create_tables = PythonOperator(
        task_id='CreateDeltaTables',
        python_callable=brew_create_tables
    )
    # ------------------------------------------------------------------------------------------------------------------
    brew_extract = PythonOperator(
        task_id='RunBreweryAPI',
        python_callable=brew_exctract_api,
        task_concurrency=1,
    )
    # ------------------------------------------------------------------------------------------------------------------
    move_files_task = PythonOperator(
        task_id='MoveFiles',
        python_callable=brew_clean_raw_data,
        task_concurrency=1,
    )
    # ------------------------------------------------------------------------------------------------------------------
    brew_silver = PythonOperator(
        task_id='Silver',
        python_callable=brew_silver
    )
    # ------------------------------------------------------------------------------------------------------------------
    brew_type_total = PythonOperator(
        task_id='GoldBreweryTypeTotal',
        python_callable=brew_gold_type_total,
        task_concurrency=1,
    )
    # ------------------------------------------------------------------------------------------------------------------

    brew_extract >> brew_silver >> move_files_task
    brew_create_tables >> brew_silver >> brew_type_total