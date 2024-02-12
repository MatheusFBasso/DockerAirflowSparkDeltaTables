from airflow import DAG
from airflow.operators.python import PythonOperator
import pendulum
from lib.brewery.delta.brew_classes import BreweryClasses


def brew_exctract_api():
    print(f'{"."*120}\n STARTING '.center(120, '.'))
    BreweryClasses('brew_extract_api')
    print(f'{"."*120}\nFINISHED '.center(120, '.'))


def brew_delta_tables():
    print(f'{"."*120}\nSTARTING '.center(120, '.'))
    BreweryClasses('brew_set_delta_tables')
    print(f'{"."*120}\nFINISHED '.center(120, '.'))


def brew_clean_bronze():
    print(f'{"."*120}\nSTARTING '.center(120, '.'))
    BreweryClasses('brew_clean_bronze')
    print(f'{"."*120}\nFINISHED '.center(120, '.'))


def brew_silver():
    print(f'{"."*120}\nSTARTING '.center(120, '.'))
    BreweryClasses('brew_silver')
    print(f'{"."*120}\nFINISHED '.center(120, '.'))


def brew_gold():
    print(f'{"."*120}\nSTARTING '.center(120, '.'))
    BreweryClasses('brew_gold')
    print(f'{"."*120}\nFINISHED '.center(120, '.'))


with DAG(
    dag_id='BreweryDelta',
    schedule_interval='10 15 * * *',
    start_date=pendulum.datetime(2024, 2, 10, tz='America/Sao_Paulo'),
    catchup=False,
    tags=['BreweryDelta']
) as dag:

    brew_extract = PythonOperator(
        task_id='brew_extract_api',
        python_callable=brew_exctract_api
    )

    brew_clean_bronze = PythonOperator(
        task_id='brew_clean_bronze',
        python_callable=brew_clean_bronze
    )

    brew_delta_tables = PythonOperator(
        task_id='brew_delta_tables',
        python_callable=brew_delta_tables
    )

    brew_silver = PythonOperator(
        task_id='brew_silver',
        python_callable=brew_silver
    )

    brew_gold = PythonOperator(
        task_id='brew_gold',
        python_callable=brew_gold
    )

    brew_extract >> brew_silver >> brew_clean_bronze
    brew_delta_tables >> brew_silver >> brew_gold
