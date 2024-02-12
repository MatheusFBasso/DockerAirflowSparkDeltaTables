# Delta Table using Airflow with Docker
A project to study Delta Tables using an ETL/ELT process and Airflow with Docker

![Infra](https://github.com/MatheusFBasso/DockerAirflowSparkDeltaTables/assets/62318283/f83de518-0209-4ca5-a670-e02db5c17ce2)

# Project details
- Used Docker Compose with Airflow for better control over the resources
- PySpark with Delta to better understand how Delta Tables work
- Used ELT (Extract, Transform and Load)

# 1 - [Docker Compose](docker-compose.yaml)
- image: docker_airflow_delta:2.8.1-python3.11
- name: docker_airflow_delta

The rest of the configuration was not changed from the standard.

# 2 - [Docker File](Dockerfile)
- requirements: will install the [requirements.txt](requirements.txt)
- Java for Spark: will install openjdk-17

## Non ARM based processors:
  Please change de Dockerfile for the base processor that you are using,
  arm64 is used on ARM based processors.
  ```
  # Set JAVA_HOME
  ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-arm64/
  RUN export JAVA_HOME
  ```

## Commands for Docker creation:
- Creating needed dirs:
```
mkdir config warehouse logs delta_lake/brewery/raw_data delta_lake/brewery/raw_data_bkp
```

- Creating the image:

```
docker-build -t docker_airflow_delta:2.8.1-python3.11 .
```

- Creating and running docker:

```
docker-compose up -d
```

# 3 - [Airflow](dags/brew_dag.py)

Orchestration

![Airflow](https://github.com/MatheusFBasso/DockerAirflowSparkDeltaTables/assets/62318283/d3fd8a72-26c8-4fd1-8c23-b0c3a620c65a)

To ease the use of the multiple classes created there is one that by passing only the name that we need will execute the desired class. [brew_classes](plugins/lib/brewery/delta/brew_classes.py)

# 4 - Extract [Bronze](plugins/lib/brewery/api_breweries/get_data_breweries.py)

Using Python's `request` library to consume data via a Brewery API to get the data as json.

# 5 - Load [Bronze](plugins/lib/brewery/delta/clean_raw_data.py)

Saves the json files on a local folder shared on both docker and local computer called `delta_lake`:
  - `delta_lake`
    - `brewery`
      - `raw_data`
      - `raw_data_bkp`
   
The *Extract* will save the json files on the `delta_lake/brewery/raw_data` with the data on their names.

After the *Silver* the files are moved to the `delta_lake/brewery/raw_data_bkp` creating a sub-folder with the date name, and also deleting old backups within a set period of 7 days.

# 6 - Transform

## 6.1 - [Creating Delta Tables](plugins/lib/brewery/delta/SetDeltaTables.py)

Before running the silver table the Delta Tables used must be created for use.

## 6.2 - [SILVER](plugins/lib/brewery/delta/raw_files_to_silver.py)

The json files on the `delta_lake/brewery/raw_data` will be transformed into PySpark DataFrame and with a few treatments to prepare and load the data into the Delta Tables previously created, with a new column named `date_ref_carga` so that we can keep the old information available.

## 6.3 - [GOLD](plugins/lib/brewery/delta/silver_to_gold.py)

After the creation and population of the silver Delta Table the gold will create multiple dim tables and a fact table. The reason behind this creation is to better understand the process and analysis using PySpark along with Delta Tables and further increase read time after.
For further updates will be a way to load the warehouse.
