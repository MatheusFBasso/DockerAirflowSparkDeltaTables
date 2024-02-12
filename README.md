# Delta Table using Airflow with Docker
A project to study Delta Tables using an ETL/ELT process and Airflow with Docker

![image](https://github.com/MatheusFBasso/DockerAirflowSparkDeltaTables/assets/62318283/f83de518-0209-4ca5-a670-e02db5c17ce2)

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
- Creating the image:

```
docker-build -t docker_airflow_delta:2.8.1-python3.11 .
```

- Creating and running docker:

```
docker-compose up -d
```

# Delta Table Test
There is a notebook `tests.ipynb` on the project testing the Delta tables, to better see the output I suggest PyCharm/DataSpell.

For now you can check the tables by their file path using the following code:
```
df = spark.read.format("delta").load("./brew_project/warehouse/silver.db/brewery")
```
With this code is possible to load the Delta table.

The Gold layer is optimized to load queries faster, so we have partitions created on main columns which will cause a relative delay in saving files or updating them, for that reason there's a part of the code that while it prepares the `dim` tables analyses if there are new data to be inserted on which one of them, if there are no new data it won't update the delta tables, so there is no waste on processing.

For further updates will be a way to load the warehouse.
