# Delta Table using Airflow with Docker
A project to study Delta Tables using an ETL/ELT process and Airflow with Docker

![ariflowspark drawio (1)](https://github.com/MatheusFBasso/DockerAirflowSparkDeltaTables/assets/62318283/158817d4-9d9a-442e-8bf9-6129e03bbf21)


# Set-up: MacBook M1
- Python version: 3.11
- Airflow version: 2.8.1
- PySpark: 3.5.0
- Delta Spark: 3.1.0

# Non ARM based processors:
Please change de Dockerfile for the base processor that you are using,
arm64 is used on ARM based processors.
```
# Set JAVA_HOME
ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-arm64/
RUN export JAVA_HOME
```

# Docker (v4.27.1) set-up:
Commands used on terminal
```
docker build -t airflow_brewery:2.8.1-python3.11 .
```
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
