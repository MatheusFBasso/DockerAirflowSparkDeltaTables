# Delta Table using Airflow with Docker
A project to study Delta Tables using an ETL/ELT process and Airflow with Docker

![ariflowspark drawio](https://github.com/user-attachments/assets/6953e613-b441-4418-bbd8-f64017c1a336)


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

## Non ARM-based processors:
  Please change the Dockerfile for the base processor that you are using,
  arm64 is used on ARM-based processors.
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

## Medallion Architecture (Multi-hop)

It uses a multi-hop architecture, which means that we have bronze, silver, and gold layers.

## Divvy Bikes Runs and Orchestration


![Screenshot 2024-11-02 at 00 43 45](https://github.com/user-attachments/assets/f7f66a68-50b2-488d-a66d-f72d28dec794)

- 1 - [Orchestration](dags/DivvyBikesDag.py) works on a single file per execution, in this case, we can't have two extractions, and for that reason, there's a clean that will backup any files in the bronze path.
- 2 - Extract the data to the designated path as JSON. [Code link](plugins/lib/APIs/DivvyBikes_api.py)
- 3 - Create Silver, Gold, and Log tables if not created as the Database. [Code link](plugins/lib/Transformations/DivvyBikes/TableCreation.py)
- 4 - The silver part, loads the JSON and saves it via insert into the main silver table. [Code link](plugins/lib/Transformations/DivvyBikes/Silver.py)
- 5 - Back up the files, cleaning the folder for further use. [Code link](plugins/lib/utils/DivvyBikes/CleanRawData.py)
- 6 - Updates the Gold tables, which are the Silver but in "real-time", in the code the update interval is set to 10 min. [Code link](plugins/lib/Transformations/DivvyBikes/Gold.py)

For the Extraction, we have the following log:

![image](https://github.com/user-attachments/assets/4d97f3ee-7690-4edd-b49a-ed6c2a29dfd9)

For the Table Creation:

![image](https://github.com/user-attachments/assets/33523448-763c-4793-9846-ed94d98476b3)

![image](https://github.com/user-attachments/assets/6068b763-577e-4e4b-bd1d-b0270f835e12)

![image](https://github.com/user-attachments/assets/a09b6234-d3eb-4ff4-b20d-867823565aa8)

## About the logs
The log will inform the date with hour of each step, to help in case of an error.

## Data checking

![image](https://github.com/user-attachments/assets/c8628501-6c26-4ab0-aed0-6e18bb21b54f)




