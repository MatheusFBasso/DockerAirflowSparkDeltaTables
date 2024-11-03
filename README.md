# Delta Table using Airflow with Docker
A project to study Delta Tables using an ETL/ELT process and Airflow with Docker

![ariflowspark drawio (2)](https://github.com/user-attachments/assets/ac3f323e-28bd-4a92-bd5a-fb6e0f0ea1c5)


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

## Medallion Architecture (Multi-hop) - Divvy Bikes

It uses a multi-hop architecture, meaning we have bronze, silver, and gold layers.

## Divvy Bikes Runs and Orchestration

![Screenshot 2024-11-03 at 04 28 33](https://github.com/user-attachments/assets/8cc2436e-93f1-4476-80ed-a4eb248ed0af)



- 1 -> [Orchestration](dags/DivvyBikesDag.py) works on a single file per execution, in this case, we can't have two extractions, and for that reason, there's a clean that will backup any files in the bronze path.
- 2 -> Extract the data to the designated path as JSON. [Code link](plugins/lib/APIs/DivvyBikes_api.py)
- 3 -> Create the Bronze Database if it does not exist alongside the tables, and insert the JSON data into the bronze tables. [Code link](plugins/lib/Transformations/DivvyBikes/Bronze.py)
- 4 -> Back up the files, cleaning the folder for further use. [Code link](plugins/lib/utils/DivvyBikes/CleanRawData.py)
- 5 -> Create Silver, Gold, and Log tables if not created as the Database. [Code link](plugins/lib/Transformations/DivvyBikes/TableCreation.py)
- 6 -> The silver part, loads the JSON and saves it via insert into the main silver table. [Code link](plugins/lib/Transformations/DivvyBikes/Silver.py)
- 7 -> Updates the Gold tables, which are the Silver but in "real-time", in the code, the update interval is set to 10 min. [Code link](plugins/lib/Transformations/DivvyBikes/Gold.py)

## Extraction - API

### [Brewery](plugins/lib/APIs/Brewery_api.py)
Iter over pages and save them in a folder for later use, it's in version 1.0 with a log informing time, partitions, and validation.

![image](https://github.com/user-attachments/assets/58ede090-7a0f-4d8b-a15f-03fd803465e2)
![image](https://github.com/user-attachments/assets/118cf40f-a870-42ca-81df-b48a579ac3f5)

### [DivvyBikes](plugins/lib/APIs/DivvyBikes_api.py)
Way simpler than the Brewery API, we just need to get a JSON from a page.

![image](https://github.com/user-attachments/assets/1a1d8941-bf33-4728-86c5-89fe52351347)

## Bronze

### Brewery
No bronze layer yet.

### [Divvy Bikes](plugins/lib/Transformations/DivvyBikes/Bronze.py)
Creates the Database alongside the bronze tables for each file and uploads the raw data to the delta table for later use on the Silver layer.
![image](https://github.com/user-attachments/assets/5412100d-e150-4b32-bd58-895670f4b510)

## Slver

### [Brewery](plugins/lib/Transformations/Brewery/Silver.py)
Reads directly from the JSON files inserting data directly to the silver tables.
![image](https://github.com/user-attachments/assets/3d6dad02-fba7-43ad-9ef8-ca3b94b0fbd4)

### [Divvy Bikes](plugins/lib/Transformations/DivvyBikes/Silver.py)
Reads from the bronze layer, adjusting the fields with explode to create new columns with the correct data type for the gold step. The data is inserted into the correspondent Silver table.
![image](https://github.com/user-attachments/assets/b18303ef-c4f9-43d4-ab6b-8560734237da)
