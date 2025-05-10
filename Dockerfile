FROM apache/airflow:3.0.0-python3.11

ENV PIP_USER=false
COPY requirements.txt /requirements.txt
RUN python3 -m venv /opt/airflow/brew_env
RUN pip install -r /requirements.txt

USER root
# Install OpenJDK-17
RUN apt update && \
    apt-get install -y openjdk-17-jdk && \
    apt-get install -y ant && \
    apt-get clean;

# Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64/
RUN export JAVA_HOME

USER airflow
