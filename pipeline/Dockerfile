FROM apache/airflow:latest-python3.8
USER root
RUN apt-get update && \
    apt-get clean && \
    apt-get install vim-tiny -y && \
    apt-get autoremove -yqq --purge && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*
USER airflow
ENV PYTHONPATH "${PYTHONPATH}:${AIRFLOW_HOME}"
COPY requirements.txt /tmp/requirements.txt
COPY ./airflow.cfg ./airflow.cfg
RUN pip install -r /tmp/requirements.txt
