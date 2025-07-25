FROM apache/airflow:slim-2.11.0-python3.11

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY --chmod=755 entrypoint.sh /entrypoint.sh

USER airflow

ENTRYPOINT ["/entrypoint.sh"]



