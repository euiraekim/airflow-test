FROM apache/airflow:2.0.0-python3.8

EXPOSE 8080

ENTRYPOINT [ "/bin/bash" ]
CMD airflow db init