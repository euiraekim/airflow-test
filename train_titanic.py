from sklearn.preprocessing import LabelEncoder
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score
import pandas as pd

import airflow
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

dag = DAG(
    dag_id='train_titanic',
    start_date=airflow.utils.dates.days_ago(2),
    schedule_interval=None
)

download_dataset = BashOperator(
    task_id='download_dataset',
    bash_command='curl -o /tmp/titanic.csv -L https://raw.githubusercontent.com/datasciencedojo/datasets/master/titanic.csv'
    dag=dag
)

def _preprocessing_data():
    print('preprepredata')

preprocessing_data = PythonOperator(
    task_id='preprocessing_data',
    python_callable=_preprocessing_data
    dag=dag
)

notify = BashOperator(
    task_id='notify',
    bash_command='echo "pipeline is complete."'
    dag=dag
)

download_dataset >> preprocessing_data >> notify
