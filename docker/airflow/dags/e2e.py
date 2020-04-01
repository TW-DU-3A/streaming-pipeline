from __future__ import print_function

import os
import sys
from airflow.models import DAG
from datetime import datetime
from airflow.sensors import WebHdfsSensor

sys.path.insert(0,os.path.abspath(os.path.dirname("api")))

DAG_ID = os.path.basename(__file__).replace(".pyc", "").replace(".py", "")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 0,
}

dag = DAG(DAG_ID, default_args=default_args, schedule_interval=None,
          start_date=(datetime(2020, 3, 16, 0, 0, 0, 0)), catchup=False)

input_path = '/tw/stationMart/data'
file_sensor = WebHdfsSensor(
    task_id='file_exists',
    filepath=input_path,
    webhdfs_conn_id='webhdfs_default',
    dag=dag)
