from __future__ import print_function

import os
import sys
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime

sys.path.insert(0,os.path.abspath(os.path.dirname("api")))
from api import mock_data_api

DAG_ID = os.path.basename(__file__).replace(".pyc", "").replace(".py", "")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 0,
}

dag = DAG(DAG_ID, default_args=default_args, schedule_interval=None,
          start_date=(datetime(2020, 3, 16, 0, 0, 0, 0)), catchup=False)

mock_data_api = BashOperator(task_id="mock_data_api", bash_command="export FLASK_APP=/usr/local/airflow/dags/api/mock_data_api.py && export FLASK_RUN_PORT=8050 && flask run --host=0.0.0.0", dag=dag)
