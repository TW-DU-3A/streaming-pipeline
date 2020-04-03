from __future__ import print_function
from airflow.models import DAG
from datetime import datetime
from airflow.sensors import WebHdfsSensor
from airflow.operators.python_operator import PythonOperator
from hdfs import client
import pandas as pd
import sys, os
import tempfile
import glob
import requests
from io import StringIO
from airflow.hooks.base_hook import BaseHook

sys.path.insert(0, os.path.abspath(os.path.dirname("api")))

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


def read_hdfs_file():
    connection = BaseHook.get_connection("webhdfs_default")
    conn_type = connection.conn_type
    host = connection.host
    port = connection.port
    hdfs_conn = str(conn_type) + "://" + str(host) + ":" + str(port)
    print("connecting to " + hdfs_conn)
    myclient = client.Client(hdfs_conn)
    pathname = os.path.join(tempfile.mkdtemp(), 'hdfs_stationMart_data')
    print("downloading files from HDFS to " + pathname)
    myclient.download("/tw/stationMart/data/", pathname, True)
    print("downloading complete")
    all_files = glob.glob(pathname + "/*.csv")
    if len(all_files) > 0 and os.path.getsize(all_files[0]) > 0:
        actual_df = pd.read_csv(all_files[0])
        print(actual_df)
        return actual_df
    print("No files downloaded..")
    return None


def read_mock_data():
    print("Fetching mock response text...")
    connection = BaseHook.get_connection("MOCKSERVER")
    conn_type = connection.conn_type
    host = connection.host
    port = connection.port
    mockserver_conn = str(conn_type) + "://" + str(host) + ":" + str(port) + "/" + "mock-response"
    print("connecting to " + mockserver_conn)
    r = requests.get(mockserver_conn)
    StringData = StringIO(r.text)
    df = pd.read_csv(StringData, sep=",")
    return df


def compare_data(**kwargs):
    mock_df = read_mock_data()
    actual_df = read_hdfs_file()
    result = actual_df.equals(mock_df)
    print("Result: " + str(result))
    if not result:
        raise ValueError('mart data did not match with reference data!')
    return result


comparison_task = PythonOperator(
    task_id='compare_mock_with_martdata'
    , python_callable=compare_data
    , provide_context=True
    , dag=dag
)

file_sensor >> comparison_task
