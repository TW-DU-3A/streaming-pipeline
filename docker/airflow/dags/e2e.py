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
import pyarrow.parquet as pq
from pandas._testing import assert_frame_equal

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', -1)

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
    dataset = pq.ParquetDataset(pathname)
    table = dataset.read()
    df = table.to_pandas()
    print(df)
    return df


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
    df = pd.read_csv(StringData, sep=",", parse_dates=["timestamp"])
    df["bikes_available"] = df["bikes_available"].astype("int32")
    df["docks_available"] = df["docks_available"].astype("int32")
    print(df)
    return df


def compare_data(**kwargs):
    mock_df = read_mock_data()
    print(mock_df.dtypes)
    actual_df = read_hdfs_file()
    print(actual_df.dtypes)

    print("asserting...")
    assert_frame_equal(actual_df, mock_df, check_dtype=True)
    return True


comparison_task = PythonOperator(
    task_id='compare_mock_with_martdata'
    , python_callable=compare_data
    , provide_context=True
    , dag=dag
)

file_sensor >> comparison_task
