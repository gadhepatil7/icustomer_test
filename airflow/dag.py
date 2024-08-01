import logging
from datetime import datetime, timedelta
# from adp.airflow.settings import ADP_DAG_DEFAULT
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.operators.emr_create_job_flow_operator import EmrCreateJobFlowOperator
from airflow.contrib.sensors.emr_job_flow_sensor import EmrJobFlowSensor
from airflow.providers.amazon.aws.sensors.emr_step import EmrStepSensor
from airflow.providers.amazon.aws.operators.emr_terminate_job_flow import EmrTerminateJobFlowOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.operators.dummy_operator import DummyOperator
import ast
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
from operators.ms_teams_webhook_operator import MSTeamsWebhookOperator
from airflow.sensors.s3_key_sensor import S3KeySensor
from gen_data.genrate_data import genrate_data_store_to_path
from clean.clean import clean
from ingest.ingest import ingest
from transform.transform import transform
from airflow.operators.python_operator import BranchPythonOperator





dag_name = 'interactions'

#run the dag at 10 PM every day
with DAG(
    dag_id=dag_name,
    dagrun_timeout=timedelta(hours=30),
    start_date=days_ago(3),
    schedule_interval="0 22 * * *", 
    catchup=True,
    max_active_runs=1,
    tags=['piq'],
) as dag:
    start = DummyOperator(
        task_id='start'
    )
    end = DummyOperator(
        task_id='end'
    )
    #python oprator calling script to generate data
    genrate_data = PythonOperator(
        task_id='genrate_data',
        python_callable=genrate_data_store_to_path,
        op_args=[1000, 'data.csv']
    )
    #python oprator calling script to ingest data
    ingest_data = PythonOperator(
        task_id='ingest_data',
        python_callable=ingest,
        op_args=['data.csv', 'rep_db.db', 'rep_interaction']
    )
    #python oprator calling script to clean data
    clean_data = PythonOperator(
        task_id='clean_data',
        python_callable=clean,
        op_args=['rep_db.db', 'rep_interaction', 'raw_db.db', 'raw_interaction']
    )
    #python oprator calling script to transform data
    transform_data = PythonOperator(
        task_id='transform_data',
        python_callable=transform,
        op_args=['raw_db.db', 'raw_interaction', 'pub_db.db', 'pub_interaction']
    )
    start>> genrate_data >> ingest_data >> clean_data >> transform_data >> end



