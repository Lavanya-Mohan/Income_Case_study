from datetime import datetime

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.amazon.aws.operators.emr import EmrCreateJobFlowOperator
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from airflow.providers.amazon.aws.operators.emr import EmrTerminateJobFlowOperator
from airflow.utils.dates import days_ago

import boto3
from botocore.config import Config

# config = Config(region_name='us-east-2')
# client = boto3.client('s3', config=config)
client = boto3.client('s3', region_name='us-east-2')



DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['lavanyamohan66@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
}

JOB_FLOW_OVERRIDES = {
    'Name': 'readfroms3-emr',
    'ReleaseLabel': 'emr-6.8.0',
    'Applications': [
        {"Name": 'Hadoop'},
        {'Name': 'Spark'}
    ],    
    'Instances': {
        'InstanceGroups': [
            {
                'Name': "Master node",
                'Market': 'SPOT',
                'InstanceRole': 'MASTER',
                'InstanceType': 'm5.xlarge',
                'InstanceCount': 1,
            },
            {
                'Name': "Worker nodes",
                'Market': 'SPOT',
                'InstanceRole': 'CORE',
                'InstanceType': 'm5.xlarge',
                'InstanceCount': 3,
            }
        ],
        'KeepJobFlowAliveWhenNoSteps': True,
        'TerminationProtected': False,
        'Ec2KeyName': 'emr-keypair',
    },
    'VisibleToAllUsers': True,
    'JobFlowRole': 'EMR_EC2_DefaultRole',
    'ServiceRole': 'EMR_DefaultRole',
    'LogUri': 's3://income-sales-data/airflow/airflowlogs/emr/',
    
}

SPARK_STEPS = [
    {
        'Name': 's3-row-count',
        'ActionOnFailure': 'CANCEL_AND_WAIT',
        'HadoopJarStep': { 
            'Jar': 'command-runner.jar',
            'Args': ['spark-submit', 
                     '--deploy-mode', 'client', 
                     '--jars', 's3://income-sales-data/airflow/scripts/mysql-connector-j-8.3.0.jar',
                     's3://income-sales-data/airflow/scripts/transform_etl.py'
                    ]
        }
    }
]

with DAG(
    dag_id='datapipeline_DAG',
    description='Apache Airflow orchestrates Spark workflow in EMR cluster',
    default_args=DEFAULT_ARGS,
    start_date=days_ago(1),
    schedule_interval='@daily',
    tags=['datapipeline']
) as dag:

    begin = DummyOperator(
        task_id='begin_workflow'
    )

    create_cluster = EmrCreateJobFlowOperator(
        task_id='create_emr_cluster',
        job_flow_overrides=JOB_FLOW_OVERRIDES,
        aws_conn_id='aws_default',
        emr_conn_id='emr_default',
    )

    add_step = EmrAddStepsOperator(
        task_id='submit_spark_application',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        aws_conn_id='aws_default',
        steps=SPARK_STEPS,
    )

    check_step = EmrStepSensor(
        task_id='check_submission_status',
        job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
        step_id="{{ task_instance.xcom_pull(task_ids='submit_spark_application', key='return_value')[0] }}",
        aws_conn_id='aws_default',
    )

    remove_cluster = EmrTerminateJobFlowOperator(
        task_id='terminate_emr_cluster',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        aws_conn_id='aws_default',
    )

    end = DummyOperator(
        task_id='end_workflow'
    )

    begin >> create_cluster >> add_step >> check_step >> remove_cluster >> end


