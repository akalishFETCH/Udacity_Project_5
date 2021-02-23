import pandas as pd
import boto3
import os
import time
import datetime
import logging
import json

from airflow import DAG, settings
from airflow.models import Variable, Connection
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator

from libs import sql_statements

AWS_KEY = os.environ.get('AWS_KEY_PERSONAL')
AWS_SECRET = os.environ.get('AWS_SECRET_PERSONAL')
#AWS_KEY = os.environ.get('AWS_KEY_FETCH')
#AWS_SECRET = os.environ.get('AWS_SECRET_FETCH')
DWH_CLUSTER_TYPE='multi-node'
DWH_NUM_NODES='4'
DWH_NODE_TYPE='dc2.large'

DWH_IAM_ROLE_NAME='dwhRole'
DWH_CLUSTER_IDENTIFIER='dwhCluster'
DWH_DB='dwh'
DWH_DB_USER='dwhuser'
DWH_DB_PASSWORD='Passw0rd'
DWH_PORT='5439'

def redshift_cluster_creator(**kwargs):
    ec2 = boto3.resource('ec2',
                       region_name="us-west-2",
                       aws_access_key_id=AWS_KEY,
                       aws_secret_access_key=AWS_SECRET
                    )
    iam = boto3.client('iam',
                     region_name="us-west-2",
                     aws_access_key_id= AWS_KEY,
                     aws_secret_access_key= AWS_SECRET
                  )
    redshift = boto3.client('redshift',
                       region_name="us-west-2",
                       aws_access_key_id= AWS_KEY,
                       aws_secret_access_key= AWS_SECRET
                       )

    from botocore.exceptions import ClientError
    try:
        logging.info("1.1 Creating a new IAM Role")
        dwhRole = iam.create_role(
            Path='/',
            RoleName=DWH_IAM_ROLE_NAME,
            Description = "Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps(
                {'Statement': [{'Action': 'sts:AssumeRole',
                   'Effect': 'Allow',
                   'Principal': {'Service': 'redshift.amazonaws.com'}}],
                 'Version': '2012-10-17'})
        )
    except Exception as e:
        logging.info(e)

    iam.attach_role_policy(RoleName=DWH_IAM_ROLE_NAME,
                           PolicyArn="arn:aws:iam::aws:policy/AdministratorAccess"
                          )['ResponseMetadata']['HTTPStatusCode']

    roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']

    try:
        response = redshift.create_cluster(
            #HW
            ClusterType=DWH_CLUSTER_TYPE,
            NodeType=DWH_NODE_TYPE,
            NumberOfNodes=int(DWH_NUM_NODES),

            #Identifiers & Credentials
            DBName=DWH_DB,
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
            MasterUsername=DWH_DB_USER,
            MasterUserPassword=DWH_DB_PASSWORD,

            #Roles (for s3 access)
            IamRoles=[roleArn]
        )
    except Exception as e:
        logging.info(e)

    myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
    count_cycles = 0
    while myClusterProps.get('ClusterStatus') == 'creating':
        logging.info(myClusterProps.get('ClusterStatus'))
        count_cycles += 1
        if count_cycles == 7:
            print("Check connection")
            break
        else:
            time.sleep(90)
            myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
    if count_cycles ==7:
        ""
    else:
        logging.info(myClusterProps.get('ClusterStatus'))

    DWH_ENDPOINT = myClusterProps['Endpoint']['Address']
    DWH_ROLE_ARN = myClusterProps['IamRoles'][0]['IamRoleArn']

    try:
        vpc = ec2.Vpc(id=myClusterProps['VpcId'])
        defaultSg = list(vpc.security_groups.all())[0]
        logging.info(defaultSg)
        defaultSg.authorize_ingress(
            GroupName=defaultSg.group_name,
            CidrIp='73.74.224.251/32',
            IpProtocol='TCP',
            FromPort=int(DWH_PORT),
            ToPort=int(DWH_PORT)
            )
    except Exception as e:
        logging.info(e)

    return DWH_ENDPOINT

def connection_maker(**kwargs):
    ti = kwargs['ti']
    DWH_ENDPOINT = ti.xcom_pull(task_ids='create_redshift_cluster')
    conn = Connection(
        conn_id='redshift',
        conn_type='Postgres',
        host=DWH_ENDPOINT,
        schema=DWH_DB,
        login=DWH_DB_USER,
        password=DWH_DB_PASSWORD,
        port=DWH_PORT
    ) #create a connection object
    session = settings.Session() # get the session
    session.add(conn)
    session.commit()

    conn = Connection(
        conn_id='aws_credentials',
        conn_type='Amazon Web Services',
        login=AWS_KEY,
        password=AWS_SECRET
        )
    session = settings.Session() # get the session
    session.add(conn)
    session.commit()

"""def redshift_cluster_delete():
        iam = boto3.client('iam',aws_access_key_id=AWS_KEY,
                     aws_secret_access_key=AWS_SECRET
                  )

        redshift = boto3.client('redshift',
                       aws_access_key_id=AWS_KEY,
                       aws_secret_access_key=AWS_SECRET
                       )
        redshift.delete_cluster( ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,  SkipFinalClusterSnapshot=True)
        myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
        count_cycles = 0
        while myClusterProps.get('ClusterStatus') == 'deleting':
            try:
                myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
            except Exception as e:
                logging.info(e)
            logging.info(myClusterProps.get('ClusterStatus'))
            count_cycles += 1
            if count_cycles == 7:
                logging.info("Check connection")
                break
            else:
                time.sleep(90)
        iam.detach_role_policy(RoleName=DWH_IAM_ROLE_NAME, PolicyArn="arn:aws:iam::aws:policy/AdministratorAccess")
        iam.delete_role(RoleName=DWH_IAM_ROLE_NAME)"""

dag = DAG(
        'redshift_dag',
        start_date=datetime.datetime.now())

redshift_cluster_maker = PythonOperator(
    task_id='create_redshift_cluster',
    python_callable=redshift_cluster_creator,
    dag=dag,
    provide_context=True
    )

redshift_connection = PythonOperator(
    task_id = 'create_connection_to_redshift',
    python_callable=connection_maker,
    dag=dag
    )

dump_table = PostgresOperator(
    task_id="delete_tables",
    dag=dag,
    postgres_conn_id="redshift",
    sql=sql_statements.staging_events_table_drop
)

create_table = PostgresOperator(
    task_id="create_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql=sql_statements.staging_events_table_create
)

copy_to_redshift = S3ToRedshiftOperator(
    s3_bucket='udacity-dend',
    s3_key='song-data/A/A/B/',
    schema="PUBLIC",
    table='staging_events_table',
    task_id='transfer_s3_to_redshift',
    aws_conn_id='aws_credentials',
    redshift_conn_id='redshift',
    dag=dag
    )

"""redshift_cluster_deleter = PythonOperator(
    task_id='delete_redshift_cluster',
    python_callable=redshift_cluster_delete,
    dag=dag,
    provide_context=True
    )"""

redshift_cluster_maker >> redshift_connection
redshift_connection >> dump_table
dump_table >> create_table
create_table >> copy_to_redshift
"""copy_to_redshift >> redshift_cluster_deleter"""
