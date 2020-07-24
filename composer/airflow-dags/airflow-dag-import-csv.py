from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
from datetime import datetime as dt
from google.cloud import storage
import ugclogger
import timeit
import os
import io
################################################ Airflow Dag Configurations ################################################
YESTERDAY=datetime.now() - timedelta(days=1)
DAG_ID='ugc-import-v1'
GCS_BUCKET='<bucket name>'
OWNER="OWNER"
SCHEDULE_INTERVAL=None
log = ugclogger.Slack()
log.Webhook = os.environ["slack"]
log.GcpProject = OWNER
################################################ End Airflow Dag Configurations #############################################
def getFileBlob(file_name):
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(GCS_BUCKET)
    print(file_name)
    blob = bucket.get_blob(f'polishreviews/{file_name}')
    input_blob = blob.download_as_string()
    file_obj = io.BytesIO(input_blob)
    file_as_string = file_obj.read()[1:]
    return file_as_string

def start(**kwargs):
    log.LoggingV1(f'Started import', DAG_ID)
    return timeit.default_timer()

def end(**kwargs):
    ti = kwargs['ti']
    startExec = ti.xcom_pull(task_ids='start')
    endExec = timeit.default_timer()
    execution_time = endExec - startExec
    log.LoggingV1(f'Complated import', DAG_ID, execution_time)

def dcommentImport(**kwargs):
    try:
        fileName='social_proof_D_COMMENT.csv'
        file_as_string = getFileBlob(fileName)
        for line in file_as_string.decode('utf-8-sig',errors = 'ignore').split('\n')[1:]:
            data = line.split('|')
            # store the data in any of the storage
            
        print('dcommentImport called')
    except Exception as e:
        log.ErrorV1(str(e),f'001 Failed in dcommentImport', DAG_ID)
    
def dopinionImport(**kwargs):
    try:
        print('dopinionImport called')       
    except Exception as e:
        log.ErrorV1(str(e),f'001 Failed in dopinionImport', DAG_ID)

def dopiniondetailsImport(**kwargs):
    print('dopiniondetailsImport called')
           
def dopiniontorateImport(**kwargs):
    print('dopiniontorateImport called')
    
def dvisitImport(**kwargs):
    print('dvisitImport called')
    
def dvisitorImport(**kwargs):
    print('dvisitorImport called')
    
def transformation(**kwargs):
    print('transformation')
    
################################################ Airflow Dag Tasks Configurations ################################################
default_args = {
    'owner': OWNER,
    'depends_on_past': False,
    'email': [''],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=30),
    'start_date': YESTERDAY,
}

with DAG(dag_id=DAG_ID,
        default_args=default_args,
        catchup=False,
        schedule_interval=SCHEDULE_INTERVAL) as dag:

    insertdatetime = dt.now().strftime("%Y-%m-%d %H:%M:%S")
    
    start = PythonOperator(task_id='start',python_callable=start,provide_context=True, dag=dag)
    end = PythonOperator(task_id='end',python_callable=end,provide_context=True, dag=dag)
    dcommentImport = PythonOperator(task_id='dcommentImport',python_callable=dcommentImport,op_kwargs={'insertdatetime': insertdatetime},provide_context=True, dag=dag)
    dopinionImport = PythonOperator(task_id='dopinionImport',python_callable=dopinionImport,op_kwargs={'insertdatetime': insertdatetime},provide_context=True, dag=dag)
    dopiniondetailsImport = PythonOperator(task_id='dopiniondetailsImport',python_callable=dopiniondetailsImport,op_kwargs={'insertdatetime': insertdatetime},provide_context=True, dag=dag)
    dopiniontorateImport = PythonOperator(task_id='dopiniontorateImport',python_callable=dopiniontorateImport,op_kwargs={'insertdatetime': insertdatetime},provide_context=True, dag=dag)
    dvisitImport = PythonOperator(task_id='dvisitImport',python_callable=dvisitImport,op_kwargs={'insertdatetime': insertdatetime},provide_context=True, dag=dag)
    dvisitorImport = PythonOperator(task_id='dvisitorImport',python_callable=dvisitorImport,op_kwargs={'insertdatetime': insertdatetime},provide_context=True, dag=dag)
    trans = PythonOperator(task_id='trans',python_callable=transformation,provide_context=True, dag=dag)

    start >> dcommentImport >> trans >> end
    start >> dopinionImport >> trans >> end
    start >> dopiniondetailsImport >> trans >> end
    start >> dopiniontorateImport >> trans >> end
    start >> dvisitImport >> trans >> end
    start >> dvisitorImport >> trans >> end
################################################ End Airflow Dag Tasks Configurations ################################################