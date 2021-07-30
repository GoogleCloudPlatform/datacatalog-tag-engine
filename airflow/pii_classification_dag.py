import json

from airflow import models
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryGetDatasetTablesOperator
from airflow.operators import python_operator
from airflow.operators import email_operator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago

from google.cloud.dlp import DlpServiceClient

PROJECT_ID = 'data-lake-290221'
DLP_REGION = 'global'
DLP_TEMPLATE = 'projects/data-lake-290221/locations/global/inspectTemplates/pii_any_region'
DATASET = 'retail'
USER_EMAIL = 'scohen@google.com'


def create_dlp_job(index, **context):
  ti = context['ti']
  tables = ti.xcom_pull(task_ids='get_dataset_tables')
  table_dict = tables[index]
  table = table_dict['tableId']
  print('tables: ' + str(tables))
  print('index: ' + str(index))
  print('table: ' + str(table)) 
  
  dlp = DlpServiceClient()

  parent = 'projects/' + PROJECT_ID + '/locations/' + DLP_REGION
  
  inspect_job_data = {
      'storage_config': {
          'big_query_options': {
              'table_reference': {
                  'project_id': PROJECT_ID,
                  'dataset_id': DATASET,
                  'table_id': table
              },
              'rows_limit': 50000,
              'sample_method':'RANDOM_START',
          },
      },
      'inspect_template_name': DLP_TEMPLATE,
      'inspect_config': {
          'min_likelihood': 'LIKELY',
          'custom_info_types': [],
          'include_quote': True
      },
      'actions': [
          {
            'save_findings': {
              'output_config': {
                'table': {
                  'project_id': PROJECT_ID,
                  'dataset_id': DATASET + '_dlp',
                  'table_id': table + '_scan_results'
                }
              }
            }
          }
      ]
  }

  try:
      response = dlp.create_dlp_job(parent=parent, inspect_job=inspect_job_data)
      print(response)
  except Exception as e:
      print(e)  

def report_failure(context):
  send_email = email_operator.EmailOperator(
      task_id="failure",
      to=USER_EMAIL,
      start_date=days_ago(1),
      subject='PII classification job failed'
  )

DEFAULT_DAG_ARGS = {
  'start_date': days_ago(1)
}

with models.DAG(dag_id='pii_classification_dag',
                description='',
                schedule_interval=None, default_args=DEFAULT_DAG_ARGS) as dag:

  get_dataset_tables = BigQueryGetDatasetTablesOperator(
    task_id='get_dataset_tables', 
    dataset_id=DATASET,
    do_xcom_push=True
  )  

  end_parallel_tasks = DummyOperator(task_id='end_parallel_tasks',retries=0)

  task_list = []
  for i in range(0, 15):
      task_list.append(python_operator.PythonOperator(
        task_id='scan_table_' + str(i),
        provide_context=True,
        python_callable=create_dlp_job,
        op_kwargs={'index': i},
        on_failure_callback=report_failure,
        retries=0
      ))
    
      get_dataset_tables >> task_list[i] >> end_parallel_tasks
  
  update_dynamic_tags = SimpleHttpOperator(
      task_id='update_dynamic_tags',
      method='POST',
      data=json.dumps({'template_id': 'pii_classification', 'project_id': 'tag-engine-283315', 'region': 'us-central1', 'included_uris': \
                       'bigquery/project/data-lake-290221/dataset/retail/*'}),
      endpoint='dynamic_ondemand_update'
  )
      
  end_parallel_tasks >> update_dynamic_tags
        