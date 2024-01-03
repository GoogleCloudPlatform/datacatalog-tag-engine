# Copyright 2021 Google, LLC.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json

from airflow import models
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryGetDatasetTablesOperator
from airflow.operators import python_operator
from airflow.operators import email_operator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago

from google.cloud.dlp import DlpServiceClient

DLP_PROJECT = 'your-project' # project for running DLP 
DLP_REGION = 'global'
DLP_TEMPLATE = 'projects/{dlp_project}/locations/{dlp_region}/inspectTemplates/{template_name}'

BQ_PROJECT = 'your-project' # project for storing BQ data 
BQ_DATASET = 'your-dataset' # BQ dataset name with input data and for storing DLP results
USER_EMAIL = 'username@gmail.com' # email for sending failure notifications

TAG_TEMPLATE_ID = 'pii-classification' # tag template id
TAG_TEMPLATE_PROJECT = 'your-project' # project where above tag template is created
TAG_TEMPLATE_REGION = 'us-central1' # region where above tag template is created
INCLUDED_URIS = 'bigquery/project/{bq_project}/dataset/{bq_dataset}/*' # included uris field in Tag Engine

def create_dlp_job(index, **context):
  ti = context['ti']
  tables = ti.xcom_pull(task_ids='get_dataset_tables')
  table_dict = tables[index]
  table = table_dict['tableId']
  
  dlp = DlpServiceClient()

  parent = 'projects/' + PROJECT_ID + '/locations/' + DLP_REGION
  
  inspect_job_data = {
      'storage_config': {
          'big_query_options': {
              'table_reference': {
                  'project_id': BQ_PROJECT,
                  'dataset_id': BQ_DATASET,
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
                  'project_id': BQ_PROJECT,
                  'dataset_id': BQ_DATASET + '_dlp',
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
      data=json.dumps({'template_id': TAG_TEMPLATE_ID, 'template_project': TAG_TEMPLATE_PROJECT, 'template_region': TAG_TEMPLATE_REGION, 'included_uris': \
                       INCLUDED_URIS}),
      endpoint='trigger_job'
  )
      
  end_parallel_tasks >> update_dynamic_tags
        