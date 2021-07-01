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

import datetime, json
from airflow import DAG
from airflow.contrib.operators.gcp_dlp_operator import CloudDLPCreateDLPJobOperator
from airflow.providers.http.operators.http import SimpleHttpOperator

YESTERDAY = datetime.datetime.now() - datetime.timedelta(days=1)

project_id = 'data-lake-290221'
dataset = 'tpcds'
table = 'customer'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'start_date': YESTERDAY
}

dag = DAG('dlp_scan_tag_update', default_args=default_args)

inspect_job_data = {
    'storage_config': {
        'big_query_options': {
            'table_reference': {
                'project_id': project_id,
                'dataset_id': dataset,
                'table_id': table
            },
            'rows_limit':10000,
            'sample_method':'RANDOM_START',
        },
    },
    'inspect_config': {
        'info_types': [
            {'name': 'ALL_BASIC'},
        ],
        'min_likelihood': 'VERY_LIKELY',
        'custom_info_types': [],
        'include_quote': True
    },
    'actions': [
        {
          'save_findings': {
            'output_config': {
              'table': {
                'project_id': project_id,
                'dataset_id': dataset,
                'table_id': table + '_dlp_scan_results'
              }
            }
          }
        }
    ]
}

dlp_scan = CloudDLPCreateDLPJobOperator(
    task_id='dlp_scan',
    inspect_job=inspect_job_data,
    wait_until_finished=True,
    dag=dag
)

update_tags = SimpleHttpOperator(
    task_id='update_tags',
    method='POST',
    data=json.dumps({'template_id': 'dg_dlp_template', 'project_id': 'tag-engine-283315', 'region': 'us-central1', 'included_uris_hash': 'dba7f440d4e17ae35d32f12917a32de5'}),
    endpoint='dynamic_ondemand_update',
    dag=dag
)

dlp_scan >> update_tags