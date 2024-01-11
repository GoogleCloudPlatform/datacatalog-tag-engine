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
from airflow.providers.http.operators.http import SimpleHttpOperator

TAG_TEMPLATE_ID = 'quality_template' # tag template id
TAG_TEMPLATE_PROJECT = 'your-project' # project where above tag template is created
TAG_TEMPLATE_REGION = 'us-central1' # region where above tag template is created

BQ_PROJECT = 'your-project' # project where BQ data is stored 
BQ_DATASET = 'your-dataset' # BQ dataset name 
INCLUDED_URIS = 'bigquery/project/{bq_project}/dataset/{bq_dataset}/*' # included uris field in Tag Engine

USER_EMAIL = 'airflow@gmail.com' # email for sending failure notifications

YESTERDAY = datetime.datetime.now() - datetime.timedelta(days=1)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': [USER_EMAIL],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'start_date': YESTERDAY
}

dag = DAG('dynamic_tag_update', default_args=default_args)

update_tags = SimpleHttpOperator(
    task_id='trigger_job',
    method='POST',
    data=json.dumps({'template_id': TAG_TEMPLATE_ID, 'template_project': TAG_TEMPLATE_PROJECT, 'template_region': TAG_TEMPLATE_REGION, 'included_uris_hash': INCLUDED_URIS}),
    endpoint='trigger_job',
    dag=dag
)

trigger_job