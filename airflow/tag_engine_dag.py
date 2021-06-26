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

YESTERDAY = datetime.datetime.now() - datetime.timedelta(days=1)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'start_date': YESTERDAY
}

dag = DAG('tag_engine_dag', default_args=default_args)

update_tags = SimpleHttpOperator(
    task_id='update_tags',
    method='POST',
    data=json.dumps({'template_id': 'quality_template', 'project_id': 'tag-engine-283315', 'region': 'us-central1', 'included_uris_hash': 'ffa131c300794b3e5d42b9b86bfb15a4'}),
    endpoint='dynamic_ondemand_update',
    dag=dag
)

update_tags