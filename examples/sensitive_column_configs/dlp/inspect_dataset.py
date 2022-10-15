# Copyright 2022 Google, LLC.
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

from google.cloud import dlp
from google.cloud import bigquery

region = 'us-central1'
bq_project = 'tag-engine-develop'
bq_dataset = 'finwire'

dlp_project = 'tag-engine-develop'
dlp_dataset = 'finwire_dlp'

bq_client = bigquery.Client(project=bq_project)
dlp_client = dlp.DlpServiceClient()
parent = dlp_client.project_path(dlp_project)

def inspect():

    tables = bq_client.list_tables(bq_project + '.' + bq_dataset) 

    for table in tables:
        print('scanning ' + bq_dataset + '.' + table.table_id)
        print("{}.{}.{}".format(table.project, table.dataset_id, table.table_id))
        start_job(bq_dataset, table.table_id)

def start_job(bq_dataset, table):    

    inspect_job_data = {
        'storage_config': {
            'big_query_options': {
                'table_reference': {
                    'project_id': bq_project,
                    'dataset_id': bq_dataset,
                    'table_id': table
                },
                'rows_limit':100000,
                'sample_method':'RANDOM_START',
            },
        },
        'inspect_config': {
            'info_types': [
                {'name': 'ALL_BASIC'},
            ],
        },
        'actions': [
            {
                'save_findings': {
                    'output_config':{
                        'table':{
                            'project_id': bq_project,
                            'dataset_id': dlp_dataset,
                            'table_id': table
                        }
                    }
                
                },
            },
        ]
    }

    parent = 'projects/' + dlp_project + '/locations/' + region
    response = dlp_client.create_dlp_job(parent=parent, inspect_job=inspect_job_data)
    print(response)
    
if __name__ == '__main__':
    inspect()
