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

import argparse
from google.cloud import dlp
from google.cloud import bigquery


def create_dataset(region, project, dataset):

    bq_client = bigquery.Client(project=project)
    success = True
    dataset_id = bigquery.Dataset(project + '.' + dataset)
    dataset_id.location = region
    
    try:
        dataset_status = bq_client.create_dataset(dataset_id, exists_ok=True)  
        print("Created dataset {}".format(dataset_status.dataset_id))
        
    except Exception as e:
        print('Error occurred in create_dataset ', dataset_id, '. Error message: ', e)
        success = False
        
    return success, dataset_id


def inspect(region, inspect_project, inspect_datasets, result_project, result_datasets):

    bq_client = bigquery.Client(project=inspect_project)
    inspect_datasets_list = inspect_datasets.split(',')
    result_datasets_list = result_datasets.split(',')
    
    for index, inspect_dataset in enumerate(inspect_datasets_list):
        tables = bq_client.list_tables(inspect_project + '.' + inspect_dataset.strip()) 

        for table in tables:
            print("scanning {}.{}.{}".format(table.project, table.dataset_id, table.table_id))
            create_dataset(region, result_project, result_datasets_list[index].strip())
            start_job(region, inspect_project, inspect_dataset.strip(), table.table_id, result_project, result_datasets_list[index].strip())


def start_job(region, inspect_project, inspect_dataset, table, result_project, result_dataset):    

    inspect_job_data = {
        'storage_config': {
            'big_query_options': {
                'table_reference': {
                    'project_id': inspect_project,
                    'dataset_id': inspect_dataset,
                    'table_id': table
                }
            }
        },
        'inspect_config': {
            "info_types": [
              {
                "name": "CREDIT_CARD_NUMBER"
              },
              {
                "name": "EMAIL_ADDRESS"
              },
              {
                "name": "STREET_ADDRESS"
              },
              {
                "name": "PHONE_NUMBER"
              },
              {
                "name": "PERSON_NAME"
              },
              {
                "name": "FIRST_NAME"
              },
              {
                "name": "LAST_NAME"
              },
              {
                "name": "GENDER"
              },
              {
                "name": "DATE_OF_BIRTH"
              },
              {
                "name": "AGE"
              },
              {
                "name": "ETHNIC_GROUP"
              },
              {
                "name": "LOCATION_COORDINATES"
              },
              {
                "name": "IP_ADDRESS"
              }
            ],
             "min_likelihood": "LIKELY",
        },
        'actions': [
            {
                'save_findings': {
                    'output_config':{
                        'table':{
                            'project_id': result_project,
                            'dataset_id': result_dataset,
                            'table_id': table
                        }
                    }
                
                },
            },
        ]
    }

    dlp_client = dlp.DlpServiceClient()
    dlp_client.project_path(result_project)
    parent = 'projects/' + result_project + '/locations/' + region
    response = dlp_client.create_dlp_job(parent=parent, inspect_job=inspect_job_data)
    print(response)
    
if __name__ == '__main__':
    
    parser = argparse.ArgumentParser(description="Creates a DLP inspection job for each table in a BQ dataset. Pass it one or more BQ datasets to inspect.")
    parser.add_argument('region', help='The Google Cloud region in which the BQ datasets are stored.')
    parser.add_argument('inspect_project', help='The Google Cloud Project ID in which the BQ datasets reside.')
    parser.add_argument('inspect_datasets', help='The list of BQ datasets to scan with DLP.')
    parser.add_argument('result_project', help='The Google Cloud Project ID in which to write the DLP scan results.')
    parser.add_argument('result_datasets', help='The list of BQ datasets in which to write the DLP scan results.')
    args = parser.parse_args()
    
    inspect(args.region, args.inspect_project, args.inspect_datasets, args.result_project, args.result_datasets)




