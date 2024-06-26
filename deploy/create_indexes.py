# Copyright 2023 Google, LLC.
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
import time
import yaml
from google.cloud.firestore_admin_v1.services.firestore_admin.client import FirestoreAdminClient

firestore_client = FirestoreAdminClient()

YAML_FILE = 'firestore.yaml'

def create_indexes(project, database):
    
    responses = []
    
    with open(YAML_FILE) as yf:
        full = yaml.full_load(yf)
        indexes = full.get("indexes")
        
        for index in indexes: 
            coll_name = index['collection']
            fields = index['fields']

            parent = f'projects/{project}/databases/{database}/collectionGroups/{coll_name}'
            
            field_list = [] 
    
            for field in fields:
                field_path = field['field']
                
                if ':' in field_path:
                    field_path = field_path.split(':')[0]
                    field_list.append({'field_path': field_path, 'order': 'DESCENDING'})
                else:
                    field_list.append({'field_path': field_path, 'order': 'ASCENDING'})
            
            fields_json = {"fields": field_list, "query_scope": "COLLECTION"}
            
            try:
                operation = firestore_client.create_index(parent=parent, index=fields_json)
                responses.append(operation)
            except Exception as e:
                print('Error occurred while creating index', fields_json, 'on', coll_name, '. Error:', e)
            
    for resp in responses:
        sleep_until_done(resp)

    print('Created', len(responses), 'indexes.')
    

def sleep_until_done(resp):
        
    while True:
        if resp.done() != True:
            print('Index in progress. Sleeping for 30 seconds before next poll.')
            time.sleep(30)
        else:
            print('Index completed.')
            return 0            

 
if __name__ == '__main__':
    
    parser = argparse.ArgumentParser(description='Create the database indexes in Firestore which are needed for Tag Engine')
    parser.add_argument('firestore_project', help='Firestore Project Id')
    parser.add_argument('firestore_database', help='Firestore Database Name')
    args = parser.parse_args()
     
    print('Using project ' + args.firestore_project + ' and database ' + args.firestore_database)
    create_indexes(args.firestore_project, args.firestore_database)
