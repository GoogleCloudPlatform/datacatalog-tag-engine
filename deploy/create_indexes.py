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

def create_indexes(project):
    
    responses = []
    
    with open(YAML_FILE) as yf:
        full = yaml.full_load(yf)
        indexes = full.get("indexes")
        
        for index in indexes: 
            coll_name = index['collection']
            fields = index['fields']

            parent = 'projects/{}/databases/(default)/collectionGroups/{}'.format(project, coll_name)
            
            field_list = [] 
    
            for field in fields:
                field_path = field['field']
                field_list.append({'field_path': field_path, 'order': 'ASCENDING'})
            
            fields_json = {"fields": field_list, "query_scope": "COLLECTION"}
            
            try:
                operation = firestore_client.create_index(parent=parent, index=fields_json)
                responses.append(operation)
            except Exception as e:
                print('Error occurred while creating index', fields_json, 'on', coll_name, '. Error:', e)
            
    print('Creating', len(responses), 'indexes.')
    
    for resp in responses:
        sleep_until_done(resp)


def sleep_until_done(resp):
        
    while True:
        if resp.done() != True:
            print('Index in progress. Sleeping for 30 seconds before next poll.')
            time.sleep(30)
        else:
            print('Index completed.')
            return 0            

 
if __name__ == '__main__':
    
    parser = argparse.ArgumentParser(description="Create Firestore indexes needed by Tag Engine")
    parser.add_argument('tag_engine_project', help='Tag Engine project')
    args = parser.parse_args()
     
    print('Using project ' + args.tag_engine_project)
    create_indexes(args.tag_engine_project)
