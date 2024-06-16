# Copyright 2024 Google, LLC.
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

from google.cloud import bigquery
from google.cloud import datacatalog
import json

def event_handler(request):
    
    request_json = request.get_json()
    project = request_json['calls'][0][0].strip()
    region = request_json['calls'][0][1].strip()
    dataset = request_json['calls'][0][2].strip()
    table = request_json['calls'][0][3].strip()
    column = request_json['calls'][0][4].strip()

    try:
        policy_tag = main(project, region, dataset, table, column)
        return json.dumps({"replies": [policy_tag]})
    
    except Exception as e:
        print("Exception caught: " + str(e))
        return json.dumps({"errorMessage": str(e)}), 400 
        
        
def main(project, region, dataset, table, column):        
	
    bq_client = bigquery.Client(project=project, location=region)
    ptm_client = datacatalog.PolicyTagManagerClient()
    
    table_id = f'{project}.{dataset}.{table}'
    table_ref = bq_client.get_table(table_id) 
    table_schema = table_ref.schema

    for field in table_schema:
        if field.name == column:
            if field.policy_tags != None:
                policy_tag = field.policy_tags.names[0]
           
                get_request = datacatalog.GetPolicyTagRequest(name=policy_tag)
                resp = ptm_client.get_policy_tag(request=get_request)
                policy_tag = resp.display_name
        else:
            policy_tag = None
            
    return policy_tag
