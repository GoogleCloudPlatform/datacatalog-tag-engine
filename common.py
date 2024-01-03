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
import json, configparser

from google.api_core.client_info import ClientInfo
from google.cloud import logging_v2

config = configparser.ConfigParser()
config.read("tagengine.ini")

TAG_ENGINE_PROJECT = config['DEFAULT']['TAG_ENGINE_PROJECT']
USER_AGENT = 'cloud-solutions/datacatalog-tag-engine-v2'


def log_error(msg, error='', job_uuid=None):
    error = {'msg': msg, 'error': str(error), 'job_uuid': job_uuid}
    print(json.dumps(error)) 


def log_info(msg, job_uuid=None):
    info = {'msg': msg, 'job_uuid': job_uuid}
    print(json.dumps(info))
 
    
def get_log_entries(service_account):
    
    formatted_entries = []
    
    credentials, success = get_target_credentials(service_account)
    
    if success == False:
        print('Error acquiring credentials from', service_account)
    
    logging_client = logging_v2.Client(project=TAG_ENGINE_PROJECT, credentials=credentials, client_info=ClientInfo(user_agent=USER_AGENT))
    query="resource.type: cloud_run_revision"
    
    try:
        entries = list(logging_client.list_entries(filter_=query, order_by=logging_v2.DESCENDING, max_results=25))
    
        for entry in entries:
            timestamp = entry.timestamp.isoformat()[0:19]
        
            if entry.payload == None:
                continue
            
            if len(entry.payload) > 120:
                payload = entry.payload[0:120]
            else:
                payload = entry.payload
    
            formatted_entries.append((timestamp, payload))
    
    except Exception as e:
        print('Error occurred while retrieving log entries: ', e)
    
    return formatted_entries
