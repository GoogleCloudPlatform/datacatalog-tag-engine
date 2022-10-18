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

import base64, requests, json, time
from google.cloud import storage

BUCKET = 'tag-engine-configs'
TAG_ENGINE_URL = 'https://tag-engine-develop.uc.r.appspot.com'

def process_pending_status_event(event, context):
        
    message = base64.b64decode(event['data']).decode('utf-8')
    data = json.loads(message)
    #print('data: ', data)
    data_domain = data['protoPayload']['request']['tag']['fields']['data_domain']['enumValue']['displayName']
    data_product = data['protoPayload']['request']['tag']['fields']['data_product_name']['stringValue'].replace(' ', '_')
    print('data_domain: ', data_domain)
    print('data_product: ', data_product)
    
    payload = create_data_standardization_tags(data_domain, data_product)
    job_complete(payload, prev_tasks_ran=0)
    
    payload = create_data_sensitivity_tags(data_domain, data_product)
    job_complete(payload, prev_tasks_ran=0)
    
    payload = create_data_resource_tags(data_domain, data_product)
    job_complete(payload, prev_tasks_ran=0)
        
    return "OK" 
 
def get_tag_config(file_name):
    
    print('file_name: ', file_name)
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(BUCKET)
    blob = bucket.get_blob(file_name)
    blob_data = blob.download_as_string()
    json_dict = json.loads(blob_data) 
    return json_dict

def create_data_standardization_tags(data_domain, data_product):
    
    file_name = data_domain + '/' + data_product + '/' + 'data_standardization.json'
    payload = get_tag_config(file_name)
    
    url = TAG_ENGINE_URL + '/dynamic_column_tags'
    res = requests.post(url, json=payload)
    print('res: ', res.text)
    config_res = json.loads(res.text)
    return config_res


def create_data_sensitivity_tags(data_domain, data_product):
    
    file_name = data_domain + '/' + data_product + '/' + 'data_sensitivity.json'
    payload = get_tag_config(file_name)

    url = TAG_ENGINE_URL + '/sensitive_column_tags'
    res = requests.post(url, json=payload)
    print('res: ', res.text)
    config_res = json.loads(res.text)
    return config_res


def create_data_resource_tags(data_domain, data_product):
    
    file_name = data_domain + '/' + data_product + '/' + 'data_resource.json'
    payload = get_tag_config(file_name)
    
    url = TAG_ENGINE_URL + '/dynamic_table_tags'
    res = requests.post(url, json=payload)
    print('res: ', res.text)
    config_res = json.loads(res.text)
    return config_res
    
    
def job_complete(payload, prev_tasks_ran):
    
    print('payload', payload)
    print('prev_tasks_ran', prev_tasks_ran)
    
    url = TAG_ENGINE_URL + '/get_job_status'
    res = requests.post(url, json=payload)
    job_dict = json.loads(res.text)
    cur_tasks_ran = job_dict['tasks_ran']
    print('cur_tasks_ran: ', cur_tasks_ran)
    
    if job_dict['job_status'] == 'COMPLETED' or (cur_tasks_ran > 0 and prev_tasks_ran == cur_tasks_ran):
        return True
    else:
        time.sleep(5)
        job_complete(payload, cur_tasks_ran)
