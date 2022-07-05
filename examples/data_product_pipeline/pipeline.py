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

import requests, json, time
from google.cloud import storage

bucket_name = 'tag-engine-configs'

def parse_message():
    
    f = open('sample_event.json')
    data = json.load(f)
    data_domain = data['protoPayload']['request']['tag']['fields']['data_domain']['enumValue']['displayName']
    data_product = data['protoPayload']['request']['tag']['fields']['data_product_name']['stringValue']
    print('data_domain: ', data_domain)
    print('data_product: ', data_product)
 
    return data_domain, data_product
    
def get_config(file_name):
    
    print('file_name: ', file_name)
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.get_blob(file_name)
    blob_data = blob.download_as_string()
    json_dict = json.loads(blob_data) 
    return json_dict

def sensitive_config(data_domain, data_product):
    
    file_name = data_domain + '/' + data_product + '/' + 'sensitive_create_ondemand.json'
    payload = get_config(file_name)

    url = 'https://tag-engine-develop.uc.r.appspot.com/sensitive_create'
    res = requests.post(url, json=payload)
    print('res: ', res.text)
    config_res = json.loads(res.text)
    return config_res

def job_complete(payload, prev_tasks_ran):
    
    print('payload', payload)
    print('prev_tasks_ran', prev_tasks_ran)
    
    url = 'https://tag-engine-develop.uc.r.appspot.com/get_job_status'
    res = requests.post(url, json=payload)
    job_dict = json.loads(res.text)
    cur_tasks_ran = job_dict['tasks_ran']
    print('cur_tasks_ran: ', cur_tasks_ran)
    
    if job_dict['job_status'] == 'COMPLETED' or (cur_tasks_ran > 0 and prev_tasks_ran == cur_tasks_ran):
        return True
    else:
        time.sleep(5)
        job_complete(payload, cur_tasks_ran)

def dynamic_config(data_domain, data_product):
    
    file_name = data_domain + '/' + data_product + '/' + 'dynamic_create_ondemand.json'
    payload = get_config(file_name)
    
    url = 'https://tag-engine-develop.uc.r.appspot.com/dynamic_create'
    res = requests.post(url, json=payload)
    print('res: ', res.text)
    config_res = json.loads(res.text)
    return config_res

if __name__ == '__main__':
    data_domain, data_product = parse_message()
    payload = sensitive_config(data_domain, data_product)
    job_complete(payload, prev_tasks_ran=0)
    payload = dynamic_config(data_domain, data_product)
    job_complete(payload, prev_tasks_ran=0)