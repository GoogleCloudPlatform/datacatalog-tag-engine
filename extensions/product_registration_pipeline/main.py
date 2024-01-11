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

import base64, requests, json, time, datetime
from google.cloud import storage

BUCKET = 'tag-engine-configs' # replace with your bucket
TAG_ENGINE_URL = 'https://tag-engine-develop.uc.r.appspot.com' # replace with your URL

def process_pending_status_event(event, context):
        
    message = base64.b64decode(event['data']).decode('utf-8')
    data = json.loads(message)
    print('data: ', data)
    
    data_domain = data['protoPayload']['request']['tag']['fields']['data_domain']['enumValue']['displayName']
    data_product = data['protoPayload']['request']['tag']['fields']['data_product_name']['stringValue'].replace(' ', '_')
    tag_id = data['protoPayload']['request']['tag']['name']
    template_parent = data['protoPayload']['request']['tag']['template']
    
    print('data_domain: ', data_domain)
    print('data_product: ', data_product)
        
    job_id = create_data_standardization_tags(data_domain, data_product)
    job_complete(job_id, prev_tasks_ran=0)
    
    job_id = create_data_sensitivity_tags(data_domain, data_product)
    job_complete(job_id, prev_tasks_ran=0)
    
    job_id = create_data_resource_tags(data_domain, data_product)
    job_complete(job_id, prev_tasks_ran=0)
    
    resp = update_data_product_status(template_parent, tag_id)
    
    return 'Done' 


def get_tag_config(file_name):
    
    print('file_name: ', file_name)
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(BUCKET)
    blob = bucket.get_blob(file_name)
    blob_data = blob.download_as_string()
    json_dict = json.loads(blob_data) 
    return json_dict

 
def create_data_standardization_tags(data_domain, data_product):
    
    print('create_data_standardization_tags')
    
    file_name = data_domain + '/' + data_product + '/' + 'data_standardization.json'
    payload = get_tag_config(file_name)
    
    print('payload:', payload)
    
    url = TAG_ENGINE_URL + '/dynamic_column_tags'
    res = requests.post(url, json=payload)
    print('res: ', res.text)
    config_res = json.loads(res.text)
    return config_res


def create_data_sensitivity_tags(data_domain, data_product):
    
    print('create_data_sensitivity_tags')
    
    file_name = data_domain + '/' + data_product + '/' + 'data_sensitivity.json'
    payload = get_tag_config(file_name)
    
    print('payload:', payload)
    
    url = TAG_ENGINE_URL + '/sensitive_column_tags'
    res = requests.post(url, json=payload)
    print('res: ', res.text)
    config_res = json.loads(res.text)
    return config_res


def create_data_resource_tags(data_domain, data_product):
    
    print('create_data_resource_tags')
    
    file_name = data_domain + '/' + data_product + '/' + 'data_resource.json'
    payload = get_tag_config(file_name)
    
    print('payload:', payload)
    
    url = TAG_ENGINE_URL + '/dynamic_table_tags'
    res = requests.post(url, json=payload)
    print('res: ', res.text)
    config_res = json.loads(res.text)
    return config_res


def job_complete(payload, prev_tasks_ran):
    
    print('job_complete')
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
           
           
def update_data_product_status(template_parent, tag_id):
    
    print('update_data_product_status')
    
    template_id = template_parent.split('/')[5]
    template_project = template_parent.split('/')[1]
    template_region = template_parent.split('/')[3]
    
    entry_list = tag_id.split('/')[0:8]
    entry_name = "/".join(entry_list)
    print('entry_name: ', entry_name)
    
    product_status_fields = {'field_id': 'data_product_status', 'field_type': 'enum', 'field_value': 'REVIEW'}
    last_modified_date_fields = {'field_id': 'last_modified_date', 'field_type': 'timestamp', 'field_value': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
    fields = [product_status_fields, last_modified_date_fields]
    payload = {'template_id': template_id, 'template_project': template_project, 'template_region': template_region, 'entry_name': entry_name}
    payload['changed_fields'] = fields
    print('payload:', payload)
    
    url = TAG_ENGINE_URL + '/update_tag_subset'  
    res = requests.post(url, json=payload).json()
    print('update_tag_subset response:', res)
      
    return res