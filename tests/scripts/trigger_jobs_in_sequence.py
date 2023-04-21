# Be sure to set GOOGLE_APPLICATION_CREDENTIALS to your CLIENT_SA keyfile before running script
# export GOOGLE_APPLICATION_CREDENTIALS="/Users/scohen/keys/python-client.json"
import urllib
import json
import google
import requests
import time
import sys
import google.auth.transport.requests
import google.oauth2.id_token

TAG_ENGINE_URL = 'https://tag-engine-eshsagj3ta-uc.a.run.app'
CREDENTIAL_SCOPES = ["https://www.googleapis.com/auth/cloud-platform"] 

TEMPLATE_PROJECT = 'tag-engine-run'
TEMPLATE_REGION = 'us-central1'

def get_id_token():
    audience = TAG_ENGINE_URL
    auth_req = google.auth.transport.requests.Request()
    id_token = google.oauth2.id_token.fetch_id_token(auth_req, audience)
    return id_token


def get_oauth_token():
    credentials, _ = google.auth.default(scopes=CREDENTIAL_SCOPES)
    credentials.refresh(google.auth.transport.requests.Request())
    return credentials.token
  
  
def trigger_dynamic_table_job(id_token, oauth_token):
    endpoint = TAG_ENGINE_URL + '/trigger_job'

    auth_req = google.auth.transport.requests.Request()
    id_token = google.oauth2.id_token.fetch_id_token(auth_req, audience=TAG_ENGINE_URL)
    headers = {'Authorization': 'Bearer ' + id_token, 'oauth_token': oauth_token}
    
    payload = {'template_id': 'data_governance', 'template_project': TEMPLATE_PROJECT, 'template_region': TEMPLATE_REGION, \
                'config_type': 'DYNAMIC_TAG_TABLE', 'included_tables_uris': 'bigquery/project/tag-engine-run/dataset/GCP_Mockup/*'}
                
    payload_json = json.dumps(payload)   
    response = requests.post(endpoint, headers=headers, data=payload_json)
    
    print('trigger job:', response.json())
    
    return response.json()


def trigger_dynamic_column_job(id_token, oauth_token):
    endpoint = TAG_ENGINE_URL + '/trigger_job'

    auth_req = google.auth.transport.requests.Request()
    id_token = google.oauth2.id_token.fetch_id_token(auth_req, audience=TAG_ENGINE_URL)
    headers = {'Authorization': 'Bearer ' + id_token, 'oauth_token': oauth_token}
    
    payload = {'template_id': 'data_governance', 'template_project': TEMPLATE_PROJECT, 'template_region': TEMPLATE_REGION, \
                'config_type': 'DYNAMIC_TAG_COLUMN', 'included_tables_uris': 'bigquery/project/tag-engine-run/dataset/GCP_Mockup/*'}
                
    payload_json = json.dumps(payload)   
    response = requests.post(endpoint, headers=headers, data=payload_json)
    
    print('trigger job:', response.json())
    
    return response.json()
    

def trigger_sensitive_column_job(id_token, oauth_token):
    endpoint = TAG_ENGINE_URL + '/trigger_job'

    auth_req = google.auth.transport.requests.Request()
    id_token = google.oauth2.id_token.fetch_id_token(auth_req, audience=TAG_ENGINE_URL)
    headers = {'Authorization': 'Bearer ' + id_token, 'oauth_token': oauth_token}
    
    payload = {'template_id': 'data_sensitivity', 'template_project': TEMPLATE_PROJECT, 'template_region': TEMPLATE_REGION, \
                'config_type': 'SENSITIVE_TAG_COLUMN', 'included_tables_uris': 'bigquery/project/tag-engine-run/dataset/crm/*'}
                
    payload_json = json.dumps(payload)   
    response = requests.post(endpoint, headers=headers, data=payload_json)
    
    print('trigger job:', response.json())
    
    return response.json()


def poll_job(id_token, oauth_token, payload):
    
    while True:
        job_status = get_job_status(id_token, oauth_token, payload)
        if job_status['job_status'] != 'SUCCESS' and job_status['job_status'] != 'ERROR':
            print('sleeping for 10 seconds...')
            time.sleep(10)
        else:
            print('done.')
            break
    
    return response


def get_job_status(id_token, oauth_token, payload):
    endpoint = TAG_ENGINE_URL + '/get_job_status'

    auth_req = google.auth.transport.requests.Request()
    id_token = google.oauth2.id_token.fetch_id_token(auth_req, audience=TAG_ENGINE_URL)
    headers = {'Authorization': 'Bearer ' + id_token, 'oauth_token': oauth_token}
    
    payload_json = json.dumps(payload)   
    response = requests.post(endpoint, headers=headers, data=payload_json)
    
    print('job status:', response.json())
    
    return response.json()

    
if __name__ == '__main__':
    id_token = get_id_token()
    oauth_token = get_oauth_token()
    
    response = trigger_dynamic_table_job(id_token, oauth_token)
    
    if 'job_uuid' in response:
        poll_job(id_token, oauth_token, response)
    else:
        print('Error: trigger_dynamic_table_job failed. Consult Cloud Run logs for details. ')
        sys.exit()
    
    response = trigger_dynamic_column_job(id_token, oauth_token)
    
    if 'job_uuid' in response:
        poll_job(id_token, oauth_token, response)
    else:
        print('Error: trigger_dynamic_column_job failed. Consult Cloud Run logs for details. ')
        sys.exit()
    
    response = trigger_sensitive_column_job(id_token, oauth_token)
    
    if 'job_uuid' in response:
        poll_job(id_token, oauth_token, response)
    else:
        print('Error: trigger_sensitive_column_job failed. Consult Cloud Run logs for details. ')
        sys.exit()