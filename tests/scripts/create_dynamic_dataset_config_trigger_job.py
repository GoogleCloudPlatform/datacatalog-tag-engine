# Be sure to set GOOGLE_APPLICATION_CREDENTIALS to your CLIENT_SA keyfile before running script
# export GOOGLE_APPLICATION_CREDENTIALS="/Users/scohen/keys/python-client.json"
import urllib
import json
import google
import requests
import time
import google.auth.transport.requests
import google.oauth2.id_token

TAG_ENGINE_URL = 'https://tag-engine-eshsagj3ta-uc.a.run.app'
CREDENTIAL_SCOPES = ["https://www.googleapis.com/auth/cloud-platform"] 

def get_id_token():
    audience = TAG_ENGINE_URL
    auth_req = google.auth.transport.requests.Request()
    id_token = google.oauth2.id_token.fetch_id_token(auth_req, audience)
    return id_token


def get_oauth_token():
    credentials, _ = google.auth.default(scopes=CREDENTIAL_SCOPES)
    credentials.refresh(google.auth.transport.requests.Request())
    return credentials.token
  
  
def create_config(id_token, oauth_token):
    endpoint = TAG_ENGINE_URL + '/create_dynamic_table_config'

    auth_req = google.auth.transport.requests.Request()
    id_token = google.oauth2.id_token.fetch_id_token(auth_req, audience=TAG_ENGINE_URL)
    headers = {'Authorization': 'Bearer ' + id_token, 'oauth_token': oauth_token}
    
    payload = json.load(open('../configs/dynamic_table/dynamic_dataset_ondemand.json'))
    payload_json = json.dumps(payload)
    
    response = requests.post(endpoint, headers=headers, data=payload_json)
    
    print('config details:', response.json())
    
    return response.json()
    
    
def trigger_job(id_token, oauth_token, payload):
    endpoint = TAG_ENGINE_URL + '/trigger_job'

    auth_req = google.auth.transport.requests.Request()
    id_token = google.oauth2.id_token.fetch_id_token(auth_req, audience=TAG_ENGINE_URL)
    headers = {'Authorization': 'Bearer ' + id_token, 'oauth_token': oauth_token}
    
    payload_json = json.dumps(payload)   
    response = requests.post(endpoint, headers=headers, data=payload_json)
    
    print('trigger job:', response.json())
    
    return response.json()


def get_job_status(id_token, oauth_token, payload):
    endpoint = TAG_ENGINE_URL + '/get_job_status'

    auth_req = google.auth.transport.requests.Request()
    id_token = google.oauth2.id_token.fetch_id_token(auth_req, audience=TAG_ENGINE_URL)
    headers = {'Authorization': 'Bearer ' + id_token, 'oauth_token': oauth_token}
    
    payload_json = json.dumps(payload)   
    response = requests.post(endpoint, headers=headers, data=payload_json)
    
    print('job status:', response.json())
    
    return response

    
if __name__ == '__main__':
    id_token = get_id_token()
    oauth_token = get_oauth_token()
    response = create_config(id_token, oauth_token)
    response = trigger_job(id_token, oauth_token, response)
    get_job_status(id_token, oauth_token, response)
    print('sleeping for 10 seconds...')
    time.sleep(10)
    get_job_status(id_token, oauth_token, response)
    print('done.')