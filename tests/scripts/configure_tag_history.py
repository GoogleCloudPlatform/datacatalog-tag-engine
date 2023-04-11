# Be sure to set GOOGLE_APPLICATION_CREDENTIALS before running script
# export GOOGLE_APPLICATION_CREDENTIALS="/Users/scohen/keys/python-client-tag-engine-run.json"
import urllib
import json
import google
import requests
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
  
  
def set_tag_history(id_token, oauth_token):
    endpoint = TAG_ENGINE_URL + '/configure_tag_history'

    auth_req = google.auth.transport.requests.Request()
    id_token = google.oauth2.id_token.fetch_id_token(auth_req, audience=TAG_ENGINE_URL)
    headers = {'Authorization': 'Bearer ' + id_token, 'oauth_token': oauth_token}
    
    payload = {"bigquery_region":"us-central1", "bigquery_project":"tag-engine-run", "bigquery_dataset":"tag_history", "enabled":True}
    payload_json = json.dumps(payload)
   
    response = requests.post(endpoint, headers=headers, data=payload_json)
    print('response:', response)
    
if __name__ == '__main__':
    id_token = get_id_token()
    oauth_token = get_oauth_token()
    set_tag_history(id_token, oauth_token)