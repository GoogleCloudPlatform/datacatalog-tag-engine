# Be sure to set GOOGLE_APPLICATION_CREDENTIALS to your CLIENT_SA keyfile before running script
# export GOOGLE_APPLICATION_CREDENTIALS="/Users/scohen/keys/python-client.json"

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
  
def list_configs(id_token):
    endpoint = TAG_ENGINE_URL + '/list_configs'

    auth_req = google.auth.transport.requests.Request()
    id_token = google.oauth2.id_token.fetch_id_token(auth_req, audience=TAG_ENGINE_URL)
    headers = {'Authorization': 'Bearer ' + id_token}
    
    payload = {"config_type":"ALL"}
    payload_json = json.dumps(payload)
   
    response = requests.post(endpoint, headers=headers, data=payload_json)
    print(response.json())
    
if __name__ == '__main__':
    id_token = get_id_token()
    list_configs(id_token)