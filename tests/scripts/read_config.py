# Be sure to set GOOGLE_APPLICATION_CREDENTIALS before running script
# export GOOGLE_APPLICATION_CREDENTIALS="/Users/scohen/keys/python-client-tag-engine-run.json"
import urllib
import json
import google
import requests
import google.auth.transport.requests
import google.oauth2.id_token
import argparse

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
  
  
def read_config(id_token, oauth_token, config_uuid, config_type):
    endpoint = TAG_ENGINE_URL + '/get_config'

    auth_req = google.auth.transport.requests.Request()
    id_token = google.oauth2.id_token.fetch_id_token(auth_req, audience=TAG_ENGINE_URL)
    headers = {'Authorization': 'Bearer ' + id_token, 'oauth_token': oauth_token}
    
    payload = {"config_uuid": config_uuid, "config_type": config_type}
    payload_json = json.dumps(payload)
   
    response = requests.post(endpoint, headers=headers, data=payload_json)
    print(response.json())
    
if __name__ == '__main__':
    
    config_type = 'DYNAMIC_TAG_COLUMN'
    config_uuid = 'f7fd7ee4d89311ed8d4f5d52120cae96'
    
    id_token = get_id_token()
    oauth_token = get_oauth_token()

    parser = argparse.ArgumentParser(description="Reads a config from Firestore given a config_uuid and config_type.")
    parser.add_argument('config_uuid', help='The config_uuid of the config to be read.')
    parser.add_argument('config_type', help='The config_type of the config to be read.')
    args = parser.parse_args()
    read_config(id_token, oauth_token, args.config_uuid, args.config_type)

    