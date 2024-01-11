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

import base64, json, configparser, requests

import google.oauth2.service_account
import google.oauth2.credentials # user credentials

from oauth2client.client import GoogleCredentials
from googleapiclient import discovery

import TagEngineStoreHandler as tesh
from common import log_info, log_error

config = configparser.ConfigParser()
config.read("tagengine.ini")
TAG_CREATOR_SA = config['DEFAULT']['TAG_CREATOR_SA'].strip()

SCOPES = ['openid', 'https://www.googleapis.com/auth/cloud-platform', 'https://www.googleapis.com/auth/userinfo.email']

##################### Methods used by API only #################
    
# get the service account intended to process request 
def get_requested_service_account(json):
    #print("*** get_requested_service_account ***")
    store = tesh.TagEngineStoreHandler()
    
    if isinstance(json, dict) and 'service_account' in json:
        service_account = json['service_account']
    elif isinstance(json, dict) and 'config_uuid' in json and 'config_type' in json:
        service_account = store.lookup_service_account(json['config_type'], json['config_uuid'])
    elif isinstance(json, dict) and 'job_uuid' in json:
        config_uuid, config_type = store.read_config_by_job(json['job_uuid'])
        service_account = store.lookup_service_account(config_type, config_uuid)
    else:
        service_account = TAG_CREATOR_SA
    
    return service_account


def check_user_credentials_from_api(tag_creator_sa, tag_invoker_account):

    has_permission = False

    credentials = GoogleCredentials.get_application_default()
    service = discovery.build('iam', 'v1', credentials=credentials)
    
    start_index = tag_creator_sa.index('@') + 1 
    end_index = tag_creator_sa.index('.') 
    project = tag_creator_sa[start_index:end_index]

    resource = 'projects/{}/serviceAccounts/{}'.format(project, tag_creator_sa)
    print('resource:', resource)  

    try:
        request = service.projects().serviceAccounts().getIamPolicy(resource=resource)
        iam_policy = request.execute()
        #print('iam_policy:', iam_policy)
        
        if "bindings" not in iam_policy:
            return has_permission

        for binding in iam_policy.get('bindings'):
            if binding.get('role') == 'roles/iam.serviceAccountUser':
                if f"user:{tag_invoker_account}" in binding.get('members') or f"serviceAccount:{tag_invoker_account}" in binding.get('members'):
                    has_permission = True
        
    except Exception as e:
        msg = 'Error calling getIamPolicy on: {}'.format(resource)
        log_error(msg, e)

    return has_permission

    
def do_authentication(headers, json_request, ENABLE_AUTH):
    
    print('** enter do_authentication **')
    
    tag_invoker_account = get_tag_invoker_account(headers.get('Authorization'))
    tag_creator_sa = get_requested_service_account(json_request)
    
    if ENABLE_AUTH == False:
        return True, None, tag_creator_sa
              
    has_permission = check_user_credentials_from_api(tag_creator_sa, tag_invoker_account)   
    print('user has permission:', has_permission)
    
    if has_permission == False:
        response = {
            "status": "error",
            "message": "Fatal error: User " + tag_invoker_account + " is missing roles/iam.serviceAccountUser on " + tag_creator_sa,
        }
        return False, response, tag_creator_sa
       
    return True, None, tag_creator_sa


# get the account (user or service) which invoked the job
# this account is then passed to the tag history function to record the tag creation / update event
def get_tag_invoker_account(raw_token):

    token = raw_token.replace("Bearer ", "").replace("bearer ", "").replace(" ","")
    if '.' in token:
        token = token.split('.')[1]

    padded_token = token + "="*divmod(len(token),4)[1]
    raw_decode = base64.b64decode(padded_token) 
    txt_decode = raw_decode.decode("utf-8")
    token_obj = json.loads(txt_decode)
    tag_invoker_account = token_obj['email']

    return tag_invoker_account 


# *************************************************************** #
################ Methods used by both API + UI paths ##############
# *************************************************************** #

# used when TAG_CREATOR_SA credentials are needed
def get_target_credentials(target_service_account):
    
    from google.auth import impersonated_credentials # service account credentials
    source_credentials, _ = google.auth.default()
     
    try:
        target_credentials = impersonated_credentials.Credentials(source_credentials=source_credentials,
            target_principal=target_service_account,
            target_scopes=SCOPES,
            lifetime=1200) # lifetime is in seconds -> 20 minutes should be enough time
        success = True
        
    except Exception as e:
        print('Error impersonating credentials: ', e)
        success = False
        
    return target_credentials, success


##################### Methods used by UI path only #################

def check_user_credentials_from_ui(credentials_dict, service_account):
    
    has_permission = False
    
    credentials = dict_to_credentials(credentials_dict)                                                                                                        
    service = discovery.build('iam', 'v1', credentials=credentials)

    # get the GCP project which owns the service account
    start_index = service_account.index('@') + 1 
    end_index = service_account.index('.') 
    service_account_project = service_account[start_index:end_index]
    resource = 'projects/{}/serviceAccounts/{}'.format(service_account_project, service_account)
    permissions = ["iam.serviceAccounts.actAs", "iam.serviceAccounts.get"] # permissions associated with roles/iam.serviceAccountUser
    body={"permissions": permissions}

    request = service.projects().serviceAccounts().testIamPermissions(resource=resource, body=body)
    response = request.execute()
    
    if 'permissions' in response:
        #print('allowed permissions:', response['permissions'])
        user_permissions = response['permissions']
        if "iam.serviceAccounts.actAs" in user_permissions and "iam.serviceAccounts.get" in user_permissions:
            has_permission = True
          
    return has_permission


def credentials_to_dict(credentials):
    return {'token': credentials.token,
            'refresh_token': credentials.refresh_token,
            'token_uri': credentials.token_uri,
            'client_id': credentials.client_id,
            'client_secret': credentials.client_secret,
            'scopes': credentials.scopes,
            'id_token': credentials.id_token}
    
            
def dict_to_credentials(_dict):
    
    credentials = google.oauth2.credentials.Credentials(token=_dict['token'], refresh_token=_dict['refresh_token'], 
                                                        token_uri=_dict['token_uri'], client_id=_dict['client_id'], 
                                                        client_secret=_dict['client_secret'], scopes=_dict['scopes'],
                                                        id_token=_dict['id_token'])    
    return credentials
