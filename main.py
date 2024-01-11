# Copyright 2020-2023 Google, LLC.
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

from flask import Flask, render_template, request, redirect, url_for, jsonify, json, session
from flask_session import Session

import datetime, time, configparser, os, base64
import requests

import google_auth_oauthlib.flow
from googleapiclient import discovery

from google.cloud import firestore
from google.cloud import bigquery
from google.cloud import tasks_v2
from google.protobuf import timestamp_pb2

from google.api_core.client_info import ClientInfo
from google.cloud import logging_v2

from access import do_authentication
from access import check_user_credentials_from_ui
from access import credentials_to_dict
from access import get_target_credentials
from access import get_tag_invoker_account

from common import log_error

import DataCatalogController as controller
import TagEngineStoreHandler as tesh
import Resources as res
import BackupFileParser as bfp
import CsvParser as cp
import constants

import JobManager as jobm
import TaskManager as taskm
import BigQueryUtils as bq
import ConfigType as ct

config = configparser.ConfigParser()
config.read("tagengine.ini")

##################### INIT GLOBAL VARIABLES ##################################
    
TAG_ENGINE_PROJECT = config['DEFAULT']['TAG_ENGINE_PROJECT'].strip()
TAG_ENGINE_REGION = config['DEFAULT']['TAG_ENGINE_REGION'].strip()
    
TAG_ENGINE_SA = config['DEFAULT']['TAG_ENGINE_SA'].strip()
TAG_CREATOR_SA = config['DEFAULT']['TAG_CREATOR_SA'].strip()

BIGQUERY_REGION = config['DEFAULT']['BIGQUERY_REGION'].strip()

SPLIT_WORK_HANDLER = os.environ['SERVICE_URL'] + '/_split_work'
RUN_TASK_HANDLER = os.environ['SERVICE_URL'] + '/_run_task'

INJECTOR_QUEUE = config['DEFAULT']['INJECTOR_QUEUE'].strip()
WORK_QUEUE = config['DEFAULT']['WORK_QUEUE'].strip()

DB_NAME = config['DEFAULT']['DB_NAME'].strip()

jm = jobm.JobManager(TAG_ENGINE_SA, TAG_ENGINE_PROJECT, TAG_ENGINE_REGION, INJECTOR_QUEUE, SPLIT_WORK_HANDLER, DB_NAME)
tm = taskm.TaskManager(TAG_ENGINE_SA, TAG_ENGINE_PROJECT, TAG_ENGINE_REGION, WORK_QUEUE, RUN_TASK_HANDLER, DB_NAME)

SCOPES = ['openid', 'https://www.googleapis.com/auth/cloud-platform', 'https://www.googleapis.com/auth/userinfo.email']
 
USER_AGENT = 'cloud-solutions/datacatalog-tag-engine-v2'
store = tesh.TagEngineStoreHandler()

if 'ENABLE_TAG_HISTORY' in config['DEFAULT'] and config['DEFAULT']['ENABLE_TAG_HISTORY'].strip().lower() == 'true':
    ENABLE_TAG_HISTORY = True
else:
    ENABLE_TAG_HISTORY = False

if 'TAG_HISTORY_PROJECT' in config['DEFAULT']:
    TAG_HISTORY_PROJECT = config['DEFAULT']['TAG_HISTORY_PROJECT'].strip()
else:
    TAG_HISTORY_PROJECT = None

if 'TAG_HISTORY_DATASET' in config['DEFAULT']:    
    TAG_HISTORY_DATASET = config['DEFAULT']['TAG_HISTORY_DATASET'].strip()
else:
    TAG_HISTORY_DATASET = None
    
if 'ENABLE_JOB_METADATA' in config['DEFAULT'] and config['DEFAULT']['ENABLE_JOB_METADATA'].strip().lower() == 'true':    
    ENABLE_JOB_METADATA = True
else:
    ENABLE_JOB_METADATA = False
    
if 'JOB_METADATA_PROJECT' in config['DEFAULT']:
    JOB_METADATA_PROJECT = config['DEFAULT']['JOB_METADATA_PROJECT'].strip()
else:
    JOB_METADATA_PROJECT = None

if 'JOB_METADATA_DATASET' in config['DEFAULT']:    
    JOB_METADATA_DATASET = config['DEFAULT']['JOB_METADATA_DATASET'].strip()
else:
    JOB_METADATA_DATASET = None


##################### CHECK SERVICE_URL #####################
def check_service_url():
    if os.environ['SERVICE_URL'] == None:
        print('Fatal Error: SERVICE_URL environment variable not set. Please set it before running the Tag Engine app.')
        return -1

check_service_url()

##################### CHECK AUTH and CLIENT SECRET VARIABLES #####################
    
if config['DEFAULT']['ENABLE_AUTH'].lower() == 'true' or config['DEFAULT']['ENABLE_AUTH'] == 1:
    ENABLE_AUTH = True
    print('Info: ENABLE_AUTH = True')
else:
    ENABLE_AUTH = False
    print('Info: ENABLE_AUTH = False. This option is only supported in API mode as the client secret is needed to obtain an access token from the UI.')


if 'OAUTH_CLIENT_CREDENTIALS' in config['DEFAULT']:
    OAUTH_CLIENT_CREDENTIALS = config['DEFAULT']['OAUTH_CLIENT_CREDENTIALS'].strip()
    print('Info: OAUTH_CLIENT_CREDENTIALS =', OAUTH_CLIENT_CREDENTIALS)
else:
    if 'tag-engine-ui-' in os.environ['SERVICE_URL']:
        print('Fatal Error: The Tag Engine UI requires the OAUTH_CLIENT_CREDENTIALS variable to be set. Please set it in tagengine.ini.')
    else:
        print('Info: running in API mode without the client secret file')

##################### CONFIGURE TAG HISTORY ##################################
 
def configure_tag_history(): 
        
    if ENABLE_TAG_HISTORY:
        
        if TAG_HISTORY_PROJECT == None:
            print('Error: unable to configure tag history, TAG_HISTORY_PROJECT is missing from tagengine.ini')
        
        if TAG_HISTORY_DATASET == None:
            print('Error: unable to configure tag history, TAG_HISTORY_DATASET is missing from tagengine.ini')
    
    status = store.write_tag_history_settings(ENABLE_TAG_HISTORY, TAG_HISTORY_PROJECT, BIGQUERY_REGION, TAG_HISTORY_DATASET)

    if status:
        print('Tag History is set to', ENABLE_TAG_HISTORY)
    else:
        print('Error writing tag history settings to Firestore.')
        
configure_tag_history()

##################### CONFIGURE JOB METADATA ##################################
 
def configure_job_metadata(): 
        
    if ENABLE_JOB_METADATA:
        
        if JOB_METADATA_PROJECT == None:
            print('Error: unable to configure tag history, JOB_METADATA_PROJECT is missing from tagengine.ini')
        
        if JOB_METADATA_DATASET == None:
            print('Error: unable to configure tag history, JOB_METADATA_DATASET is missing from tagengine.ini')
    
    status = store.write_job_metadata_settings(ENABLE_JOB_METADATA, JOB_METADATA_PROJECT, BIGQUERY_REGION, JOB_METADATA_DATASET)
            
configure_job_metadata()


##################### COMMON METHOD USED BY UI #################

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

##################### UI METHODS #################

app = Flask(__name__)
app.config["SESSION_PERMANENT"] = False
app.config["SESSION_TYPE"] = "filesystem"
Session(app)

@app.route("/")
def authorize():
    
    flow = google_auth_oauthlib.flow.Flow.from_client_secrets_file(OAUTH_CLIENT_CREDENTIALS, scopes=SCOPES)
        
    flow.redirect_uri = url_for('oauth2callback', _external=True, _scheme='https')

    authorization_url, state = flow.authorization_url(access_type='offline', include_granted_scopes="true",)
    
    session['state'] = state
    print('state:', state)

    return redirect(authorization_url)


@app.route('/oauth2callback')
def oauth2callback():
    # Specify the state when creating the flow in the callback so that it can
    # verified in the authorization server response.
    state = session['state']

    flow = google_auth_oauthlib.flow.Flow.from_client_secrets_file(OAUTH_CLIENT_CREDENTIALS, scopes=SCOPES, state=state)
    flow.redirect_uri = url_for('oauth2callback', _external=True, _scheme='https')

    https_authorization_url = request.url.replace('http://', 'https://')
    flow.fetch_token(authorization_response=https_authorization_url)
            
    credentials = flow.credentials
    session['credentials'] = credentials_to_dict(credentials)
        
    user_info_service = discovery.build('oauth2', 'v2', credentials=credentials)
    user_info = user_info_service.userinfo().get().execute()
    session['user_email'] = user_info['email']
        
    return redirect(url_for('home'))
    
    
@app.route("/logout")
def logout():
    
    if 'credentials' in session:
        del session['credentials']
        
    return redirect(url_for('authorize'))
    

@app.route("/home")
def home():
    
    if 'credentials' not in session:
        return redirect('/')
      
    exists, settings = store.read_default_settings(session['user_email'])
    
    if exists:
        template_id = settings['template_id']
        template_project = settings['template_project']
        template_region = settings['template_region']
        service_account = settings['service_account']
    else:
        template_id = "{tag_template_id}"
        template_project = "{tag_template_project}"
        template_region = "{tag_template_region}"
        service_account = "{service_account}"
    
    # [END homepage]
    # [START render_template]
    return render_template(
        'home.html',
        template_id=template_id,
        template_project=template_project,
        template_region=template_region,
        service_account=service_account)

    
@app.route("/default_settings<int:saved>", methods=["GET"])
def default_settings(saved):
    
    if 'credentials' not in session:
        return redirect('/')
        
    exists, settings = store.read_default_settings(session['user_email'])
    
    if exists:
        template_id = settings['template_id']
        template_project = settings['template_project']
        template_region = settings['template_region']
        service_account = settings['service_account']
    else:
        template_id = "{tag_template_id}"
        template_project = "{tag_template_project}"
        template_region = "{tag_template_region}"
        service_account = "{service_account}"
    
    # [END default_settings]
    # [START render_template]
    return render_template(
        'default_settings.html',
        template_id=template_id,
        template_project=template_project,
        template_region=template_region,
        service_account=service_account,
        settings=saved)
    # [END render_template]
         
@app.route("/coverage_report_settings<int:saved>")
def coverage_report_settings(saved):
    
    if 'credentials' not in session:
        return redirect('/')
        
    exists, settings = store.read_coverage_report_settings()
    
    if exists:
        included_bigquery_projects = settings['included_bigquery_projects']
        excluded_bigquery_datasets = settings['excluded_bigquery_datasets']
        excluded_bigquery_tables = settings['excluded_bigquery_tables']
    else:
        included_bigquery_projects = "{projectA}, {projectB}, {projectC}"
        excluded_bigquery_datasets = "{project.dataset1}, {project.dataset2}, {project.dataset3}"
        excluded_bigquery_tables = "{project.dataset.table1}, {project.dataset.table2}, {project.dataset.view3}"
    
    # [END report_settings]
    # [START render_template]
    return render_template(
        'coverage_report_settings.html',
        included_bigquery_projects=included_bigquery_projects,
        excluded_bigquery_datasets=excluded_bigquery_datasets,
        excluded_bigquery_tables=excluded_bigquery_tables,
        settings=saved)
    # [END render_template]
    
@app.route("/tag_history_settings", methods=["GET"])
def tag_history_settings():
    
    if 'credentials' not in session:
        return redirect('/')
        
    enabled, settings = store.read_tag_history_settings()
    
    bigquery_project = settings['bigquery_project']
    bigquery_region = settings['bigquery_region']
    bigquery_dataset = settings['bigquery_dataset']
      
    # [END tag_history_settings]
    # [START render_template]
    return render_template(
        'tag_history_settings.html',
        enabled=enabled,
        bigquery_project=bigquery_project,
        bigquery_region=bigquery_region,
        bigquery_dataset=bigquery_dataset)
    # [END render_template]

@app.route("/set_default_settings", methods=['POST'])
def set_default_settings():
    
    if 'credentials' not in session:
        return redirect('/')
        
    template_id = request.form['template_id'].rstrip()
    template_project = request.form['template_project'].rstrip()
    template_region = request.form['template_region'].rstrip()
    service_account = request.form['service_account'].rstrip()
    
    if template_id == "{tag_template_id}":
        template_id = None
    if template_project == "{tag_template_project}":
        template_project = None
    if template_region == "{tag_template_region}":
        template_region = None
    if service_account == "{service_account}":
        service_account = None
    
    if template_id != None or template_project != None or template_region != None or service_account != None:
        store.write_default_settings(session['user_email'], template_id, template_project, template_region, service_account)
        
    return default_settings(1)
        
                
@app.route("/set_coverage_report", methods=['POST'])
def set_coverage_report():
    
    if 'credentials' not in session:
        return redirect('/')
        
    included_bigquery_projects = request.form['included_bigquery_projects'].rstrip()
    
    if request.form['excluded_bigquery_datasets']:
        excluded_bigquery_datasets = request.form['excluded_bigquery_datasets'].rstrip()
    else:
        excluded_bigquery_datasets = None
        
    if request.form['excluded_bigquery_tables']:
        excluded_bigquery_tables = request.form['excluded_bigquery_tables'].rstrip()
    else:
        excluded_bigquery_tables = None
    
    print("included_bigquery_projects: ", included_bigquery_projects)
    print("excluded_bigquery_datasets: ", excluded_bigquery_datasets)
    print("excluded_bigquery_tables: ", excluded_bigquery_tables)
    
    if included_bigquery_projects == "{projectA}, {projectB}, {projectC}":
        included_bigquery_projects = None
    if excluded_bigquery_datasets == "{project.dataset1}, {project.dataset2}, {project.dataset3}":
        excluded_bigquery_datasets = None
    if excluded_bigquery_tables == "{project.dataset.table1}, {project.dataset.table2}, {project.dataset.view3}":
        excluded_bigquery_tables = None
    
    if included_bigquery_projects != None:
        store.write_coverage_report_settings(included_bigquery_projects, excluded_bigquery_datasets, excluded_bigquery_tables)
        
    return coverage_report_settings(1)  
     
@app.route("/coverage_report")
def coverage_report():
    
    if 'credentials' not in session:
        return redirect('/')
        
    summary_report, detailed_report = store.generate_coverage_report(session['credentials'])
    
    print('summary_report: ' + str(summary_report))
    print('detailed_report: ' + str(detailed_report))
    
    exists, settings = store.read_coverage_report_settings()
    included_bigquery_projects = settings['included_bigquery_projects']
    
    return render_template(
        "coverage_report.html",
        included_bigquery_projects=included_bigquery_projects,
        report_headers=summary_report,
        report_data=detailed_report)

# TO DO: re-implement this method using the DC API        
@app.route("/coverage_details<string:res>", methods=['GET'])
def coverage_details(res):
    
    if 'credentials' not in session:
        return redirect('/')
    
    bigquery_project = res.split('.')[0]
    resource = res.split('.')[1]
    
    configs = store.read_configs_on_res(res)
    
    return render_template(
        'view_tags_on_res.html',
        resource=res,
        bigquery_project=bigquery_project,
        configs=configs)
                
# [START search_tag_template]
@app.route('/search_tag_template', methods=['POST'])
def search_tag_template():

    if 'credentials' not in session:
        return redirect('/')
        
    template_id = request.form['template_id']
    template_project = request.form['template_project']
    template_region = request.form['template_region']
    service_account = request.form['service_account']
    
    # make sure user is authorized to use the service account
    if ENABLE_AUTH == True:
        has_permission = check_user_credentials_from_ui(session['credentials'], service_account)   
        print('user has permission:', has_permission)
    
        if has_permission == False:
            return render_template(
                'home.html',
                template_id=template_id,
                template_project=template_project,
                template_region=template_region,
                service_account=service_account,
                missing_permissions=True)
      
    credentials, success = get_target_credentials(service_account)
    
    if success == False:
        print('Error acquiring credentials from', service_account)
     
    dcc = controller.DataCatalogController(credentials, None, None, template_id, template_project, template_region)
    fields = dcc.get_template()
    
    if len(fields) == 0:
        # error retrieving the tag template
        return render_template(
            'home.html',
            template_id=template_id,
            template_project=template_project,
            template_region=template_region,
            service_account=service_account,
            tag_template_error=True)
        
    #print("fields: " + str(fields))
    
    # [END search_tag_template]
    # [START render_template]
    return render_template(
        'tag_template.html',
        template_id=template_id,
        template_project=template_project,
        template_region=template_region,
        service_account=service_account,
        fields=fields)
    # [END render_template]
        
def view_remaining_configs(service_account, template_id, template_project, template_region):
    
    if 'credentials' not in session:
        return redirect('/')
        
    print("template_id: " + str(template_id))
    print("template_project: " + str(template_project))
    print("template_region: " + str(template_region))
    print("service_account: " + str(service_account))
    
    credentials, success = get_target_credentials(service_account)
    
    if success == False:
        print('Error acquiring credentials from', service_account)
    
    dcc = controller.DataCatalogController(credentials, None, None, template_id, template_project, template_region)
    template_fields = dcc.get_template()
    
    configs = store.read_configs(service_account, 'ALL', template_id, template_project, template_region)
    
    #print('configs: ', configs)
    
    return render_template(
        'view_configs.html',
        template_id=template_id,
        template_project=template_project,
        template_region=template_region,
        service_account=service_account,
        configs=configs)
    # [END render_template]
    

# [START view_export_configs]
@app.route('/view_export_configs', methods=['GET'])
def view_export_configs():
    
    if 'credentials' not in session:
        return redirect('/')
        
    configs = store.read_export_configs()
    
    print('configs: ', configs)
    
    return render_template(
        'view_export_configs.html',
        configs=configs)
    # [END render_template]

# [START view_config_options]
@app.route('/view_config_options', methods=['POST'])
def view_config_options():
    
    if 'credentials' not in session:
        return redirect('/')
        
    template_id = request.form['template_id']
    template_project = request.form['template_project']
    template_region = request.form['template_region']
    service_account = request.form['service_account']
    action = request.form['action']

    print("template_id: " + str(template_id))
    print("template_project: " + str(template_project))
    print("template_region: " + str(template_region))
    print("action: " + str(action))
    
    credentials, success = get_target_credentials(service_account)
    
    if success == False:
        print('Error acquiring credentials from', service_account)
    
    dcc = controller.DataCatalogController(credentials, None, None, template_id, template_project, template_region)
    template_fields = dcc.get_template()
    
    history_enabled, _ = store.read_tag_history_settings()
    
    if action == "View Existing Configs":

        configs = store.read_configs(service_account, 'ALL', template_id, template_project, template_region)
        
        print('configs: ', configs)
        
        return render_template(
            'view_configs.html',
            template_id=template_id,
            template_project=template_project,
            template_region=template_region,
            service_account=service_account,
            configs=configs)
        
    elif action == "Create Static Asset Tags":
        return render_template(
            'static_asset_config.html',
            template_id=template_id,
            template_project=template_project,
            template_region=template_region,
            service_account=service_account,
            fields=template_fields,
            current_time=datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
            tag_history_option=history_enabled)
            
    elif action == "Create Dynamic Table Tags":
        return render_template(
            'dynamic_table_config.html',
            template_id=template_id,
            template_project=template_project,
            template_region=template_region,
            service_account=service_account,
            fields=template_fields,
            tag_history_option=history_enabled)
            
    elif action == "Create Dynamic Column Tags":
        return render_template(
            'dynamic_column_config.html',
            template_id=template_id,
            template_project=template_project,
            template_region=template_region,
            service_account=service_account,
            fields=template_fields,
            tag_history_option=history_enabled)
            
    elif action == "Create Data Catalog Entries":
        return render_template(
            'entry_config.html',
            template_id=template_id,
            template_project=template_project,
            template_region=template_region,
            service_account=service_account,
            fields=template_fields,
            tag_history_option=history_enabled)
            
    elif action == "Create Glossary Asset Tags":
        return render_template(
            'glossary_asset_config.html',
            template_id=template_id,
            template_project=template_project,
            template_region=template_region,
            service_account=service_account,
            fields=template_fields,
            tag_history_option=history_enabled)
    
    elif action == "Create Sensitive Column Tags":
        return render_template(
            'sensitive_column_config.html',
            template_id=template_id,
            template_project=template_project,
            template_region=template_region,
            service_account=service_account,
            fields=template_fields,
            tag_history_option=history_enabled)
                        
    elif action == "Import Tags":
        return render_template(
            'import_config.html',
            template_id=template_id,
            template_project=template_project,
            template_region=template_region,
            service_account=service_account,
            fields=template_fields,
            tag_history_option=history_enabled)
    
    elif action == "Restore Tags":
        return render_template(
            'restore_config.html',
            template_id=template_id,
            template_project=template_project,
            template_region=template_region,
            service_account=service_account,
            fields=template_fields,
            tag_history_option=history_enabled)
            
    elif action == "Switch Template / Return Home" or action == 'Return Home':
        return render_template(
            'home.html',
            template_id=template_id,
            template_project=template_project,
            template_region=template_region,
            service_account=service_account)
            
    # [END render_template]


# [START process_created_config_action]
@app.route('/process_created_config_action', methods=['POST'])
def process_created_config_action():
    
    if 'credentials' not in session:
        return redirect('/')
        
    config_uuid = request.form['config_uuid']
    config_type = request.form['config_type']
    
    if 'template_id' in request.form:
        template_id = request.form['template_id']
    else:
        template_id = 'N/A' # export config handler
    
    if 'template_project' in request.form:
        template_project = request.form['template_project']
    else:
        template_project = 'N/A' # export config handler
        
    if 'template_region' in request.form:
        template_region = request.form['template_region']
    else:
        template_region = 'N/A' # export config handler
        
    if template_id == 'N/A' and template_project == 'N/A' and template_region == 'N/A':
        target_project = request.form['target_project']
        target_dataset = request.form['target_dataset']
    else:
        target_project = 'N/A'
        target_dataset = 'N/A'
    
    service_account = request.form['service_account']
    
    action = request.form['action']
    
    if action == "Trigger Job":
        job_uuid = jm.create_job(tag_creator_account=service_account, tag_invoker_account=session['user_email'], config_uuid=config_uuid, config_type=config_type)
        job = jm.get_job_status(job_uuid)
        
        entries = get_log_entries(service_account)
        
        return render_template(
            'job_status.html',
            job_uuid=job_uuid,
            entries=entries,
            job_status=job['job_status'],
            config_uuid=config_uuid,
            config_type=config_type,
            template_id=template_id,
            template_project=template_project,
            template_region=template_region,
            service_account=service_account,
            target_project=target_project,
            target_dataset=target_dataset)
   
    if action == "View Existing Configs":
        configs = store.read_configs(service_account, 'ALL', template_id, template_project, template_region)

        return render_template(
            'view_configs.html',
            template_id=template_id,
            template_project=template_project,
            template_region=template_region,
            service_account=service_account,
            configs=configs)
    
    if action == "Return Home":
        return render_template(
            'home.html',
            template_id=template_id,
            template_project=template_project,
            template_region=template_region,
            service_account=service_account)


# [START refresh_job_status]
@app.route('/refresh_job_status', methods=['POST'])
def refresh_job_status():
    
    if 'credentials' not in session:
        return redirect('/')
    
    job_uuid = request.form['job_uuid']    
    config_uuid = request.form['config_uuid']
    config_type = request.form['config_type']
    
    if 'template_id' in request.form:
        template_id = request.form['template_id']
    else:
        template_id = 'N/A' # export config handler
    
    if 'template_project' in request.form:
        template_project = request.form['template_project']
    else:
        template_project = 'N/A' # export config handler
        
    if 'template_region' in request.form:
        template_region = request.form['template_region']
    else:
        template_region = 'N/A' # export config handler
        
    if template_id == 'N/A' and template_project == 'N/A' and template_region == 'N/A':
        target_project = request.form['target_project']
        target_dataset = request.form['target_dataset']
    else:
        target_project = 'N/A'
        target_dataset = 'N/A'
    
    service_account = request.form['service_account']
    action = request.form['action']
    
    if action == "Refresh":
        job = jm.get_job_status(job_uuid)
        entries = get_log_entries(service_account)
        
        return render_template(
            'job_status.html',
            job_uuid=job_uuid,
            entries=entries,
            job_status=job['job_status'],
            config_uuid=config_uuid,
            config_type=config_type,
            template_id=template_id,
            template_project=template_project,
            template_region=template_region,
            service_account=service_account,
            target_project=target_project,
            target_dataset=target_dataset)
    
    if action == "View Existing Configs":
        configs = store.read_configs(service_account, 'ALL', template_id, template_project, template_region)

        return render_template(
            'view_configs.html',
            template_id=template_id,
            template_project=template_project,
            template_region=template_region,
            service_account=service_account,
            configs=configs)
    
    if action == "Return Home":
        return render_template(
            'home.html',
            template_id=template_id,
            template_project=template_project,
            template_region=template_region,
            service_account=service_account)


# [START display_export_option]
@app.route('/display_export_option', methods=['POST'])
def display_export_option():
    
    if 'credentials' not in session:
        return redirect('/')
        
    action = request.form['action']
    
    if action == "Create Export Config":
        return render_template('export_config.html')
            
    elif action == "View and Edit Configs":
        return view_export_configs()


@app.route('/create_export_option', methods=['GET'])
def create_export_option():
    
    if 'credentials' not in session:
        return redirect('/')
        
    return render_template(
            'export_config.html')
            

@app.route('/choose_config_action', methods=['POST'])
def choose_config_action():
    
    if 'credentials' not in session:
        return redirect('/')
        
    template_id = request.form['template_id']
    template_project = request.form['template_project']
    template_region = request.form['template_region']
    service_account = request.form['service_account']
    
    config_uuid = request.form['config_uuid']
    config_type = request.form['config_type']
    action = request.form['action']
    
    print('template_id:', template_id)
    print('template_project:', template_project)
    print('template_region:', template_region)
    print('service_account:', service_account)
    
    print('config_uuid:', config_uuid)
    print('config_type:', config_type)
    print('action:', action)

    if action == "View Job History":
        jobs = store.read_jobs_by_config(config_uuid)
        
        return render_template(
            'job_history.html',
            jobs=jobs,
            config_uuid=config_uuid,
            config_type=config_type,
            template_id=template_id,
            template_project=template_project,
            template_region=template_region,
            service_account=service_account)

    if action == "Trigger Job":
        job_uuid = jm.create_job(tag_creator_account=service_account, tag_invoker_account=session['user_email'], config_uuid=config_uuid, config_type=config_type)
        job = jm.get_job_status(job_uuid)
        entries = get_log_entries(service_account)

        return render_template(
            'job_status.html',
            job_uuid=job_uuid,
            entries=entries,
            job_status=job['job_status'],
            config_uuid=config_uuid,
            config_type=config_type,
            template_id=template_id,
            template_project=template_project,
            template_region=template_region,
            service_account=service_account)
   
    if action == "View Configs":
        print("View Configs")
    
    if action == "Delete Config":
        store.delete_config(service_account, config_uuid, config_type)
        return view_remaining_configs(service_account, template_id, template_project, template_region)
    
    # action == Update Config
    config = store.read_config(service_account, config_uuid, config_type)
    print("config: " + str(config))
    
    credentials, success = get_target_credentials(service_account)
    
    if success == False:
        print('Error acquiring credentials from', service_account)
    
    dcc = controller.DataCatalogController(credentials, None, None, template_id, template_project, template_region)
    template_fields = dcc.get_template()
    print('template_fields:', template_fields)
    
    if config_type == "STATIC_TAG_ASSET":
        return render_template(
            'update_static_asset_config.html',
            template_id=template_id,
            template_project=template_project,
            template_region=template_region,
            service_account=service_account,
            fields=template_fields,
            config=config, 
            current_time=datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'))
    
    if config_type == "DYNAMIC_TAG_TABLE":
        return render_template(
            'update_dynamic_table_config.html',
            template_id=template_id,
            template_project=template_project,
            template_region=template_region,
            service_account=service_account,
            fields=template_fields,
            config=config)
            
    if config_type == "DYNAMIC_TAG_COLUMN":
        return render_template(
            'update_dynamic_column_config.html',
            template_id=template_id,
            template_project=template_project,
            template_region=template_region,
            service_account=service_account,
            fields=template_fields,
            config=config)
            
    if config_type == "ENTRY_CREATE":
        return render_template(
            'update_entry_config.html',
            template_id=template_id,
            template_project=template_project,
            template_region=template_region,
            service_account=service_account,
            fields=template_fields,
            config=config)
            
    if config_type == "GLOSSARY_TAG_ASSET":
        return render_template(
            'update_glossary_asset_config.html',
            template_id=template_id,
            template_project=template_project,
            template_region=template_region,
            service_account=service_account,
            fields=template_fields,
            config=config)
            
    if config_type == "SENSITIVE_TAG_COLUMN":
        return render_template(
            'update_sensitive_column_config.html',
            template_id=template_id,
            template_project=template_project,
            template_region=template_region,
            service_account=service_account,
            fields=template_fields,
            config=config)
    
    if config_type == "TAG_IMPORT":
        return render_template(
            'update_import_config.html',
            template_id=template_id,
            template_project=template_project,
            template_region=template_region,
            service_account=service_account,
            config=config)
            
    if config_type == "TAG_RESTORE":
        return render_template(
            'update_restore_config.html',
            template_id=template_id,
            template_project=template_project,
            template_region=template_region,
            service_account=service_account,
            config=config)
    # [END render_template]
    

@app.route('/choose_job_history_action', methods=['POST'])
def choose_job_history_action():
    
    if 'credentials' not in session:
        return redirect('/')
        
    config_uuid = request.form['config_uuid']
    config_type = request.form['config_type']
    
    template_id = request.form['template_id']
    template_project = request.form['template_project']
    template_region = request.form['template_region']
    service_account = request.form['service_account']
    
    action = request.form['action']

    if action == "Trigger Job":
        job_uuid = jm.create_job(tag_creator_account=service_account, tag_invoker_account=session['user_email'], config_uuid=config_uuid, config_type=config_type)
        job = jm.get_job_status(job_uuid)
        
        entries = get_log_entries(service_account)

        return render_template(
            'job_status.html',
            job_uuid=job_uuid,
            entries=entries,
            job_status=job['job_status'],
            config_uuid=config_uuid,
            config_type=config_type,
            template_id=template_id,
            template_project=template_project,
            template_region=template_region,
            service_account=service_account)
    
    if action == "View Existing Configs":
        configs = store.read_configs(service_account, 'ALL', template_id, template_project, template_region)
        print('configs: ', configs)
        
        return render_template(
            'view_configs.html',
            template_id=template_id,
            template_project=template_project,
            template_region=template_region,
            service_account=service_account,
            configs=configs)
            
    if action == "Return Home":
        return render_template(
            'home.html',
            template_id=template_id,
            template_project=template_project,
            template_region=template_region,
            service_account=service_account)
    
    
@app.route('/update_export_config', methods=['POST'])
def update_export_config():
    
    if 'credentials' not in session:
        return redirect('/')
        
    config_uuid = request.form['config_uuid']
    action = request.form['action']
    
    print('config_uuid: ', config_uuid)
    
    if action == "Delete Config":
        store.delete_config(config_uuid, 'EXPORT_TAG')
        return view_export_configs()
    
    config = store.read_config(config_uuid, 'EXPORT_TAG', reformat=True)
    print("config: " + str(config))
    
    return render_template(
        'update_export_config.html',
        config=config)
    
    
@app.route('/process_static_asset_config', methods=['POST'])
def process_static_asset_config():
    
    if 'credentials' not in session:
        return redirect('/')
        
    template_id = request.form['template_id']
    template_project = request.form['template_project']
    template_region = request.form['template_region']
    service_account = request.form['service_account']
    included_assets_uris = request.form['included_assets_uris'].rstrip()
    excluded_assets_uris = request.form['excluded_assets_uris'].rstrip()
    refresh_mode = request.form['refresh_mode']
    refresh_frequency = request.form['refresh_frequency'].rstrip()
    refresh_unit = request.form['refresh_unit']
    action = request.form['action']
    
    print('included_assets_uris: ' + included_assets_uris)
    print('excluded_assets_uris: ' + excluded_assets_uris)
    print('service_account: ' + service_account)

    credentials, success = get_target_credentials(service_account)
    
    if success == False:
        print('Error acquiring credentials from', service_account)

    dcc = controller.DataCatalogController(credentials, None, None, template_id, template_project, template_region)
    template = dcc.get_template()
    
    if action == "Cancel Changes":
        
        return render_template(
            'tag_template.html',
            template_id=template_id,
            template_project=template_project,
            template_region=template_region, 
            service_account=service_account, 
            fields=template)
    
    if action == "View Existing Configs":

        configs = store.read_configs(service_account, 'ALL', template_id, template_project, template_region)

        return render_template(
            'view_configs.html',
            template_id=template_id,
            template_project=template_project,
            template_region=template_region,
            service_account=service_account,
            configs=configs)
    
    fields = []
    
    selected_fields = request.form.getlist("selected")
    print("selected_fields: " + str(selected_fields))
    
    for selected_field in selected_fields:
        selected_type = request.form.get(selected_field + "_datatype")

        if selected_type == 'bool':
            selected_value = request.form.get(selected_field)
            
            if selected_value.lower() == 'true':
                selected_value = True
            else:
                selected_value = False
        else:
            selected_value = request.form.get(selected_field)
        
        #print(selected_field + ", " + str(selected_value) + ", " + selected_type)
        
        for template_field in template:
            
            if template_field['field_id'] != selected_field:
                continue
            
            is_required = template_field['is_required']
            field = {'field_id': selected_field, 'field_value': selected_value, 'field_type': selected_type, 'is_required': is_required}
            fields.append(field)
            break
    
    #print('fields: ' + str(fields))
    
    if excluded_assets_uris == 'None':
        excluded_assets_uris = ''
    
    tag_history_option, _ = store.read_tag_history_settings()
    
    if tag_history_option == True:
        tag_history_display = "ON"
    else:
        tag_history_display = "OFF"
                    
    template_uuid = store.write_tag_template(template_id, template_project, template_region)
    
    # TO DO: decide how best to let users specify the overwrite field from the UI 
    config_uuid = store.write_static_asset_config(service_account, fields, included_assets_uris, excluded_assets_uris, \
                                                  template_uuid, template_id, template_project, template_region, \
                                                  refresh_mode, refresh_frequency, refresh_unit, tag_history_option, overwrite=True)
    
    # [START render_template]
    return render_template(
        'created_static_asset_config.html',
        config_uuid=config_uuid,
        config_type='STATIC_TAG_ASSET',
        template_id=template_id,
        template_project=template_project,
        template_region=template_region,
        service_account=service_account,
        fields=fields,
        included_assets_uris=included_assets_uris,
        excluded_assets_uris=excluded_assets_uris,
        tag_history=tag_history_display)
    # [END render_template]


@app.route('/process_dynamic_table_config', methods=['POST'])
def process_dynamic_table_config():
    
    if 'credentials' not in session:
        return redirect('/')
        
    template_id = request.form['template_id']
    template_project = request.form['template_project']
    template_region = request.form['template_region']
    service_account = request.form['service_account']
    included_tables_uris = request.form['included_tables_uris'].rstrip()
    excluded_tables_uris = request.form['excluded_tables_uris'].rstrip()
    refresh_mode = request.form['refresh_mode']
    refresh_frequency = request.form['refresh_frequency']
    refresh_unit = request.form['refresh_unit']
    action = request.form['action']
    
    credentials, success = get_target_credentials(service_account)
    
    if success == False:
        print('Error acquiring credentials from', service_account)
    
    dcc = controller.DataCatalogController(credentials, None, None, template_id, template_project, template_region)
    template = dcc.get_template()
    
    if action == "Cancel Changes":
        
        return render_template(
            'tag_template.html',
            template_id=template_id,
            template_project=template_project,
            template_region=template_region, 
            service_account=service_account, 
            fields=template)

    fields = []
    selected_fields = request.form.getlist("selected")
    #print("selected_fields: " + str(selected_fields))
    
    for selected_field in selected_fields:
        query_expression = request.form.get(selected_field).replace('\t', '').replace('\r', '').replace('\n', ' ').strip()
        #print("query_expression: " + query_expression)
        selected_field_type = request.form.get(selected_field + "_datatype")
        #print("selected_field_type: " + selected_field_type)
        print(selected_field + ", " + query_expression + ", " + selected_field_type)
        
        for template_field in template:
            
            if template_field['field_id'] != selected_field:
                continue
        
            is_required = template_field['is_required']
            field = {'field_id': selected_field, 'query_expression': query_expression, 'field_type': selected_field_type,\
                     'is_required': is_required}
            fields.append(field)
            break
    
    #print('fields: ' + str(fields))
    
    if excluded_tables_uris == 'None':
        excluded_tables_uris = ''
    
    tag_history_option, _ = store.read_tag_history_settings()
    
    if tag_history_option == True:
        tag_history_display = "ON"
    else:
        tag_history_display = "OFF"
    
    template_uuid = store.write_tag_template(template_id, template_project, template_region)
    config_uuid = store.write_dynamic_table_config(service_account, fields, included_tables_uris, excluded_tables_uris, \
                                                   template_uuid, template_id, template_project, template_region, \
                                                   refresh_mode, refresh_frequency, refresh_unit, tag_history_option)
     
    # [END process_dynamic_table_config]
    # [START render_template]
    return render_template(
        'created_dynamic_table_config.html',
        config_uuid=config_uuid,
        config_type='DYNAMIC_TAG_TABLE',
        template_id=template_id,
        template_project=template_project,
        template_region=template_region,
        service_account=service_account,
        fields=fields,
        included_tables_uris=included_tables_uris,
        excluded_tables_uris=excluded_tables_uris,
        refresh_mode=refresh_mode,
        refresh_frequency=refresh_frequency,
        refresh_unit=refresh_unit,
        tag_history=tag_history_display)
    # [END render_template]


@app.route('/process_dynamic_column_config', methods=['POST'])
def process_dynamic_column_config():
    template_id = request.form['template_id']
    template_project = request.form['template_project']
    template_region = request.form['template_region']
    service_account = request.form['service_account']
    included_columns_query = request.form['included_columns_query']
    included_tables_uris = request.form['included_tables_uris'].rstrip()
    excluded_tables_uris = request.form['excluded_tables_uris'].rstrip()
    refresh_mode = request.form['refresh_mode']
    refresh_frequency = request.form['refresh_frequency']
    refresh_unit = request.form['refresh_unit']
    action = request.form['action']
    
    credentials, success = get_target_credentials(service_account)
    
    if success == False:
        print('Error acquiring credentials from', service_account)
    
    dcc = controller.DataCatalogController(credentials, None, None, template_id, template_project, template_region)
    template = dcc.get_template()
    
    if action == "Cancel Changes":
        
        return render_template(
            'tag_template.html',
            template_id=template_id,
            template_project=template_project,
            template_region=template_region, 
            service_account=service_account, 
            fields=template)
    
    fields = []
    selected_fields = request.form.getlist("selected")
    #print("selected_fields: " + str(selected_fields))
    
    for selected_field in selected_fields:
        query_expression = request.form.get(selected_field).replace('\t', '').replace('\r', '').replace('\n', ' ').strip()
        #print("query_expression: " + query_expression)
        selected_field_type = request.form.get(selected_field + "_datatype")
        #print("selected_field_type: " + selected_field_type)
        print(selected_field + ", " + query_expression + ", " + selected_field_type)
        
        for template_field in template:
            
            if template_field['field_id'] != selected_field:
                continue
        
            is_required = template_field['is_required']
            field = {'field_id': selected_field, 'query_expression': query_expression, 'field_type': selected_field_type,\
                     'is_required': is_required}
            fields.append(field)
            break
    
    #print('fields: ' + str(fields))
    
    if excluded_tables_uris == 'None':
        excluded_tables_uris = ''
    
    tag_history_option, _ = store.read_tag_history_settings()
    
    if tag_history_option == True:
        tag_history_display = "ON"
    else:
        tag_history_display = "OFF"
     
    template_uuid = store.write_tag_template(template_id, template_project, template_region)
    config_uuid = store.write_dynamic_column_config(service_account, fields, included_columns_query, \
                                                    included_tables_uris, excluded_tables_uris, template_uuid,\
                                                    refresh_mode, refresh_frequency, refresh_unit, \
                                                    tag_history_option)
     
    # [END process_dynamic_column_config]
    # [START render_template]
    return render_template(
        'created_dynamic_column_config.html',
        config_uuid=config_uuid,
        config_type='DYNAMIC_TAG_COLUMN',
        template_id=template_id,
        template_project=template_project,
        template_region=template_region,
        service_account=service_account,
        fields=fields,
        included_columns_query=included_columns_query,
        included_tables_uris=included_tables_uris,
        excluded_tables_uris=excluded_tables_uris,
        refresh_mode=refresh_mode,
        refresh_frequency=refresh_frequency,
        refresh_unit=refresh_unit,
        tag_history=tag_history_display)
    # [END render_template]


@app.route('/process_entry_config', methods=['POST'])
def process_entry_config():
    template_id = request.form['template_id']
    template_project = request.form['template_project']
    template_region = request.form['template_region']
    service_account = request.form['service_account']
    included_assets_uris = request.form['included_assets_uris'].rstrip()
    excluded_assets_uris = request.form['excluded_assets_uris'].rstrip()
    refresh_mode = request.form['refresh_mode']
    refresh_frequency = request.form['refresh_frequency']
    refresh_unit = request.form['refresh_unit']
    action = request.form['action']
    
    #print('included_assets_uris: ' + included_assets_uris)
    #print('excluded_assets_uris: ' + excluded_assets_uris)
    #print('refresh_mode: ' + refresh_mode)
    #print('refresh_frequency: ' + refresh_frequency)
    #print('refresh_unit: ' + refresh_unit)
    
    credentials, success = get_target_credentials(service_account)
    
    if success == False:
        print('Error acquiring credentials from', service_account)
    
    dcc = controller.DataCatalogController(credentials, None, None, template_id, template_project, template_region)
    template = dcc.get_template()
    
    if action == "Cancel Changes":
        
        return render_template(
            'tag_template.html',
            template_id=template_id,
            template_project=template_project,
            template_region=template_region,
            service_account=service_account,  
            fields=template)

    fields = []
    
    selected_fields = request.form.getlist("selected")
    #print("selected_fields: " + str(selected_fields))
    
    for selected_field in selected_fields:
        selected_field_type = request.form.get(selected_field + "_datatype")
        #print(selected_field + ", " + selected_field_type)
        
        for template_field in template:
            
            if template_field['field_id'] != selected_field:
                continue
        
            is_required = template_field['is_required']
            field = {'field_id': selected_field, 'field_type': selected_field_type,\
                     'is_required': is_required}
            fields.append(field)
            break
    
    #print('fields: ' + str(fields))
    
    if excluded_assets_uris == 'None':
        excluded_assets_uris = ''
    
    tag_history_option, _ = store.read_tag_history_settings()
    
    if tag_history_option == True:
        tag_history_display = "ON"
    else:
        tag_history_display = "OFF"
        
    template_uuid = store.write_tag_template(template_id, template_project, template_region)
    config_uuid = store.write_entry_config(service_account, fields, included_assets_uris, excluded_assets_uris, template_uuid,\
                                           template_id, template_project, template_region, \
                                           refresh_mode, refresh_frequency, refresh_unit, tag_history_option)
     
    # [END process_entry_config]
    # [START render_template]
    return render_template(
        'created_entry_config.html',
        config_uuid=config_uuid,
        config_type='ENTRY_CREATE',
        template_id=template_id,
        template_project=template_project,
        template_region=template_region,
        service_account=service_account,
        fields=fields,
        included_assets_uris=included_assets_uris,
        excluded_assets_uris=excluded_assets_uris,
        refresh_mode=refresh_mode,
        refresh_frequency=refresh_frequency,
        refresh_unit=refresh_unit,
        tag_history=tag_history_display)
    # [END render_template]


@app.route('/process_glossary_asset_config', methods=['POST'])
def process_glossary_asset_config():
    template_id = request.form['template_id']
    template_project = request.form['template_project']
    template_region = request.form['template_region']
    service_account = request.form['service_account']
    mapping_table = request.form['mapping_table'].rstrip()
    included_assets_uris = request.form['included_assets_uris'].rstrip()
    excluded_assets_uris = request.form['excluded_assets_uris'].rstrip()
    refresh_mode = request.form['refresh_mode']
    refresh_frequency = request.form['refresh_frequency']
    refresh_unit = request.form['refresh_unit']
    overwrite = True # set to true as we are creating a new glossary asset config
    action = request.form['action']
    
    credentials, success = get_target_credentials(service_account)
    
    if success == False:
        print('Error acquiring credentials from', service_account)
    
    dcc = controller.DataCatalogController(credentials, None, None, template_id, template_project, template_region)
    template = dcc.get_template()
    
    if action == "Cancel Changes":
        
        return render_template(
            'tag_template.html',
            template_id=template_id,
            template_project=template_project,
            template_region=template_region, 
            service_account=service_account, 
            fields=template)

    fields = []
    
    selected_fields = request.form.getlist("selected")
    #print("selected_fields: " + str(selected_fields))
    
    for selected_field in selected_fields:
        selected_field_type = request.form.get(selected_field + "_datatype")
        #print(selected_field + ", " + selected_field_type)
        
        for template_field in template:
            
            if template_field['field_id'] != selected_field:
                continue
        
            is_required = template_field['is_required']
            field = {'field_id': selected_field, 'field_type': selected_field_type,\
                     'is_required': is_required}
            fields.append(field)
            break
    
    #print('fields: ' + str(fields))
    
    if excluded_assets_uris == 'None':
        excluded_assets_uris = ''
    
    tag_history_option, _ = store.read_tag_history_settings()
    
    if tag_history_option == True:
        tag_history_display = "ON"
    else:
        tag_history_display = "OFF"
    
    template_uuid = store.write_tag_template(template_id, template_project, template_region)

    config_uuid = store.write_glossary_asset_config(service_account, fields, mapping_table, included_assets_uris, \
                                                    excluded_assets_uris, template_uuid, template_id, template_project, template_region, \
                                                    refresh_mode, refresh_frequency, refresh_unit, \
                                                    tag_history_option, overwrite)
     
    # [END process_dynamic_tag]
    # [START render_template]
    return render_template(
        'created_glossary_asset_config.html',
        config_uuid=config_uuid,
        config_type='GLOSSARY_TAG_ASSET',
        template_id=template_id,
        template_project=template_project,
        template_region=template_region,
        service_account=service_account,
        fields=fields,
        mapping_table=mapping_table,
        included_assets_uris=included_assets_uris,
        excluded_assets_uris=excluded_assets_uris,
        refresh_mode=refresh_mode,
        refresh_frequency=refresh_frequency,
        refresh_unit=refresh_unit,
        tag_history=tag_history_display)
    # [END render_template]


@app.route('/process_sensitive_column_config', methods=['POST'])
def process_sensitive_column_config():
    template_id = request.form['template_id']
    template_project = request.form['template_project']
    template_region = request.form['template_region']
    service_account = request.form['service_account']
    dlp_dataset = request.form['dlp_dataset'].rstrip()
    infotype_selection_table = request.form['infotype_selection_table'].rstrip()
    infotype_classification_table = request.form['infotype_classification_table'].rstrip()
    included_tables_uris = request.form['included_tables_uris'].rstrip()
    excluded_tables_uris = request.form['excluded_tables_uris'].rstrip()
    
    # policy tag inputs
    policy_tags = request.form['policy_tags']
    if policy_tags == "true":
        create_policy_tags = True
        taxonomy_id = request.form['taxonomy_id'].rstrip()
    else:
        create_policy_tags = False
        taxonomy_id = None
    
    refresh_mode = request.form['refresh_mode']
    refresh_frequency = request.form['refresh_frequency']
    refresh_unit = request.form['refresh_unit']
    overwrite = True # set to true as we are creating a new sensitive config
    action = request.form['action']
    
    credentials, success = get_target_credentials(service_account)
    
    if success == False:
        print('Error acquiring credentials from', service_account)
    
    dcc = controller.DataCatalogController(credentials, None, None, template_id, template_project, template_region)
    template = dcc.get_template()
    
    if action == "Cancel Changes":
        
        return render_template(
            'tag_template.html',
            template_id=template_id,
            template_project=template_project,
            template_region=template_region,
            service_account=service_account,  
            fields=template)

    fields = []
    
    selected_fields = request.form.getlist("selected")
    #print("selected_fields: " + str(selected_fields))
    
    for selected_field in selected_fields:
        selected_field_type = request.form.get(selected_field + "_datatype")
        #print(selected_field + ", " + selected_field_type)
        
        for template_field in template:
            
            if template_field['field_id'] != selected_field:
                continue
        
            is_required = template_field['is_required']
            field = {'field_id': selected_field, 'field_type': selected_field_type,\
                     'is_required': is_required}
            fields.append(field)
            break
    
    #print('fields: ' + str(fields))
    
    if excluded_tables_uris == 'None':
        excluded_tables_uris = ''
    
    tag_history_option, _ = store.read_tag_history_settings()
    
    if tag_history_option == True:
        tag_history_display = "ON"
    else:
        tag_history_display = "OFF"
    
    template_uuid = store.write_tag_template(template_id, template_project, template_region)

    config_uuid = store.write_sensitive_column_config(service_account, fields, dlp_dataset, infotype_selection_table, \
                                                      infotype_classification_table, included_tables_uris, excluded_tables_uris, \
                                                      create_policy_tags, taxonomy_id, template_uuid, template_id, template_project, \
                                                      template_region, refresh_mode, refresh_frequency, refresh_unit, \
                                                      tag_history_option, overwrite)
     
    # [END process_sensitive_column_config]
    # [START render_template]
    return render_template(
        'created_sensitive_column_config.html',
        config_uuid=config_uuid,
        config_type='SENSITIVE_TAG_COLUMN',
        template_id=template_id,
        template_project=template_project,
        template_region=template_region,
        service_account=service_account,
        fields=fields,
        dlp_dataset=dlp_dataset,
        infotype_selection_table=infotype_selection_table,
        infotype_classification_table=infotype_classification_table,
        included_tables_uris=included_tables_uris,
        excluded_tables_uris=excluded_tables_uris,
        policy_tags=policy_tags,
        taxonomy_id=taxonomy_id,
        refresh_mode=refresh_mode,
        refresh_frequency=refresh_frequency,
        refresh_unit=refresh_unit,
        tag_history=tag_history_display)
    # [END render_template]


@app.route('/process_restore_config', methods=['POST'])
def process_restore_config():
    
    template_id = request.form['template_id']
    template_project = request.form['template_project']
    template_region = request.form['template_region']
    service_account = request.form['service_account']
    action = request.form['action']
    
    credentials, success = get_target_credentials(service_account)
    
    if success == False:
        print('Error acquiring credentials from', service_account)
    
    dcc = controller.DataCatalogController(credentials, None, None, template_id, template_project, template_region)
    template = dcc.get_template()
    
    if action == "Cancel Changes":
        
        return render_template(
            'tag_template.html',
            template_id=template_id,
            template_project=template_project,
            template_region=template_region, 
            service_account=service_account, 
            fields=template)
            
    source_template_id = request.form['source_template_id']
    source_template_project = request.form['source_template_project']
    source_template_region = request.form['source_template_region']
    
    target_template_id = request.form['target_template_id']
    target_template_project = request.form['target_template_project']
    target_template_region = request.form['target_template_region']
    
    metadata_export_location = request.form['metadata_export_location']
    
    action = request.form['action']
      
    tag_history_option, _ = store.read_tag_history_settings()
    
    if tag_history_option == True:
        tag_history_display = "ON"
    else:
        tag_history_display = "OFF"
        
    source_template_uuid = store.write_tag_template(source_template_id, source_template_project, source_template_region)
    target_template_uuid = store.write_tag_template(target_template_id, target_template_project, target_template_region)
    
    config_uuid = store.write_tag_restore_config(service_account, source_template_uuid, source_template_id, source_template_project, \
                                                 source_template_region, target_template_uuid, target_template_id, target_template_project, \
                                                 target_template_region, metadata_export_location, tag_history_option)                                                      

    # [END process_restore_config]
    # [START render_template]
    return render_template(
        'created_restore_config.html',
        config_uuid=config_uuid,
        config_type='RESTORE_TAG',
        source_template_id=source_template_id,
        source_template_project=source_template_project,
        source_template_region=source_template_region,
        target_template_id=target_template_id,
        target_template_project=target_template_project,
        target_template_region=target_template_region,
        service_account = service_account,
        metadata_export_location=metadata_export_location,
        tag_history=tag_history_display)
    # [END render_template]


@app.route('/process_import_config', methods=['POST'])
def process_import_config():
    
    template_id = request.form['template_id']
    template_project = request.form['template_project']
    template_region = request.form['template_region']
    service_account = request.form['service_account']
    metadata_import_location = request.form['metadata_import_location']
    
    action = request.form['action']
    
    credentials, success = get_target_credentials(service_account)
    
    if success == False:
        print('Error acquiring credentials from', service_account)
    
    dcc = controller.DataCatalogController(credentials, None, None, template_id, template_project, template_region)
    template = dcc.get_template()
    
    if action == "Cancel Changes":
        
        return render_template(
            'tag_template.html',
            template_id=template_id,
            template_project=template_project,
            template_region=template_region, 
            service_account=service_account, 
            fields=template)
        
    tag_history_option, _ = store.read_tag_history_settings()
    
    if tag_history_option == True:
        tag_history_display = "ON"
    else:
        tag_history_display = "OFF"          
        
    template_uuid = store.write_tag_template(template_id, template_project, template_region)
          
    config_uuid = store.write_tag_import_config(service_account, template_uuid, template_id, template_project, template_region, \
                                                metadata_import_location, tag_history_option)                                                      

    # [END process_import_config]
    # [START render_template]
    return render_template(
        'created_import_config.html',
        config_uuid=config_uuid,
        config_type='IMPORT_TAG',
        template_id=template_id,
        template_project=template_project,
        template_region=template_region,
        service_account=service_account,
        metadata_import_location=metadata_import_location,
        tag_history=tag_history_display)
    # [END render_template]


@app.route('/process_export_config', methods=['POST'])
def process_export_config():
    
    source_projects = request.form['source_projects']
    source_folder = request.form['source_folder']
    source_region = request.form['source_region']
    
    target_project = request.form['target_project']
    target_dataset = request.form['target_dataset']
    target_region = request.form['target_region']
    write_option = request.form['write_option']
    
    service_account = request.form['service_account']
    
    refresh_mode = request.form['refresh_mode']
    refresh_frequency = request.form['refresh_frequency']
    refresh_unit = request.form['refresh_unit']
    
    action = request.form['action']
        
    if action == "Cancel Changes":
        
        return home()
            
    # put source projects into a list
    print('source_projects:', source_projects)
    project_list = []
    projects = source_projects.split(',')
    for project in projects:
        project_list.append(project.strip())
    print('project_list:', project_list)
        
    config_uuid = store.write_tag_export_config(service_account, project_list, source_folder, source_region, \
                                                target_project, target_dataset, target_region, write_option, \
                                                refresh_mode, refresh_frequency, refresh_unit)                                                      

    # [END process_export_config]
    # [START render_template]
    return render_template(
        'created_export_config.html',
        config_uuid=config_uuid,
        config_type='TAG_EXPORT',
        source_projects=source_projects,
        source_folder=source_folder,
        source_region=source_region,
        target_project=target_project,
        target_dataset=target_dataset,
        target_region=target_region,
        service_account=service_account,
        write_option=write_option,
        refresh_mode=refresh_mode,
        refresh_frequency=refresh_frequency,
        refresh_unit=refresh_unit)
    # [END render_template]


##################### INTERNAL METHODS #################

def get_refresh_parameters(json_request):
    
    refresh_mode = json_request['refresh_mode']
    refresh_frequency = ''
    refresh_unit = ''
    
    if refresh_mode == 'AUTO':
        if 'refresh_frequency' in json_request:
            refresh_frequency = json_request['refresh_frequency']
        else:
            print("config request must include a refresh_frequency when refresh_mode is set to AUTO. This is a required parameter.")
            resp = jsonify(success=False)
            return resp
    
    if refresh_mode == 'AUTO':
        if 'refresh_unit' in json_request:
            refresh_unit = json_request['refresh_unit']
        else:
            print("config request must include a refresh_unit when refresh_mode is set to AUTO. This is a required parameter.")
            resp = jsonify(success=False)
            return resp
    
    return refresh_mode, refresh_frequency, refresh_unit


def check_template_parameters(request_name, json_request):

    valid_parameters = True

    if 'template_id' in json_request:
        template_id = json_request['template_id']
    else:
        print("The " + request_name + " request requires a template_id parameter.")
        valid_parameters = False
        return valid_parameters, None, None, None

    if 'template_project' in json_request:
        template_project = json_request['template_project']
    else:
        print("The " + request_name + " request requires a template_project parameter.")
        valid_parameters = False
        return valid_parameters, None, None, None

    if 'template_region' in json_request:
        template_region = json_request['template_region']
    else:
        print("The " + request_name + " request requires a template_region parameter.")
        valid_parameters = False
        return valid_parameters, None, None, None

    return valid_parameters, template_id, template_project, template_region

      
def check_config_type(requested_ct):
    
    print('*** enter check_config_type ***')
    
    for available_ct in (ct.ConfigType):
        if available_ct.name == requested_ct:
            return True
    
    return False


def get_available_config_types():
    
    print('*** enter get_available_config_types ***')
    config_types = ''
    
    for config_type in (ct.ConfigType):
        config_types += config_type.name + ', '
            
    return config_types[0:-2]

    
##################### API METHODS #################

"""
Args:
    template_id: tag template to use
    template_project: tag template's Google Cloud project 
    template_region: tag template's region 
    fields: list of all the template field names to include in the tag (no need to include the field type)
    included_tables_uris: The paths to the resources (either in BQ or GCS) 
    excluded_tables_uris: The paths to the resources to exclude (optional)
    refresh_mode: AUTO or ON_DEMAND
    refresh_frequency: positive integer
    refresh_unit: minutes or hours
Returns:
    config_uuid
"""
@app.route("/create_static_asset_config", methods=['POST'])
def create_static_asset_config():
    
    print('*** enter create_static_asset_config ***')
    
    json_request = request.get_json(force=True) 
    print('json request: ', json_request)
    
    status, response, tag_creator_sa = do_authentication(request.headers, json_request, ENABLE_AUTH)
    
    if status == False:
        return jsonify(response), 400
        
    valid_parameters, template_id, template_project, template_region = check_template_parameters('static_asset_config', json_request)
    
    if valid_parameters != True:
        response = {
                "status": "error",
                "message": "Request JSON is missing some required tag template parameters",
        }
        return jsonify(response), 400
     
    print('template_id: ' + template_id)
    print('template_project: ' + template_project)
    print('template_region: ' + template_region)
    
    template_uuid = store.write_tag_template(template_id, template_project, template_region)
    
    credentials, success = get_target_credentials(tag_creator_sa)
    
    if success == False:
        print('Error acquiring credentials from', tag_creator_sa)
    
    dcc = controller.DataCatalogController(credentials, None, None, template_id, template_project, template_region)
    fields = dcc.get_template(included_fields=json_request['fields'])
    #print('fields:', fields)
    
    if 'included_assets_uris' in json_request:
        included_assets_uris = json_request['included_assets_uris']
    else:
        print("The create_static_asset_config request requires an included_assets_uris parameter.")
        resp = jsonify(success=False)
        return resp
    
    if 'excluded_assets_uris' in json_request:
        excluded_assets_uris = json_request['excluded_assets_uris']
    else:
        excluded_assets_uris = ''
    
    refresh_mode, refresh_frequency, refresh_unit = get_refresh_parameters(json_request)
    
    if 'overwrite' in json_request:  
        overwrite = json_request['overwrite']
    else:
        overwrite = True
    
    tag_history_option, _ = store.read_tag_history_settings()     
    config_uuid = store.write_static_asset_config(tag_creator_sa, fields, included_assets_uris, \
                                                  excluded_assets_uris, template_uuid, template_id, \
                                                  template_project, template_region, refresh_mode, refresh_frequency, refresh_unit, \
                                                  tag_history_option, overwrite)

    return jsonify(config_uuid=config_uuid, config_type='STATIC_TAG_ASSET')


"""
Args:
    template_id: tag template to use
    template_project: tag template's Google Cloud project 
    template_region: tag template's region 
    fields: list of all the template field names to include in the tag (no need to include the field type)
    included_tables_uris: The paths to the resources (either in BQ or GCS) 
    excluded_tables_uris: The paths to the resources to exclude (optional)
    refresh_mode: AUTO or ON_DEMAND
    refresh_frequency: positive integer
    refresh_unit: minutes or hours
Returns:
    config_uuid 
"""
@app.route("/create_dynamic_table_config", methods=['POST'])
def create_dynamic_table_config():
    
    json_request = request.get_json(force=True) 
    print('json request: ', json_request)
    
    status, response, tag_creator_sa = do_authentication(request.headers, json_request, ENABLE_AUTH)
    
    if status == False:
        return jsonify(response), 400
       
    valid_parameters, template_id, template_project, template_region = check_template_parameters('dynamic_table_config', json_request)
    
    if valid_parameters != True:
        response = {
                "status": "error",
                "message": "Request JSON is missing some required tag template parameters",
        }
        return jsonify(response), 400
     
    #print('template_id: ' + template_id)
    #print('template_project: ' + template_project)
    #print('template_region: ' + template_region)
    
    template_uuid = store.write_tag_template(template_id, template_project, template_region)
    
    credentials, success = get_target_credentials(tag_creator_sa)
    
    if success == False:
        print('Error acquiring credentials from', tag_creator_sa)
    
    dcc = controller.DataCatalogController(credentials, None, None, template_id, template_project, template_region)
    included_fields = json_request['fields']
    
    fields = dcc.get_template(included_fields=included_fields)
    print('field:', fields)
    
    if 'included_tables_uris' in json_request:
        included_tables_uris = json_request['included_tables_uris']
    else:
        print("The create_dynamic_table_config request requires an included_tables_uris parameter.")
        resp = jsonify(success=False)
        return resp
    
    if 'excluded_tables_uris' in json_request:
        excluded_tables_uris = json_request['excluded_tables_uris']
    else:
        excluded_tables_uris = ''
    
    refresh_mode, refresh_frequency, refresh_unit = get_refresh_parameters(json_request)
    
    tag_history_option, _ = store.read_tag_history_settings()
    config_uuid = store.write_dynamic_table_config(tag_creator_sa, fields, included_tables_uris, excluded_tables_uris, \
                                                   template_uuid, template_id, template_project, template_region, \
                                                   refresh_mode, refresh_frequency, refresh_unit, \
                                                   tag_history_option)                                                      


    return jsonify(config_uuid=config_uuid, config_type='DYNAMIC_TAG_TABLE')
    

"""
Args:
    template_id: tag template to use
    template_project: tag template's Google Cloud project 
    template_region: tag template's region 
    fields: list of all the template field names to include in the tag (no need to include the field type)
    included_tables_uris: The paths to the resources (either in BQ or GCS) 
    excluded_tables_uris: The paths to the resources to exclude (optional)
    refresh_mode: AUTO or ON_DEMAND
    refresh_frequency: positive integer
    refresh_unit: minutes or hours
Returns:
    job_uuid 
"""
@app.route("/create_dynamic_column_config", methods=['POST'])
def create_dynamic_column_config():
    
    json_request = request.get_json(force=True) 
    print('json request: ', json_request)
    
    status, response, tag_creator_sa = do_authentication(request.headers, json_request, ENABLE_AUTH)
    
    if status == False:
        return jsonify(response), 400
       
    valid_parameters, template_id, template_project, template_region = check_template_parameters('dynamic_column_config', json_request)
    
    if valid_parameters != True:
        response = {
                "status": "error",
                "message": "Request JSON is missing some required tag template parameters",
        }
        return jsonify(response), 400
     
    #print('template_id: ' + template_id)
    #print('template_project: ' + template_project)
    #print('template_region: ' + template_region)
    
    template_uuid = store.write_tag_template(template_id, template_project, template_region)
    
    credentials, success = get_target_credentials(tag_creator_sa)
    
    if success == False:
        print('Error acquiring credentials from', tag_creator_sa)
    
    dcc = controller.DataCatalogController(credentials, None, None, template_id, template_project, template_region)
    included_fields = json_request['fields']
    fields = dcc.get_template(included_fields=included_fields)

    if 'included_columns_query' in json_request:
        included_columns_query = json_request['included_columns_query']
    else:
        print("The create_dynamic_columns_config request requires an included_columns_query parameter.")
        resp = jsonify(success=False)
        return resp
    
    if 'included_tables_uris' in json_request:
        included_tables_uris = json_request['included_tables_uris']
    else:
        print("The create_dynamic_table_config request requires an included_tables_uris parameter.")
        resp = jsonify(success=False)
        return resp
    
    if 'excluded_tables_uris' in json_request:
        excluded_tables_uris = json_request['excluded_tables_uris']
    else:
        excluded_tables_uris = ''
    
    refresh_mode, refresh_frequency, refresh_unit = get_refresh_parameters(json_request)
    
    tag_history_option, _ = store.read_tag_history_settings()
    config_uuid = store.write_dynamic_column_config(tag_creator_sa, fields, included_columns_query, included_tables_uris, \
                                                    excluded_tables_uris, template_uuid, template_id, template_project, template_region, \
                                                    refresh_mode, refresh_frequency, refresh_unit, tag_history_option)                                                      


    return jsonify(config_uuid=config_uuid, config_type='DYNAMIC_TAG_COLUMN')

        
"""
Args:
    template_id: file metadata tag template id
    template_project: tag template's Google Cloud project 
    template_region: tag template's region 
    fields: list of all the template field names to include in the tag (no need to include the field type)
    included_assets_uris: The paths to the GCS resources 
    excluded_assets_uris: The paths to the GCS resources to exclude (optional)
    refresh_mode: AUTO or ON_DEMAND
    refresh_frequency: positive integer
    refresh_unit: minutes or hours
Returns:
    config_uuid 
"""
@app.route("/create_entry_config", methods=['POST'])
def create_entry_config():
    
    json_request = request.get_json(force=True) 
    print('json request: ', json_request)
    
    status, response, tag_creator_sa = do_authentication(request.headers, json_request, ENABLE_AUTH)
    
    if status == False:
        return jsonify(response), 400
        
    valid_parameters, template_id, template_project, template_region = check_template_parameters('entry_config', json_request)
    
    if valid_parameters != True:
        response = {
                "status": "error",
                "message": "Request JSON is missing some required tag template parameters",
        }
        return jsonify(response), 400
     
    #print('template_id: ' + template_id)
    #print('template_project: ' + template_project)
    #print('template_region: ' + template_region)
    
    template_uuid = store.write_tag_template(template_id, template_project, template_region)
    
    credentials, success = get_target_credentials(tag_creator_sa)
    
    if success == False:
        print('Error acquiring credentials from', tag_creator_sa)
        
    dcc = controller.DataCatalogController(credentials, None, None, template_id, template_project, template_region)
    fields = dcc.get_template(included_fields=json_request['fields'])

    if 'included_assets_uris' in json_request:
        included_assets_uris = json_request['included_assets_uris']
    else:
        print("The entry request requires an included_assets_uris parameter.")
        resp = jsonify(success=False)
        return resp

    if 'excluded_assets_uris' in json_request:
        excluded_assets_uris = json_request['excluded_assets_uris']
    else:
        excluded_assets_uris = ''
    
    refresh_mode, refresh_frequency, refresh_unit = get_refresh_parameters(json_request)
    
    tag_history_option, _ = store.read_tag_history_settings()
    
    config_uuid = store.write_entry_config(tag_creator_sa, fields, included_assets_uris, excluded_assets_uris,\
                                            template_uuid, template_id, template_project, template_region, \
                                            refresh_mode, refresh_frequency, refresh_unit, tag_history_option)                                                      

    return jsonify(config_uuid=config_uuid, config_type='ENTRY_CREATE')


"""
Args:
    template_id: enterprise dictionary tag template id
    template_project: tag template's Google Cloud project 
    template_region: tag template's region 
    fields: list of all the template field names to include in the tag (no need to include the field type)
    mapping_table: The path to the mapping table in BQ. This is required. 
    included_assets_uris: The path(s) to the resources in BQ or GCS 
    excluded_assets_uris: The path(s) to the resources to exclude (optional)
    refresh_mode: AUTO or ON_DEMAND
    refresh_frequency: positive integer
    refresh_unit: minutes or hours
Returns:
    config_uuid 
"""
@app.route("/create_glossary_asset_config", methods=['POST'])
def create_glossary_asset_config():
    
    json_request = request.get_json(force=True) 
    print('json request: ', json_request)
    
    status, response, tag_creator_sa = do_authentication(request.headers, json_request, ENABLE_AUTH)
    
    if status == False:
        return jsonify(response), 400
       
    valid_parameters, template_id, template_project, template_region = check_template_parameters('glossary_asset_config', json_request)
    
    if valid_parameters != True:
        response = {
                "status": "error",
                "message": "Request JSON is missing some required tag template parameters",
        }
        return jsonify(response), 400
     
    template_uuid = store.write_tag_template(template_id, template_project, template_region)
    
    credentials, success = get_target_credentials(tag_creator_sa)
    
    if success == False:
        print('Error acquiring credentials from', tag_creator_sa)
        
    dcc = controller.DataCatalogController(credentials, None, None, template_id, template_project, template_region)
    fields = dcc.get_template(included_fields=json_request['fields'])
    
    # validate mapping_table field
    if 'mapping_table' in json_request:
        mapping_table = json_request['mapping_table']
    else:
        print("glossary_asset_configs request doesn't include a mapping_table field. This is a required parameter.")
        resp = jsonify(success=False)
        return resp
    
    if 'included_assets_uris' in json_request:
        included_assets_uris = json_request['included_assets_uris']
    else:
        print("The glossary_asset_config request requires an included_assets_uris parameter.")
        resp = jsonify(success=False)
        return resp
    
    if 'excluded_assets_uris' in json_request:
        excluded_assets_uris = json_request['excluded_assets_uris']
    else:
        excluded_assets_uris = ''
    
    refresh_mode, refresh_frequency, refresh_unit = get_refresh_parameters(json_request)
    
    if 'overwrite' in json_request:  
        overwrite = json_request['overwrite']
    else:
        overwrite = True
     
    tag_history_option, _ = store.read_tag_history_settings()
        
    config_uuid = store.write_glossary_asset_config(tag_creator_sa, fields, mapping_table, included_assets_uris, \
                                                    excluded_assets_uris, template_uuid, template_id, template_project, template_region, \
                                                    refresh_mode, refresh_frequency, refresh_unit, tag_history_option, overwrite)                                                      
    
    return jsonify(config_uuid=config_uuid, config_type='GLOSSARY_TAG_ASSET')


"""
Args:
    template_id: data attribute tag template id
    template_project: tag template's Google Cloud project 
    template_region: tag template's region 
    fields: list of aincluded_tables_urisll the template field names to include in the tag (no need to include the field type)
    dlp_dataset: The path to the dataset in BQ in which the DLP findings tables are stored
    infotype_selection_table: The path to the infotype selection table in BQ. This is required. 
    infotype_classification_table: The path to the infotype classification table in BQ. This is required. 
    included_tables_uris: The path(s) to the BQ tables to be tagged 
    excluded_tables_uris: The path(s) to the BQ tables to exclude from the tagging (optional)
    create_policy_tags: true if this request should also create the policy tags on the sensitive columns, false otherwise
    taxonomy_id: The fully-qualified path to the policy tag taxonomy (projects/[PROJECT]/locations/[REGION]/taxonomies/[TAXONOMY_ID])
    refresh_mode: AUTO or ON_DEMAND
    refresh_frequency: positive integer
    refresh_unit: minutes or hours
Returns:
    config_uuid 
"""
@app.route("/create_sensitive_column_config", methods=['POST'])
def create_sensitive_column_config():
    
    json_request = request.get_json(force=True) 
    print('json request: ', json_request)
    
    status, response, tag_creator_sa = do_authentication(request.headers, json_request, ENABLE_AUTH)
    
    if status == False:
        return jsonify(response), 400
    
    valid_parameters, template_id, template_project, template_region = check_template_parameters('sensitive_column_config', json_request)
    
    if valid_parameters != True:
        response = {
                "status": "error",
                "message": "Request JSON is missing some required tag template parameters",
        }
        return jsonify(response), 400
     
    #print('template_id: ' + template_id)
    #print('template_project: ' + template_project)
    #print('template_region: ' + template_region)
    
    template_uuid = store.write_tag_template(template_id, template_project, template_region)
    
    credentials, success = get_target_credentials(tag_creator_sa)
    
    if success == False:
        print('Error acquiring credentials from', tag_creator_sa)
    
    dcc = controller.DataCatalogController(credentials, None, None, template_id, template_project, template_region)
    fields = dcc.get_template(included_fields=json_request['fields'])

    # validate dlp_dataset parameter
    if 'dlp_dataset' in json_request:
        dlp_dataset = json_request['dlp_dataset']
    else:
        print("The sensitive_column_config request doesn't include a dlp_dataset field. This is a required parameter.")
        resp = jsonify(success=False)
        return resp
            
    # validate infotype_selection_table parameter
    if 'infotype_selection_table' in json_request:
        infotype_selection_table = json_request['infotype_selection_table']
    else:
        print("The sensitive_column_config request doesn't include an infotype_selection_table field. This is a required parameter.")
        resp = jsonify(success=False)
        return resp
        
    # validate infotype_classification_table parameter
    if 'infotype_classification_table' in json_request:
        infotype_classification_table = json_request['infotype_classification_table']
    else:
        print("The sensitive_column_config request doesn't include an infotype_classification_table field. This is a required parameter.")
        resp = jsonify(success=False)
        return resp
    
    if 'included_tables_uris' in json_request:
        included_tables_uris = json_request['included_tables_uris']
    else:
        print("The sensitive_column_tags request requires an included_tables_uris parameter.")
        resp = jsonify(success=False)
        return resp
    
    if 'excluded_tables_uris' in json_request:
        excluded_tables_uris = json_request['excluded_tables_uris']
    else:
        excluded_tables_uris = ''
    
    # validate create_policy_tags parameter
    if 'create_policy_tags' in json_request:
        create_policy_tags = json_request['create_policy_tags']
    else:
        print("The sensitive_column_tags request requires a create_policy_tags field.")
        resp = jsonify(success=False)
        return resp
        
    if create_policy_tags:
        if 'taxonomy_id' in json_request:
            taxonomy_id = json_request['taxonomy_id']
        else:
            print("The sensitive_column_tags request requires a taxonomy_id when the create_policy_tags field is true. ")
            resp = jsonify(success=False)
            return resp
        
    refresh_mode, refresh_frequency, refresh_unit = get_refresh_parameters(json_request)
            
    tag_history_option, _ = store.read_tag_history_settings()
  
    if 'overwrite' in json_request:  
        overwrite = json_request['overwrite']
    else:
        overwrite = True
        
    config_uuid = store.write_sensitive_column_config(tag_creator_sa, fields, dlp_dataset, infotype_selection_table,\
                                                      infotype_classification_table, included_tables_uris, \
                                                      excluded_tables_uris, create_policy_tags, \
                                                      taxonomy_id, template_uuid, template_id, template_project, template_region, \
                                                      refresh_mode, refresh_frequency, refresh_unit, \
                                                      tag_history_option, overwrite)                                                      
    
    return jsonify(config_uuid=config_uuid, config_type='SENSITIVE_TAG_COLUMN')


"""
Args:
    source_template_id: The tag template id whose tags are to be restored
    source_template_project: The source tag template's project id 
    source_template_region: The source tag template's region 
    target_template_id: The tag template id whose tags are to be restored
    target_template_project: The source tag template's project id 
    target_template_region: The source tag template's region
    metadata_export_location: The path to the export files on GCS (Cloud Storage)
Returns:
    {config_type, config_uuid} 
"""
@app.route("/create_restore_config", methods=['POST'])
def create_restore_config():
    
    json_request = request.get_json(force=True) 
    print('json request: ', json_request)
    
    status, response, tag_creator_sa = do_authentication(request.headers, json_request, ENABLE_AUTH)
    
    if status == False:
        return jsonify(response), 400
    
    if 'source_template_id' in json_request:
        source_template_id = json_request['source_template_id']
    else:
        print("The restore_tags request requires a source_template_id parameter.")
        resp = jsonify(success=False)
        return resp

    if 'source_template_project' in json_request:
        source_template_project = json_request['source_template_project']
    else:
        print("The restore_tags request requires a source_template_project parameter.")
        resp = jsonify(success=False)
        return resp
    
    if 'source_template_region' in json_request:
        source_template_region = json_request['source_template_region']
    else:
        print("The restore_tags request requires a source_template_region parameter.")
        resp = jsonify(success=False)
        return resp
       
    if 'target_template_id' in json_request:
        target_template_id = json_request['target_template_id']
    else:
        print("The restore_tags request requires a target_template_id parameter.")
        resp = jsonify(success=False)
        return resp

    if 'target_template_project' in json_request:
        target_template_project = json_request['target_template_project']
    else:
        print("The restore_tags request requires a target_template_project parameter.")
        resp = jsonify(success=False)
        return resp
    
    if 'target_template_region' in json_request:
        target_template_region = json_request['target_template_region']
    else:
        print("The restore_tags request requires a target_template_region parameter.")
        resp = jsonify(success=False)
        return resp

    if 'metadata_export_location' in json_request:
        metadata_export_location = json_request['metadata_export_location']
    else:
        print("The restore_tags request requires the metadata_export_location parameter.")
        resp = jsonify(success=False)
        return resp

    source_template_uuid = store.write_tag_template(source_template_id, source_template_project, source_template_region)
    target_template_uuid = store.write_tag_template(target_template_id, target_template_project, target_template_region)
    
    tag_history_option, _ = store.read_tag_history_settings()

    if 'overwrite' in json_request:  
        overwrite = json_request['overwrite']
    else:
        overwrite = True
        
    config_uuid = store.write_tag_restore_config(tag_creator_sa, source_template_uuid, source_template_id, \
                                                source_template_project, source_template_region, \
                                                target_template_uuid, target_template_id, \
                                                target_template_project, target_template_region, \
                                                metadata_export_location, tag_history_option, overwrite)                                                      
    
    return jsonify(config_uuid=config_uuid, config_type='TAG_RESTORE')


"""
Args:
    template_id: The tag template id with which to create the tags
    template_project: The tag template's project id 
    template_region: The tag template's region 
    metadata_import_location: The path to the import files on GCS
    service_account: The email address of the Tag Creator SA (optional param)
    overwrite: Whether to overwrite the existing tags, True or False, defaults to True (optional param)
Returns:
    {config_type, config_uuid} 
"""
@app.route("/create_import_config", methods=['POST'])
def create_import_config():
    
    json_request = request.get_json(force=True) 
    print('json request: ', json_request)
    
    status, response, tag_creator_sa = do_authentication(request.headers, json_request, ENABLE_AUTH)
    
    if status == False:
        return jsonify(response), 400
       
    valid_parameters, template_id, template_project, template_region = check_template_parameters('import_config', json_request)
    
    if valid_parameters != True:
        response = {
                "status": "error",
                "message": "Request JSON is missing some required tag template parameters",
        }
        return jsonify(response), 400

    template_uuid = store.write_tag_template(template_id, template_project, template_region)

    if 'metadata_import_location' in json_request:
        metadata_import_location = json_request['metadata_import_location']
    else:
        print("import config type requires the metadata_import_location parameter. Please add this parameter to the json object.")
        resp = jsonify(success=False)
        return resp
              
    if 'overwrite' in json_request:  
        overwrite = json_request['overwrite']
    else:
        overwrite = True
        
    tag_history_option, _ = store.read_tag_history_settings()

    config_uuid = store.write_tag_import_config(tag_creator_sa, template_uuid, template_id, template_project, template_region, \
                                                metadata_import_location, tag_history_option, overwrite)                                                      
    
    return jsonify(config_uuid=config_uuid, config_type='TAG_IMPORT')


@app.route("/create_export_config", methods=['POST'])
def create_export_config():
    
    json_request = request.get_json(force=True) 
    print('json request: ', json_request)
    
    status, response, tag_creator_sa = do_authentication(request.headers, json_request, ENABLE_AUTH)
        
    if status == False:
        return jsonify(response), 400
       
    if 'source_projects' in json_request:
        source_projects = json_request['source_projects']
    else:
        source_projects = ''
    
    if 'source_folder' in json_request:
        source_folder = json_request['source_folder']
    else:
        source_folder = ''
    
    if source_projects == '' and source_folder == '':
        print("The export config requires either a source_projects or source_folder parameter. Please add the parameter to the json object.")
        resp = jsonify(success=False)
        return resp
    
    if 'source_region' in json_request:
        source_region = json_request['source_region']
    else:
        print("The export config requires either a source_region parameter. Please add the parameter to the json object.")
        resp = jsonify(success=False)
        return resp
              
    if 'target_project' in json_request:
        target_project = json_request['target_project']
    else:
        print("The export config requires a target_project parameter. Please add the parameter to the json object.")
        resp = jsonify(success=False)
        return resp

    if 'target_dataset' in json_request:
        target_dataset = json_request['target_dataset']
    else:
        print("The export config requires a target_dataset parameter. Please add the parameter to the json object.")
        resp = jsonify(success=False)
        return resp
    
    if 'target_region' in json_request:
        target_region = json_request['target_region']
    else:
        print("The export config requires a target_region parameter. Please add the parameter to the json object.")
        resp = jsonify(success=False)
        return resp
      
    if 'refresh_mode' in json_request:
        refresh_mode = json_request['refresh_mode']
    else:
        print("The export config requires a refresh_mode parameter. Please add the parameter to the json object.")
        resp = jsonify(success=False)
        return resp  
    
    if refresh_mode.upper() == 'AUTO':
        
        if 'refresh_frequency' in json_request:
            refresh_frequency = json_request['refresh_frequency']
        else:
            print("The export config requires a refresh_frequency parameter when refresh_mode = AUTO. Please add the parameter to the json object.")
            resp = jsonify(success=False)
            return resp
        
        if 'refresh_unit' in json_request:
            refresh_unit = json_request['refresh_unit']
        else:
            print("The export config requires a refresh_unit parameter when refresh_mode = AUTO. Please add the parameter to the json object.")
            resp = jsonify(success=False)
            return resp
    else:
        refresh_frequency = None
        refresh_unit = None 
    
    
    if 'write_option' in json_request:
        write_option = json_request['write_option']
    else:
        print("The export config requires a write_option parameter. Please add the parameter to the json object.")
        resp = jsonify(success=False)
        return resp
        
    config_uuid = store.write_tag_export_config(tag_creator_sa, source_projects, source_folder, source_region, \
                                              target_project, target_dataset, target_region, write_option, \
                                              refresh_mode, refresh_frequency, refresh_unit)                                                      
    
    return jsonify(config_uuid=config_uuid, config_type='TAG_EXPORT')


@app.route("/copy_tags", methods=['POST'])
def copy_tags():
    
    json_request = request.get_json(force=True) 
    print('json request: ', json_request)
    
    status, response, tag_creator_sa = do_authentication(request.headers, json_request, ENABLE_AUTH)
    
    if status == False:
        return jsonify(response), 400
       
    if 'source_project' in json_request:
        source_project = json_request['source_project']
    else:
        response = {
                "status": "error",
                "message": "Request JSON is missing a source_project parameter",
        }
        return jsonify(response), 400
    
    if 'source_dataset' in json_request:
        source_dataset = json_request['source_dataset']
    else:
        response = {
                "status": "error",
                "message": "Request JSON is missing a source_dataset parameter",
        }
        return jsonify(response), 400
    
    if 'source_table' in json_request:
         source_table = json_request['source_table']
    else:
         response = {
             "status": "error",
             "message": "Request JSON is missing a source_table parameter",
     }
         return jsonify(response), 400
 
    if 'target_project' in json_request:
        target_project = json_request['target_project']
    else:
        response = {
                "status": "error",
                "message": "Request JSON is missing a target_project parameter",
        }
        return jsonify(response), 400
    
    if 'target_dataset' in json_request:
        target_dataset = json_request['target_dataset']
    else:
        response = {
                "status": "error",
                "message": "Request JSON is missing a target_dataset parameter",
        }
        return jsonify(response), 400
    
    if 'target_table' in json_request:
         target_table = json_request['target_table']
    else:
         response = {
             "status": "error",
             "message": "Request JSON is missing a target_table parameter",
     }
         return jsonify(response), 400

    credentials, success = get_target_credentials(tag_creator_sa)
    
    if success == False:
        print('Error acquiring credentials from', tag_creator_sa)
    
    dcc = controller.DataCatalogController(credentials)
    success = dcc.copy_tags(source_project, source_dataset, source_table, target_project, target_dataset, target_table)                                                      
    
    if success:
        response = {"status": "success"}
    else:
        response = {"status": "failure"}
    
    return jsonify(response)


@app.route("/update_tag_subset", methods=['POST'])
def update_tag_subset():
    
    json_request = request.get_json(force=True) 
    print('json request: ', json_request)
    
    status, response, tag_creator_sa = do_authentication(request.headers, json_request, ENABLE_AUTH)
    
    if status == False:
        return jsonify(response), 400
    
    valid_parameters, template_id, template_project, template_region = check_template_parameters('update_tag_subset', json_request)
    
    if valid_parameters != True:
        response = {
                "status": "error",
                "message": "Request JSON is missing some required tag template parameters",
        }
        return jsonify(response), 400   
        
    if 'entry_name' in json_request:
        entry_name = json_request['entry_name']
    else:
        response = {
                "status": "error",
                "message": "Request JSON is missing a entry_name parameter",
        }
        return jsonify(response), 400
    
    if 'changed_fields' in json_request:
         changed_fields = json_request['changed_fields']
    else:
         response = {
             "status": "error",
             "message": "Request JSON is missing a changed_fields parameter",
     }
         return jsonify(response), 400

    credentials, success = get_target_credentials(tag_creator_sa)
    
    if success == False:
        print('Error acquiring credentials from', tag_creator_sa)
        
    dcc = controller.DataCatalogController(credentials, None, None, template_id, template_project, template_region)
    success = dcc.update_tag_subset(template_id, template_project, template_region, entry_name, changed_fields)

    if success:
        response = {"status": "success"}
    else:
        response = {"status": "failure"}
    
    return jsonify(response)

    
"""
Args:
    config_type = on of (STATIC_TAG_ASSET, DYNAMIC_TAG_TABLE, DYNAMIC_TAG_COLUMN, etc.) 
    config_id = config identifier
    job_metadata = json object containing metadata about the workflow. This parameter is optional. 
Returns:
    True if request succeeded, False otherwise
""" 
@app.route("/trigger_job", methods=['POST'])
def trigger_job(): 
    
    print('enter trigger_job')
    
    json_request = request.get_json(force=True) 
    print('json request: ', json_request)
    
    status, response, tag_creator_sa = do_authentication(request.headers, json_request, ENABLE_AUTH)
    print('status:', status, ', response:', response, ', tag_creator_sa:', tag_creator_sa)
    
    if status == False:
        return jsonify(response), 400
    
    tag_invoker_sa = get_tag_invoker_account(request.headers.get('Authorization'))
    print('tag_invoker_sa:', tag_invoker_sa)
    
    if 'config_type' in json_request:
        config_type = json_request['config_type']
        is_valid = check_config_type(json_request['config_type'])
    else:
        print("trigger_job request is missing the required parameter config_type. Please add this parameter to the json object.")
        resp = jsonify(success=False)
        return resp
        
    if is_valid == False:
        print("Invalid config_type parameter. Please choose a config_type from this list: " + get_available_config_types())
        resp = jsonify(success=False)
        return resp
    
    if 'config_uuid' in json_request:
        
        config_uuid = json_request['config_uuid']
        config_type = json_request['config_type']
        
        if isinstance(config_uuid, str):
            is_active = store.check_active_config(config_uuid, config_type)
            
            if is_active != True:
                print('Error: The config_uuid', config_uuid, 'is not active and cannot be used to run a job.')
                resp = jsonify(success=False)
                return resp
    
    elif 'template_id' in json_request and 'template_project' in json_request and 'template_region' in json_request:
         template_id = json_request['template_id']
         template_project = json_request['template_project']
         template_region = json_request['template_region']
         
         if 'included_tables_uris' in json_request:
             included_uris = json_request['included_tables_uris']
         elif 'included_assets_uris' in json_request:
             included_uris = json_request['included_assets_uris'] 
         else:
             print("trigger_job request is missing the required parameter included_tables_uris or included_assets_uris. Please add this parameter to the json object.")
             resp = jsonify(success=False)
             return resp 
             
         success, config_uuid = store.lookup_config_by_uris(template_id, template_project, template_region, config_type, included_uris)
         
         if success != True or config_uuid == '':
             print('Error: could not locate the config based on the parameters provided in json request.')
             resp = jsonify(success=False)
             return resp

    else:
        print("trigger_job request is missing the required parameters. Please add the config_uuid or the template_id, template_project, template_region, and included_uris to the json object.")
        resp = jsonify(success=False)
        return resp
        
    if 'job_metadata' in json_request:
        
        job_metadata = json_request['job_metadata']
        
        if isinstance(job_metadata, dict) == False:
            print('Warning: job metadata cannot be recorded because', job_metadata, 'is not a json object') 
        
        else: 
            if ENABLE_JOB_METADATA == False:
                print('Warning: Ignoring job metadata in request because ENABLE_JOB_METADATA = False in tagengine.ini')
                job_uuid = jm.create_job(tag_creator_sa, tag_invoker_sa, config_uuid, json_request['config_type'])
            else:
                job_uuid = jm.create_job(tag_creator_sa, tag_invoker_sa, config_uuid, config_type, job_metadata)
                template_id = store.lookup_tag_template(config_type, config_uuid)
                
                credentials, success = get_target_credentials(tag_creator_sa)
                bqu = bq.BigQueryUtils(credentials, BIGQUERY_REGION)
                success = bqu.write_job_metadata(job_uuid, template_id, job_metadata)
                print('Wrote job metadata to BigQuery for job', job_uuid, '. Success =', success)
                       
    else:    
        job_uuid = jm.create_job(tag_creator_sa, tag_invoker_sa, config_uuid, config_type)
    
    
    return jsonify(job_uuid=job_uuid)

   
"""
Args:
    job_uuid = unique identifer for job
Returns:
    job_status = one of (PENDING, RUNNING, COMPLETED, ERROR)
    task_count = number of tasks associates with this jobs
    tasks_ran = number of tasks that have run
    tasks_completed = number of tasks which have completed
    tasks_failed = number of tasks which have failed
""" 
@app.route("/get_job_status", methods=['POST'])
def get_job_status(): 
    
    json_request = request.get_json(force=True) 
    print('json request: ', json_request)
    
    status, response, tag_creator_sa = do_authentication(request.headers, json_request, ENABLE_AUTH)
    
    if status == False:
        return jsonify(response), 400
    
    if 'job_uuid' in json_request:
        job_uuid = json_request['job_uuid']
    else:
        print("get_job_status request is missing the required parameter job_uuid. Please add this parameter to the json object.")
        resp = jsonify(success=False)
        return resp
        
    job = jm.get_job_status(job_uuid)
    print('job: ', job)
    
    if job is None:
        return jsonify(success=False, message="job_uuid " + job_uuid + " cannot be found.")
        
    elif job['job_status'] == 'COMPLETED':
        return jsonify(success=True, job_status=job['job_status'], task_count=job['task_count'], tasks_ran=job['tasks_ran'],\
                       tasks_completed=job['tasks_success'], tasks_failed=job['tasks_failed'])
    else:
        return jsonify(job_status=job['job_status'], task_count=job['task_count'], tasks_ran=job['tasks_ran'],\
                       tasks_completed=job['tasks_success'], tasks_failed=job['tasks_failed'])
    

"""
Method called by Cloud Scheduler to update all tags which are set to auto refresh (regardless of the service account attached to the config)
Args:
    None
Returns:
    True if the request succeeded, False otherwise
""" 
@app.route("/scheduled_auto_updates", methods=['POST'])
def scheduled_auto_updates():
    
    try:    
        print('*** enter scheduled_auto_updates ***')
        
        json_request = request.get_json(force=True) 
        print('json request: ', json_request)
    
        status, response, tag_creator_sa = do_authentication(request.headers, json_request, ENABLE_AUTH)
        
        if status == False:
            return jsonify(response), 400
        
        jobs = []
        
        ready_configs = store.read_ready_configs()
        
        print('ready_configs:', ready_configs)
        
        for config_uuid, config_type in ready_configs:
        
            print('ready config:', config_uuid, ',', config_type)
            
            if isinstance(config_uuid, str): 
                store.update_job_status(config_uuid, config_type, 'PENDING')
                store.increment_version_next_run(tag_creator_sa, config_uuid, config_type)
                job_uuid = jm.create_job(tag_creator_sa, config_uuid, config_type)
                jobs.append(job_uuid)

        print('created jobs:', jobs)
        resp = jsonify(success=True, job_ids=json.dumps(jobs))
    
    except Exception as e:
        msg = 'failed scheduled_auto_updates'
        log_error(msg, e)
        resp = jsonify(success=False, message='failed scheduled_auto_updates ' + str(e))
    
    return resp


"""
Method called to list the configs
Args:
    config_type = one of (ALL, STATIC_TAG_ASSET, DYNAMIC_TAG_TABLE, DYNAMIC_TAG_COLUMN, SENSITIVE_TAG_COLUMN, etc.)
    service_account (Optional) = the service account attached to the config
Returns:
    True if the request succeeded, False otherwise
""" 
@app.route("/list_configs", methods=['POST'])
def list_configs():
    
    json_request = request.get_json(force=True) 
    print('json request: ', json_request)
    
    status, response, tag_creator_sa = do_authentication(request.headers, json_request, ENABLE_AUTH)
        
    if status == False:
        return jsonify(response), 400
       
    if 'config_type' in json_request:
        config_type = json_request['config_type']
        
        if config_type == 'ALL':
            is_valid = True
        else:
            is_valid = check_config_type(config_type)
    else:
        print("list_configs request is missing the required parameter config_type. Please add this parameter to the json object.")
        resp = jsonify(success=False)
        return resp
        
    if is_valid == False:
        print("Invalid config_type parameter. Please choose a config_type from this list: " + get_available_config_types() + " or use ALL.")
        resp = jsonify(success=False)
        return resp
                      
    configs = store.read_configs(tag_creator_sa, config_type) 
    
    configs_trimmed = []
    
    for config in configs:
        config_trimmed = {'config_uuid': config['config_uuid'], 'config_type': config['config_type'], \
                         'config_status': config['config_status'], 'creation_time': config['creation_time']}
        configs_trimmed.append(config_trimmed)
    
    return jsonify(configs=configs_trimmed)

"""
Method called to get a specific config
Args:
    service_account (Optional) = the service account attached to the config. Defaults to TAG_CREATOR_ACCOUNT. 
    config_type = one of (ALL, STATIC_TAG_ASSET, DYNAMIC_TAG_TABLE, DYNAMIC_TAG_COLUMN, SENSITIVE_TAG_COLUMN, etc.)
    config_uuid = the unique identifier of the config 
Returns:
    True if the request succeeded, False otherwise
""" 
@app.route("/get_config", methods=['POST'])
def get_config():
    
    json_request = request.get_json(force=True) 
    print('json request: ', json_request)
    
    status, response, tag_creator_sa = do_authentication(request.headers, json_request, ENABLE_AUTH)
        
    if status == False:
        return jsonify(response), 400
       
    if 'config_type' in json_request:
        config_type = json_request['config_type']
        is_valid = check_config_type(config_type)
    else:
        print("read_config request is missing the required parameter config_type. Please add this parameter to the json object.")
        resp = jsonify(success=False)
        return resp
        
    if is_valid == False:
        print("Invalid config_type parameter. Please choose a config_type from this list: " + get_available_config_types() + " or use ALL.")
        resp = jsonify(success=False)
        return resp
    
    if 'config_uuid' in json_request:
        config_uuid = json_request['config_uuid']
    else:
        print("read_config request is missing the required parameter config_uuid. Please add this parameter to the json object.")
        resp = jsonify(success=False)
        return resp
                     
    config = store.read_config(tag_creator_sa, config_uuid, config_type) 
    
    return jsonify(configs=config)

"""
Method called to delete a specific config
Args:
    service_account (Optional) = the service account attached to the config. Defaults to TAG_CREATOR_ACCOUNT. 
    config_type = one of (ALL, STATIC_TAG_ASSET, DYNAMIC_TAG_TABLE, DYNAMIC_TAG_COLUMN, SENSITIVE_TAG_COLUMN, etc.)
    config_uuid = the unique identifier of the config 
Returns:
    True if the request succeeded, False otherwise
""" 
@app.route("/delete_config", methods=['POST'])
def delete_config():
    
    json_request = request.get_json(force=True) 
    print('json request: ', json_request)
    
    status, response, tag_creator_sa = do_authentication(request.headers, json_request, ENABLE_AUTH)
     
    if status == False:
        return jsonify(response), 400
       
    if 'config_type' in json_request:
        config_type = json_request['config_type']
        is_valid = check_config_type(config_type)
    else:
        print("delete_config request is missing the required parameter config_type. Please add this parameter to the json object.")
        resp = jsonify(success=False)
        return resp
        
    if is_valid == False:
        print("Invalid config_type parameter. Please choose a config_type from this list: " + get_available_config_types() + " or use ALL.")
        resp = jsonify(success=False)
        return resp
    
    if 'config_uuid' in json_request:
        config_uuid = json_request['config_uuid']
    else:
        print("delete_config request is missing the required parameter config_uuid. Please add this parameter to the json object.")
        resp = jsonify(success=False)
        return resp
                     
    status = store.delete_config(tag_creator_sa, config_uuid, config_type) 
    
    return jsonify(status=status)


"""
Method called to purge the inactive configs from Firestore
Args:
    service_account (Optional) = the service account attached to the config. Defaults to TAG_CREATOR_ACCOUNT. 
    config_type = one of (ALL, STATIC_TAG_ASSET, DYNAMIC_TAG_TABLE, DYNAMIC_TAG_COLUMN, SENSITIVE_TAG_COLUMN, etc.)
Returns:
    True if the request succeeded, False otherwise
""" 
@app.route("/purge_inactive_configs", methods=['POST'])
def purge_inactive_configs():
    
    json_request = request.get_json(force=True) 
    print('json request: ', json_request)
    
    status, response, tag_creator_sa = do_authentication(request.headers, json_request, ENABLE_AUTH)
        
    if status == False:
        return jsonify(response), 400
       
    if 'config_type' in json_request:
        config_type = json_request['config_type']
        
        if config_type == 'ALL':
            is_valid = True
        else:
            is_valid = check_config_type(config_type)
    else:
        print("purge_inactive_configs request is missing the required parameter config_type. Please add this parameter to the json object.")
        resp = jsonify(success=False)
        return resp
        
    if is_valid == False:
        print("Invalid config_type parameter. Please choose a config_type from this list: " + get_available_config_types() + " or use ALL.")
        resp = jsonify(success=False)
        return resp
                      
    deleted_count = store.purge_inactive_configs(tag_creator_sa, config_type) 
    
    return jsonify(deleted_count=deleted_count)


################ INTERNAL PROCESSING METHODS #################

@app.route("/_split_work", methods=['POST'])
def _split_work():
    
    print('*** enter _split_work ***')
    
    json_request = request.get_json(force=True)
    print('json_request: ', json_request)
    
    job_uuid = json_request['job_uuid']
    config_uuid = json_request['config_uuid']
    config_type = json_request['config_type']
    tag_creator_sa = json_request['tag_creator_account']
    tag_invoker_sa = json_request['tag_invoker_account']

    config = store.read_config(tag_creator_sa, config_uuid, config_type)
    
    print('config: ', config)
    
    if config == {}:
       resp = jsonify(success=False)
       return resp 
    
    # get the credentials for the SA that is associated with this config
    credentials, success = get_target_credentials(tag_creator_sa)
    
    if success == False:
        print('Error acquiring credentials from', tag_creator_sa)
        update_job_status(self, config_uuid, config_type, 'ERROR')
        resp = jsonify(success=False)
        return resp
       
    re = res.Resources(credentials) 
    
    # dynamic table and dynamic column and sensitive column configs
    if 'included_tables_uris' in config:
        uris = list(re.get_resources(config.get('included_tables_uris'), config.get('excluded_tables_uris', None)))
        
        print('inside _split_work() uris: ', uris)
        
        jm.record_num_tasks(job_uuid, len(uris))
        jm.update_job_running(job_uuid) 
        tm.create_config_uuid_tasks(tag_creator_sa, tag_invoker_sa, job_uuid, config_uuid, config_type, uris)
    
    # static asset config and glossary asset config    
    if 'included_assets_uris' in config:
        uris = list(re.get_resources(config.get('included_assets_uris'), config.get('excluded_assets_uris', None)))
        
        print('inside _split_work() uris: ', uris)
        
        jm.record_num_tasks(job_uuid, len(uris))
        jm.update_job_running(job_uuid) 
        tm.create_config_uuid_tasks(tag_creator_sa, tag_invoker_sa, job_uuid, config_uuid, config_type, uris)
    
    # export tag config
    if config_type == 'TAG_EXPORT':
        
        bqu = bq.BigQueryUtils(credentials, config['target_region'])
        
        # create report tables if they don't exist
        tables_created = bqu.create_report_tables(config['target_project'], config['target_dataset'])
        print('Info: created report tables:', tables_created)
        
        if tables_created == False and config['write_option'] == 'truncate':
            bqu.truncate_report_tables(config['target_project'], config['target_dataset'])
            print('Info: truncated report tables')

        if config['source_folder']:
            uris = re.get_resources_by_folder(config['source_folder'])
        else:
            uris = re.get_resources_by_project(config['source_projects'])
        
        print('Info: Number of uris:', uris)
        print('Info: uris:', uris)
        
        jm.record_num_tasks(job_uuid, len(uris))
        jm.update_job_running(job_uuid) 
        tm.create_config_uuid_tasks(tag_creator_sa, tag_invoker_sa, job_uuid, config_uuid, config_type, uris)
    
    # import or restore tag config
    if config_type == 'TAG_IMPORT' or config_type == 'TAG_RESTORE':
                    
        if config_type == 'TAG_IMPORT':
            
            try:
                csv_files = list(re.get_resources(config.get('metadata_import_location'), None))
                print('csv_files: ', csv_files)
            except Exception as e:
                msg = 'Error: unable to read CSV from {}'.format(config.get('metadata_import_location'))
                log_error(msg, e, job_uuid)
                
                store.update_job_status(config_uuid, config_type, 'ERROR')
                jm.set_job_status(job_uuid, 'ERROR')
                resp = jsonify(success=False)
                return resp
            
            if len(csv_files) == 0:
                msg = 'Error: unable to read CSV from {}'.format(config.get('metadata_import_location'))
                error = {'job_uuid': job_uuid, 'msg': msg}
                print(json.dumps(error))
                
                store.update_job_status(config_uuid, config_type, 'ERROR')
                jm.set_job_status(job_uuid, 'ERROR')
                resp = jsonify(success=False)
                return resp
            
            extracted_tags = []
        
            for csv_file in csv_files:
                extracted_tags.extend(cp.CsvParser.extract_tags(credentials, csv_file))
                
            if len(extracted_tags) == 0:
                print('Error: unable to extract tags from CSV. Please verify the format of the CSV.') 
                store.update_job_status(config_uuid, config_type, 'ERROR')
                jm.set_job_status(job_uuid, 'ERROR')
                resp = jsonify(success=False)
                return resp
    
        if config_type == 'TAG_RESTORE':
            bkp_files = list(re.get_resources(config.get('metadata_export_location'), None))
        
            #print('bkp_files: ', bkp_files)
            extracted_tags = []
        
            for bkp_file in bkp_files:
                extracted_tags.append(bfp.BackupFileParser.extract_tags(credentials, \
                                                                        config.get('source_template_id'), \
                                                                        config.get('source_template_project'), \
                                                                        bkp_file))
             
        # no tags were extracted from the CSV files
        if extracted_tags == [[]]:
           resp = jsonify(success=False)
           return resp
        
        jm.record_num_tasks(job_uuid, len(extracted_tags))
        jm.update_job_running(job_uuid) 
        tm.create_tag_extract_tasks(tag_creator_sa, tag_invoker_sa, job_uuid, config_uuid, config_type, extracted_tags)
    

    # update the status of the config, no matter which config type is running
    store.update_job_status(config_uuid, config_type, 'RUNNING')
    jm.set_job_status(job_uuid, 'RUNNING')
    resp = jsonify(success=True)
    return resp
    

@app.route("/_run_task", methods=['POST'])
def _run_task():
    
    print('*** enter _run_task ***')
    
    creation_status = constants.ERROR
    
    json_request = request.get_json(force=True)
    job_uuid = json_request['job_uuid']
    config_uuid = json_request['config_uuid']
    config_type = json_request['config_type']
    shard_uuid = json_request['shard_uuid']
    task_uuid = json_request['task_uuid']
    tag_creator_sa = json_request['tag_creator_account']
    tag_invoker_sa = json_request['tag_invoker_account']
    
    if 'uri' in json_request:
        uri = json_request['uri']
    else:
        uri = None
        #print('uri: ', uri)
        
    if 'tag_extract' in json_request:
        tag_extract = json_request['tag_extract']
        #print('tag_extract: ', tag_extact)
    else:
        tag_extract = None
        
    
    credentials, success = get_target_credentials(tag_creator_sa)
    
    if success == False:
        print('Error acquiring credentials from', tag_creator_sa)
        tm.update_task_status(shard_uuid, task_uuid, 'ERROR')   
    
    # retrieve the config 
    tm.update_task_status(shard_uuid, task_uuid, 'RUNNING')
    
    config = store.read_config(tag_creator_sa, config_uuid, config_type)
    print('config: ', config)
           
    if config_type == 'TAG_EXPORT':
        dcc = controller.DataCatalogController(credentials)
    
    elif config_type == 'TAG_IMPORT':
        
        if 'template_id' not in config or 'template_project' not in config or 'template_region' not in config:
            response = {
                    "status": "error",
                    "message": "Request JSON is missing required template parameters",
            }
            return jsonify(response), 400
        
        dcc = controller.DataCatalogController(credentials, tag_creator_sa, tag_invoker_sa, \
                                               config['template_id'], config['template_project'], config['template_region'])
    
    elif config_type == 'TAG_RESTORE':
        
        if 'target_template_id' not in config or 'target_template_project' not in config or 'target_template_region' not in config:
            response = {
                    "status": "error",
                    "message": "Request JSON is missing some required target tag template parameters",
            }
            return jsonify(response), 400
        if 'source_template_id' not in config or 'source_template_project' not in config or 'source_template_region' not in config:
            response = {
                    "status": "error",
                    "message": "Request JSON is missing some required source tag template parameters",
            }
            return jsonify(response), 400
        
        dcc = controller.DataCatalogController(credentials, tag_creator_sa, tag_invoker_sa, \
                                                config['target_template_id'], config['target_template_project'], 
                                                config['target_template_region'])
    else:
        if 'template_uuid' not in config:
            response = {
                    "status": "error",
                    "message": "Request JSON is missing some required template_uuid parameter",
            }
            return jsonify(response), 400
            
        template_config = store.read_tag_template_config(config['template_uuid'])
        dcc = controller.DataCatalogController(credentials, tag_creator_sa, tag_invoker_sa, \
                                               template_config['template_id'], template_config['template_project'], \
                                               template_config['template_region'])
            
    
    if config_type == 'DYNAMIC_TAG_TABLE':
        creation_status = dcc.apply_dynamic_table_config(config['fields'], uri, job_uuid, config_uuid, \
                                                         config['template_uuid'], config['tag_history'])                                               
    if config_type == 'DYNAMIC_TAG_COLUMN':
        creation_status = dcc.apply_dynamic_column_config(config['fields'], config['included_columns_query'], uri, job_uuid, config_uuid, \
                                                          config['template_uuid'], config['tag_history'])
    if config_type == 'STATIC_TAG_ASSET':
        creation_status = dcc.apply_static_asset_config(config['fields'], uri, job_uuid, config_uuid, \
                                                        config['template_uuid'], config['tag_history'], \
                                                        config['overwrite'])                                                   
    if config_type == 'ENTRY_CREATE':
        creation_status = dcc.apply_entry_config(config['fields'], uri, job_uuid, config_uuid, \
                                                 config['template_uuid'], config['tag_history']) 
    if config_type == 'GLOSSARY_TAG_ASSET':
        creation_status = dcc.apply_glossary_asset_config(config['fields'], config['mapping_table'], uri, job_uuid, config_uuid, \
                                                    config['template_uuid'], config['tag_history'], config['overwrite'])
    if config_type == 'SENSITIVE_TAG_COLUMN':
        creation_status = dcc.apply_sensitive_column_config(config['fields'], config['dlp_dataset'], config['infotype_selection_table'], \
                                                            config['infotype_classification_table'], uri, config['create_policy_tags'], \
                                                            config['taxonomy_id'], job_uuid, config_uuid, \
                                                            config['template_uuid'], config['tag_history'], \
                                                            config['overwrite'])
    if config_type == 'TAG_EXPORT':
        creation_status = dcc.apply_export_config(config['config_uuid'], config['target_project'], config['target_dataset'], config['target_region'], uri)
    
    if config_type == 'TAG_IMPORT':
        creation_status = dcc.apply_import_config(job_uuid, config_uuid, tag_extract, \
                                                  config['tag_history'], config['overwrite'])
    if config_type == 'TAG_RESTORE':
        creation_status = dcc.apply_restore_config(job_uuid, config_uuid, tag_extract, \
                                                   config['tag_history'], config['overwrite'])
                                              
    if creation_status == constants.SUCCESS:
        tm.update_task_status(shard_uuid, task_uuid, 'SUCCESS')
    else:
        tm.update_task_status(shard_uuid, task_uuid, 'ERROR')
    
    # fan-in
    tasks_success, tasks_failed, pct_complete = jm.calculate_job_completion(job_uuid)
    print('tasks_success:', tasks_success)
    print('tasks_failed:', tasks_failed)
    print('pct_complete:', pct_complete)
        
    if pct_complete == 100:
        if tasks_failed > 0:
            store.update_job_status(config_uuid, config_type, 'ERROR')
            jm.set_job_status(job_uuid, 'ERROR')
            store.update_scheduling_status(config_uuid, config_type, 'READY')
            resp = jsonify(success=True)
        else:
            store.update_job_status(config_uuid, config_type, 'SUCCESS')
            jm.set_job_status(job_uuid, 'SUCCESS')
            resp = jsonify(success=False)
    else:
        store.update_job_status(config_uuid, config_type, 'RUNNING: {}% complete'.format(pct_complete))
        jm.set_job_status(job_uuid, 'RUNNING: {}% complete'.format(pct_complete))
        resp = jsonify(success=True)
    
    return resp
#[END _run_task]

####################### VERSION METHOD ####################################  
    
@app.route("/version", methods=['GET'])
def version():
    return "Welcome to Tag Engine version 2.1.9\n"
    
####################### TEST METHOD ####################################  
    
@app.route("/ping", methods=['GET'])
def ping():
    return "Tag Engine is alive\n"
#[END ping]

@app.errorhandler(500)
def server_error(e):
    # Log the error and stacktrace.
    #logging.exception('An error occurred during a request.')
    return 'An internal error occurred: ' + str(e), 500

if __name__ == "__main__":
    #os.environ['OAUTHLIB_INSECURE_TRANSPORT'] = "1" # uncomment only when running locally
    os.environ['OAUTHLIB_RELAX_TOKEN_SCOPE'] = "1" # to allow for scope changes
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080))) # for running on Cloud Run
    #app.run(debug=True, port=5000) # for running locally 
    