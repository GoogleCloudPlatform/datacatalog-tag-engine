# Copyright 2020-2022 Google, LLC.
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

from flask import Flask, render_template, request, redirect, url_for, jsonify, json
import datetime, configparser
from google.cloud import firestore
import DataCatalogUtils as dc
import TagEngineUtils as te
import Resources as res
import BackupFileParser as bfp
import CsvParser as cp
import constants

import JobManager as jobm
import TaskManager as taskm
import BigQueryUtils as bq

from google.cloud import tasks_v2
from google.protobuf import timestamp_pb2
import datetime, time

config = configparser.ConfigParser()
config.read("tagengine.ini")

app = Flask(__name__)
teu = te.TagEngineUtils()

# handles create requests from API and on-demand update requests from API (i.e. config contains refresh_mode = ON_DEMAND) 
jm = jobm.JobManager(config['DEFAULT']['TAG_ENGINE_PROJECT'], config['DEFAULT']['QUEUE_REGION'], config['DEFAULT']['INJECTOR_QUEUE'], "/_split_work")
tm = taskm.TaskManager(config['DEFAULT']['TAG_ENGINE_PROJECT'], config['DEFAULT']['QUEUE_REGION'], config['DEFAULT']['WORK_QUEUE'], "/_run_task")

##################### UI METHODS #################

@app.route("/")
def homepage():
    
    exists, settings = teu.read_default_tag_template_settings()
    
    if exists:
        template_id = settings['template_id']
        template_project = settings['template_project']
        template_region = settings['template_region']
    else:
        template_id = "{your_template_id}"
        template_project = "{your_template_project}"
        template_region = "{your_template_region}"
    
    # [END homepage]
    # [START render_template]
    return render_template(
        'index.html',
        template_id=template_id,
        template_project=template_project,
        template_region=template_region)

    
@app.route("/default_tag_template_settings<int:saved>", methods=["GET"])
def default_tag_template_settings(saved):
    
    exists, settings = teu.read_default_tag_template_settings()
    
    if exists:
        template_id = settings['template_id']
        template_project = settings['template_project']
        template_region = settings['template_region']
    else:
        template_id = "{your_template_id}"
        template_project = "{your_template_project}"
        template_region = "{your_template_region}"
    
    # [END default_tag_template_settings]
    # [START render_template]
    return render_template(
        'default_tag_template_settings.html',
        template_id=template_id,
        template_project=template_project,
        template_region=template_region,
        settings=saved)
    # [END render_template]
         
@app.route("/coverage_report_settings<int:saved>")
def coverage_report_settings(saved):
    
    exists, settings = teu.read_coverage_report_settings()
    
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
    
@app.route("/tag_history_settings<int:saved>", methods=["GET"])
def tag_history_settings(saved):
    
    enabled, settings = teu.read_tag_history_settings()
    
    if enabled:
        enabled = settings['enabled']
        bigquery_project = settings['bigquery_project']
        bigquery_region = settings['bigquery_region']
        bigquery_dataset = settings['bigquery_dataset']
    else:
        bigquery_project = "{your_bigquery_project}"
        bigquery_region = "{your_bigquery_region}"
        bigquery_dataset = "{your_bigquery_dataset}"
    
    # [END tag_history_settings]
    # [START render_template]
    return render_template(
        'tag_history_settings.html',
        enabled=enabled,
        bigquery_project=bigquery_project,
        bigquery_region=bigquery_region,
        bigquery_dataset=bigquery_dataset,
        settings=saved)
    # [END render_template]

@app.route("/tag_stream_settings<int:saved>", methods=["GET"])
def tag_stream_settings(saved):
    
    enabled, settings = teu.read_tag_stream_settings()
    
    if enabled:
        enabled = settings['enabled']
        pubsub_project = settings['pubsub_project']
        pubsub_topic = settings['pubsub_topic']
    else:
        pubsub_project = "{your_pubsub_project}"
        pubsub_topic = "{your_pubsub_topic}"
    
    # [END tag_stream_settings]
    # [START render_template]
    return render_template(
        'tag_stream_settings.html',
        enabled=enabled,
        pubsub_project=pubsub_project,
        pubsub_topic=pubsub_topic,
        settings=saved)
    # [END render_template]
    

@app.route("/set_default_tag_template", methods=['POST'])
def set_default_tag_template():
    
    template_id = request.form['template_id'].rstrip()
    template_project = request.form['template_project'].rstrip()
    template_region = request.form['template_region'].rstrip()
    
    if template_id == "{your_template_id}":
        template_id = None
    if template_project == "{your_template_project}":
        template_project = None
    if template_region == "{your_template_region}":
        template_region = None
    
    if template_id != None or template_project != None or template_region != None:
        teu.write_default_tag_template_settings(template_id, template_project, template_region)
        
    return default_tag_template_settings(1)
        
        
@app.route("/set_tag_history", methods=['POST'])
def set_tag_history():
    
    enabled = request.form['enabled'].rstrip()
    bigquery_project = request.form['bigquery_project'].rstrip()
    bigquery_region = request.form['bigquery_region'].rstrip()
    bigquery_dataset = request.form['bigquery_dataset'].rstrip()
    
    print("enabled: " + enabled)
    print("bigquery_project: " + bigquery_project)
    print("bigquery_region: " + bigquery_region)
    print("bigquery_dataset: " + bigquery_dataset)
    
    if enabled == "on":
        enabled = True
    else:
        enabled = False    
    
    if bigquery_project == "{your_bigquery_project}":
        bigquery_project = None
    if bigquery_region == "{your_bigquery_region}":
        bigquery_region = None
    if bigquery_dataset == "{your_bigquery_dataset}":
        bigquery_dataset = None
    
    # can't be enabled if either of the required fields are NULL
    if enabled and (bigquery_project == None or bigquery_region == None or bigquery_dataset == None):
        enabled = False
    
    if bigquery_project != None or bigquery_region != None or bigquery_dataset != None:
        teu.write_tag_history_settings(enabled, bigquery_project, bigquery_region, bigquery_dataset)
        
        return tag_history_settings(1)
    else:
        return tag_history_settings(0)


@app.route("/set_tag_stream", methods=['POST'])
def set_tag_stream():
    
    enabled = request.form['enabled'].rstrip()
    pubsub_project = request.form['pubsub_project'].rstrip()
    pubsub_topic = request.form['pubsub_topic'].rstrip()

    print("enabled: " + enabled)
    print("pubsub_project: " + pubsub_project)
    print("pubsub_topic: " + pubsub_topic)
    
    if enabled == "on":
        enabled = True
    else:
        enabled = False    
    
    if pubsub_project == "{your_pubsub_project}":
        pubsub_project = None
    if pubsub_topic == "{your_pubsub_topic}":
        pubsub_topic = None
    
    # can't be enabled if either the required fields are NULL
    if enabled and (pubsub_project == None or pubsub_topic == None):
        enabled = False
    
    if pubsub_project != None or pubsub_topic != None:
        teu.write_tag_stream_settings(enabled, pubsub_project, pubsub_topic)
        
        return tag_stream_settings(1)
    else:
        return tag_stream_settings(0)
        
        
@app.route("/set_coverage_report", methods=['POST'])
def set_coverage_report():
    
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
        teu.write_coverage_report_settings(included_bigquery_projects, excluded_bigquery_datasets, excluded_bigquery_tables)
        
    return coverage_report_settings(1)  
     
@app.route("/coverage_report")
def coverage_report():
    
    summary_report, detailed_report = teu.generate_coverage_report()
    
    print('summary_report: ' + str(summary_report))
    print('detailed_report: ' + str(detailed_report))
    
    exists, settings = teu.read_coverage_report_settings()
    included_bigquery_projects = settings['included_bigquery_projects']
    
    return render_template(
        "coverage_report.html",
        included_bigquery_projects=included_bigquery_projects,
        report_headers=summary_report,
        report_data=detailed_report)

# TO DO: re-implement this method using the DC API        
@app.route("/coverage_details<string:res>", methods=['GET'])
def coverage_details(res):
    print("res: " + res)
    
    bigquery_project = res.split('.')[0]
    resource = res.split('.')[1]
    
    configs = teu.read_configs_on_res(res)
    
    return render_template(
        'view_tags_on_res.html',
        resource=res,
        bigquery_project=bigquery_project,
        configs=configs)
                
# [START search_tag_template]
@app.route('/search_tag_template', methods=['POST'])
def search_tag_template():

    template_id = request.form['template_id']
    template_project = request.form['template_project']
    template_region = request.form['template_region']
    
    dcu = dc.DataCatalogUtils(template_id, template_project, template_region)
    fields = dcu.get_template()
    
    #print("fields: " + str(fields))
    
    # [END search_tag_template]
    # [START render_template]
    return render_template(
        'tag_template.html',
        template_id=template_id,
        template_project=template_project,
        template_region=template_region,
        fields=fields)
    # [END render_template]
        
@app.route('/choose_action', methods=['GET'])
def choose_action():
    
    template_id = request.args.get('template_id')
    template_project = request.args.get('template_project')
    template_region = request.args.get('template_region')
    
    dcu = dc.DataCatalogUtils(template_id, template_project, template_region)
    fields = dcu.get_template()
    
    #print("fields: " + str(fields))
    
    # [END choose_action]
    # [START render_template]
    return render_template(
        'tag_template.html',
        template_id=template_id,
        template_project=template_project,
        template_region=template_region,
        fields=fields)
    # [END render_template]

# [START view_configs]
@app.route('/view_configs', methods=['GET'])
def view_configs():
    
    template_id = request.args.get('template_id')
    template_project = request.args.get('template_project')
    template_region = request.args.get('template_region')
    
    print("template_id: " + str(template_id))
    print("template_project: " + str(template_project))
    print("template_region: " + str(template_region))
    
    dcu = dc.DataCatalogUtils(template_id, template_project, template_region)
    template_fields = dcu.get_template()
    
    history_enabled, history_settings = teu.read_tag_history_settings()
    stream_enabled, stream_settings = teu.read_tag_stream_settings()
    
    configs = teu.read_configs(template_id, template_project, template_region)
    
    #print('configs: ', configs)
    
    return render_template(
        'view_configs.html',
        template_id=template_id,
        template_project=template_project,
        template_region=template_region,
        configs=configs)
    # [END render_template]

def view_remaining_configs(template_id, template_project, template_region):
    
    print("template_id: " + str(template_id))
    print("template_project: " + str(template_project))
    print("template_region: " + str(template_region))
    
    dcu = dc.DataCatalogUtils(template_id, template_project, template_region)
    template_fields = dcu.get_template()
    
    history_enabled, history_settings = teu.read_tag_history_settings()
    stream_enabled, stream_settings = teu.read_tag_stream_settings()
    
    configs = teu.read_configs(template_id, template_project, template_region)
    
    #print('configs: ', configs)
    
    return render_template(
        'view_configs.html',
        template_id=template_id,
        template_project=template_project,
        template_region=template_region,
        configs=configs)
    # [END render_template]
    

# [START view_export_configs]
@app.route('/view_export_configs', methods=['GET'])
def view_export_configs():
    
    configs = teu.read_export_configs()
    
    print('configs: ', configs)
    
    return render_template(
        'view_export_configs.html',
        configs=configs)
    # [END render_template]

# [START display_configuration]
@app.route('/display_configuration', methods=['POST'])
def display_configuration():
    template_id = request.form['template_id']
    template_project = request.form['template_project']
    template_region = request.form['template_region']
    action = request.form['action']

    print("template_id: " + str(template_id))
    print("template_project: " + str(template_project))
    print("template_region: " + str(template_region))
    print("action: " + str(action))
    
    dcu = dc.DataCatalogUtils(template_id, template_project, template_region)
    template_fields = dcu.get_template()
    
    history_enabled, history_settings = teu.read_tag_history_settings()
    stream_enabled, stream_settings = teu.read_tag_stream_settings()
    
    if action == "View and Edit Configurations":

        configs = teu.read_configs(template_id, template_project, template_region)
        
        print('configs: ', configs)
        
        return render_template(
            'view_configs.html',
            template_id=template_id,
            template_project=template_project,
            template_region=template_region,
            configs=configs)
        
    elif action == "Create Static Asset Tags":
        return render_template(
            'static_asset_config.html',
            template_id=template_id,
            template_project=template_project,
            template_region=template_region,
            fields=template_fields,
            current_time=datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
            display_tag_history=history_enabled,
            display_tag_stream=stream_enabled)
            
    elif action == "Create Dynamic Table Tags":
        return render_template(
            'dynamic_table_config.html',
            template_id=template_id,
            template_project=template_project,
            template_region=template_region,
            fields=template_fields,
            display_tag_history=history_enabled,
            display_tag_stream=stream_enabled)
            
    elif action == "Create Dynamic Column Tags":
        return render_template(
            'dynamic_column_config.html',
            template_id=template_id,
            template_project=template_project,
            template_region=template_region,
            fields=template_fields,
            display_tag_history=history_enabled,
            display_tag_stream=stream_enabled)
            
    elif action == "Create Data Catalog Entries":
        return render_template(
            'entry_config.html',
            template_id=template_id,
            template_project=template_project,
            template_region=template_region,
            fields=template_fields,
            display_tag_history=history_enabled,
            display_tag_stream=stream_enabled)
            
    elif action == "Create Glossary Asset Tags":
        return render_template(
            'glossary_asset_config.html',
            template_id=template_id,
            template_project=template_project,
            template_region=template_region,
            fields=template_fields,
            display_tag_history=history_enabled,
            display_tag_stream=stream_enabled)
    
    elif action == "Create Sensitive Column Tags":
        return render_template(
            'sensitive_column_config.html',
            template_id=template_id,
            template_project=template_project,
            template_region=template_region,
            fields=template_fields,
            display_tag_history=history_enabled,
            display_tag_stream=stream_enabled)
                        
    elif action == "Import Tags":
        return render_template(
            'import_config.html',
            template_id=template_id,
            template_project=template_project,
            template_region=template_region,
            fields=template_fields,
            display_tag_history=history_enabled,
            display_tag_stream=stream_enabled)
    
    elif action == "Restore Tags":
        return render_template(
            'restore_config.html',
            template_id=template_id,
            template_project=template_project,
            template_region=template_region,
            fields=template_fields,
            display_tag_history=history_enabled,
            display_tag_stream=stream_enabled)
            
    # [END render_template]

# [START display_export_option]
@app.route('/display_export_option', methods=['POST'])
def display_export_option():
    
    action = request.form['action']
    
    if action == "Create Export Config":
        return render_template(
            'export_config.html')
            
    elif action == "View and Edit Configs":
        return view_export_configs()


@app.route('/create_export_option', methods=['GET'])
def create_export_option():
    
    return render_template(
            'export_config.html')
            

@app.route('/update_config', methods=['POST'])
def update_config():
    
    print('enter update_config')
    
    template_id = request.form['template_id']
    template_project = request.form['template_project']
    template_region = request.form['template_region']
    config_uuid = request.form['config_uuid']
    config_type = request.form['config_type']
    action = request.form['action']
    
    print('template_id:', template_id)
    print('template_project:', template_project)
    print('template_region:', template_region)
    print('config_uuid:', config_uuid)
    print('config_type:', config_type)
    print('action:', action)
    
    if action == "Delete Config":
        teu.delete_config(config_uuid, config_type)
        return view_remaining_configs(template_id, template_project, template_region)
    
    config = teu.read_config(config_uuid, config_type)
    print("config: " + str(config))
    
    dcu = dc.DataCatalogUtils(template_id, template_project, template_region)
    template_fields = dcu.get_template()
    print('template_fields:', template_fields)
    
    enabled, settings = teu.read_tag_history_settings()
    
    if enabled:
        tag_history = 1
    else:
        tag_history = 0
    
    print("tag_history: " + str(tag_history))

    enabled, settings = teu.read_tag_stream_settings()
    
    if enabled:
        tag_stream = 1
    else:
        tag_stream = 0
    
    print("tag_stream: " + str(tag_stream))

    if config_type == "STATIC_ASSET_TAG":
        return render_template(
            'update_static_asset_config.html',
            template_id=template_id,
            template_project=template_project,
            template_region=template_region,
            fields=template_fields,
            config=config, 
            current_time=datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
            display_tag_history_option=tag_history,
            display_tag_stream_option=tag_stream)
    
    if config_type == "DYNAMIC_TABLE_TAG":
        return render_template(
            'update_dynamic_table_config.html',
            template_id=template_id,
            template_project=template_project,
            template_region=template_region,
            fields=template_fields,
            config=config,
            display_tag_history_option=tag_history,
            display_tag_stream_option=tag_stream)
            
    if config_type == "DYNAMIC_COLUMN_TAG":
        return render_template(
            'update_dynamic_column_config.html',
            template_id=template_id,
            template_project=template_project,
            template_region=template_region,
            fields=template_fields,
            config=config,
            display_tag_history_option=tag_history,
            display_tag_stream_option=tag_stream)
            
    if config_type == "ENTRY":
        return render_template(
            'update_entry_config.html',
            template_id=template_id,
            template_project=template_project,
            template_region=template_region,
            fields=template_fields,
            config=config,
            display_tag_history_option=tag_history,
            display_tag_stream_option=tag_stream)
            
    if config_type == "GLOSSARY_ASSET_TAG":
        return render_template(
            'update_glossary_asset_config.html',
            template_id=template_id,
            template_project=template_project,
            template_region=template_region,
            fields=template_fields,
            config=config,
            display_tag_history_option=tag_history,
            display_tag_stream_option=tag_stream)
            
    if config_type == "SENSITIVE_COLUMN_TAG":
        return render_template(
            'update_sensitive_column_config.html',
            template_id=template_id,
            template_project=template_project,
            template_region=template_region,
            fields=template_fields,
            config=config,
            display_tag_history_option=tag_history,
            display_tag_stream_option=tag_stream)
    
    if config_type == "IMPORT_TAG":
        return render_template(
            'update_import_config.html',
            template_id=template_id,
            template_project=template_project,
            template_region=template_region,
            config=config,
            display_tag_history_option=tag_history,
            display_tag_stream_option=tag_stream)
            
    if config_type == "RESTORE_TAG":
        return render_template(
            'update_restore_config.html',
            template_id=template_id,
            template_project=template_project,
            template_region=template_region,
            config=config,
            display_tag_history_option=tag_history,
            display_tag_stream_option=tag_stream)
    # [END render_template]
    
 
@app.route('/update_export_config', methods=['POST'])
def update_export_config():
    
    config_uuid = request.form['config_uuid']
    action = request.form['action']
    
    print('config_uuid: ', config_uuid)
    
    if action == "Delete Config":
        teu.delete_config(config_uuid, 'EXPORT_TAG')
        return view_export_configs()
    
    config = teu.read_config(config_uuid, 'EXPORT_TAG', reformat=True)
    print("config: " + str(config))
    
    return render_template(
        'update_export_config.html',
        config=config)
    
    
@app.route('/process_static_asset_config', methods=['POST'])
def process_static_asset_config():
    template_id = request.form['template_id']
    template_project = request.form['template_project']
    template_region = request.form['template_region']
    included_assets_uris = request.form['included_assets_uris'].rstrip()
    excluded_assets_uris = request.form['excluded_assets_uris'].rstrip()
    refresh_mode = request.form['refresh_mode']
    refresh_frequency = request.form['refresh_frequency'].rstrip()
    refresh_unit = request.form['refresh_unit']
    action = request.form['action']
    
    print('included_assets_uris: ' + included_assets_uris)
    print('excluded_assets_uris: ' + excluded_assets_uris)
    
    dcu = dc.DataCatalogUtils(template_id, template_project, template_region)
    template = dcu.get_template()
    
    if action == "Cancel Changes":
        
        return render_template(
            'tag_template.html',
            template_id=template_id,
            template_project=template_project,
            template_region=template_region, 
            fields=template)
    
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
    
    tag_history_option = False
    tag_history_enabled = "OFF"
    
    if "tag_history" in request.form:
        tag_history = request.form.get("tag_history")
    
        if tag_history == "selected":
            tag_history_option = True
            tag_history_enabled = "ON"
            
    tag_stream_option = False
    tag_stream_enabled = "OFF"
    
    if "tag_stream" in request.form:
        tag_stream = request.form.get("tag_stream")
    
        if tag_stream == "selected":
            tag_stream_option = True
            tag_stream_enabled = "ON"            
        
    template_uuid = teu.write_tag_template(template_id, template_project, template_region)
    config_uuid, included_tables_uris_hash = teu.write_static_asset_config('PENDING', fields, included_assets_uris, excluded_assets_uris, template_uuid, refresh_mode, \
                                                                    refresh_frequency, refresh_unit, tag_history_option, tag_stream_option)
    
    if isinstance(config_uuid, str):
        job_uuid = jm.create_job(config_uuid, 'STATIC_ASSET_TAG')
    
    if job_uuid != None: 
        job_creation = constants.SUCCESS
    else:
        job_creation = constants.ERROR
            
    # [END process_static_asset_config]
    # [START render_template]
    return render_template(
        'submitted_static_asset_config.html',
        template_id=template_id,
        template_project=template_project,
        template_region=template_region,
        fields=fields,
        included_assets_uris=included_assets_uris,
        excluded_assets_uris=excluded_assets_uris,
        tag_history=tag_history_enabled,
        tag_stream=tag_stream_enabled,
        status=job_creation)
    # [END render_template]


@app.route('/process_dynamic_table_config', methods=['POST'])
def process_dynamic_table_config():
    template_id = request.form['template_id']
    template_project = request.form['template_project']
    template_region = request.form['template_region']
    included_tables_uris = request.form['included_tables_uris'].rstrip()
    excluded_tables_uris = request.form['excluded_tables_uris'].rstrip()
    refresh_mode = request.form['refresh_mode']
    refresh_frequency = request.form['refresh_frequency']
    refresh_unit = request.form['refresh_unit']
    action = request.form['action']
    
    #print('included_tables_uris: ' + included_tables_uris)
    #print('excluded_tables_uris: ' + excluded_tables_uris)
    #print('refresh_mode: ' + refresh_mode)
    #print('refresh_frequency: ' + refresh_frequency)
    #print('refresh_unit: ' + refresh_unit)
    
    dcu = dc.DataCatalogUtils(template_id, template_project, template_region)
    template = dcu.get_template()
    
    if action == "Cancel Changes":
        
        return render_template(
            'tag_template.html',
            template_id=template_id,
            template_project=template_project,
            template_region=template_region, 
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
    
    tag_history_option = False
    tag_history_enabled = "OFF"
    
    if "tag_history" in request.form:
        tag_history = request.form.get("tag_history")
    
        if tag_history == "selected":
            tag_history_option = True
            tag_history_enabled = "ON"
            
    tag_stream_option = False
    tag_stream_enabled = "OFF"
    
    if "tag_stream" in request.form:
        tag_stream = request.form.get("tag_stream")
    
        if tag_stream == "selected":
            tag_stream_option = True
            tag_stream_enabled = "ON"
    
    template_uuid = teu.write_tag_template(template_id, template_project, template_region)
    config_uuid, included_tables_uris_hash = teu.write_dynamic_table_config('PENDING', fields, included_tables_uris, excluded_tables_uris, template_uuid,\
                                                                           refresh_mode, refresh_frequency, refresh_unit, \
                                                                           tag_history_option, tag_stream_option)
    if isinstance(config_uuid, str): 
        job_uuid = jm.create_job(config_uuid, 'DYNAMIC_TABLE_TAG')
    else:
        job_uuid = None
    
    if job_uuid != None: 
        job_creation = constants.SUCCESS
    else:
        job_creation = constants.ERROR
     
    # [END process_dynamic_table_config]
    # [START render_template]
    return render_template(
        'submitted_dynamic_table_config.html',
        template_id=template_id,
        template_project=template_project,
        template_region=template_region,
        fields=fields,
        included_tables_uris=included_tables_uris,
        included_tables_uris_hash=included_tables_uris_hash,
        excluded_tables_uris=excluded_tables_uris,
        refresh_mode=refresh_mode,
        refresh_frequency=refresh_frequency,
        refresh_unit=refresh_unit,
        tag_history=tag_history_enabled,
        tag_stream=tag_stream_enabled,
        status=job_creation)
    # [END render_template]


@app.route('/process_dynamic_column_config', methods=['POST'])
def process_dynamic_column_config():
    template_id = request.form['template_id']
    template_project = request.form['template_project']
    template_region = request.form['template_region']
    
    included_columns_query = request.form['included_columns_query']
    included_tables_uris = request.form['included_tables_uris'].rstrip()
    excluded_tables_uris = request.form['excluded_tables_uris'].rstrip()
    
    refresh_mode = request.form['refresh_mode']
    refresh_frequency = request.form['refresh_frequency']
    refresh_unit = request.form['refresh_unit']
    action = request.form['action']
    
    dcu = dc.DataCatalogUtils(template_id, template_project, template_region)
    template = dcu.get_template()
    
    if action == "Cancel Changes":
        
        return render_template(
            'tag_template.html',
            template_id=template_id,
            template_project=template_project,
            template_region=template_region, 
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
    
    tag_history_option = False
    tag_history_enabled = "OFF"
    
    if "tag_history" in request.form:
        tag_history = request.form.get("tag_history")
    
        if tag_history == "selected":
            tag_history_option = True
            tag_history_enabled = "ON"
            
    tag_stream_option = False
    tag_stream_enabled = "OFF"
    
    if "tag_stream" in request.form:
        tag_stream = request.form.get("tag_stream")
    
        if tag_stream == "selected":
            tag_stream_option = True
            tag_stream_enabled = "ON"
    
    template_uuid = teu.write_tag_template(template_id, template_project, template_region)
    config_uuid, included_tables_uris_hash = teu.write_dynamic_column_config('PENDING', fields, included_columns_query, included_tables_uris, excluded_tables_uris, template_uuid,\
                                                                           refresh_mode, refresh_frequency, refresh_unit, \
                                                                           tag_history_option, tag_stream_option)
    if isinstance(config_uuid, str): 
        job_uuid = jm.create_job(config_uuid, 'DYNAMIC_COLUMN_TAG')
    else:
        job_uuid = None
    
    if job_uuid != None: 
        job_creation = constants.SUCCESS
    else:
        job_creation = constants.ERROR
     
    # [END process_dynamic_column_config]
    # [START render_template]
    return render_template(
        'submitted_dynamic_column_config.html',
        template_id=template_id,
        template_project=template_project,
        template_region=template_region,
        fields=fields,
        included_columns_query=included_columns_query,
        included_tables_uris=included_tables_uris,
        included_tables_uris_hash=included_tables_uris_hash,
        excluded_tables_uris=excluded_tables_uris,
        refresh_mode=refresh_mode,
        refresh_frequency=refresh_frequency,
        refresh_unit=refresh_unit,
        tag_history=tag_history_enabled,
        tag_stream=tag_stream_enabled,
        status=job_creation)
    # [END render_template]

@app.route('/process_entry_config', methods=['POST'])
def process_entry_config():
    template_id = request.form['template_id']
    template_project = request.form['template_project']
    template_region = request.form['template_region']
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
    
    dcu = dc.DataCatalogUtils(template_id, template_project, template_region)
    template = dcu.get_template()
    
    if action == "Cancel Changes":
        
        return render_template(
            'tag_template.html',
            template_id=template_id,
            template_project=template_project,
            template_region=template_region, 
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
    
    tag_history_option = False
    tag_history_enabled = "OFF"
    
    if "tag_history" in request.form:
        tag_history = request.form.get("tag_history")
    
        if tag_history == "selected":
            tag_history_option = True
            tag_history_enabled = "ON"
            
    tag_stream_option = False
    tag_stream_enabled = "OFF"
    
    if "tag_stream" in request.form:
        tag_stream = request.form.get("tag_stream")
    
        if tag_stream == "selected":
            tag_stream_option = True
            tag_stream_enabled = "ON"
    
    template_uuid = teu.write_tag_template(template_id, template_project, template_region)
    config_uuid, included_assets_uris_hash = teu.write_entry_config('PENDING', fields, included_assets_uris, excluded_assets_uris, template_uuid,\
                                                             refresh_mode, refresh_frequency, refresh_unit, \
                                                             tag_history_option, tag_stream_option)
    if isinstance(config_uuid, str): 
        job_uuid = jm.create_job(config_uuid, 'ENTRY')
    else:
        job_uuid = None
    
    if job_uuid != None: 
        job_creation = constants.SUCCESS
    else:
        job_creation = constants.ERROR
     
    # [END process_entry_config]
    # [START render_template]
    return render_template(
        'submitted_entry_config.html',
        template_id=template_id,
        template_project=template_project,
        template_region=template_region,
        fields=fields,
        included_assets_uris=included_assets_uris,
        included_assets_uris_hash=included_assets_uris_hash,
        excluded_assets_uris=excluded_assets_uris,
        refresh_mode=refresh_mode,
        refresh_frequency=refresh_frequency,
        refresh_unit=refresh_unit,
        tag_history=tag_history_enabled,
        tag_stream=tag_stream_enabled,
        status=job_creation)
    # [END render_template]


@app.route('/process_glossary_asset_config', methods=['POST'])
def process_glossary_asset_config():
    template_id = request.form['template_id']
    template_project = request.form['template_project']
    template_region = request.form['template_region']
    mapping_table = request.form['mapping_table'].rstrip()
    included_assets_uris = request.form['included_assets_uris'].rstrip()
    excluded_assets_uris = request.form['excluded_assets_uris'].rstrip()
    refresh_mode = request.form['refresh_mode']
    refresh_frequency = request.form['refresh_frequency']
    refresh_unit = request.form['refresh_unit']
    overwrite = True # set to true as we are creating a new glossary asset config
    action = request.form['action']
    
    #print('included_assets_uris: ' + included_assets_uris)
    #print('excluded_assets_uris: ' + excluded_assets_uris)
    #print('refresh_mode: ' + refresh_mode)
    #print('refresh_frequency: ' + refresh_frequency)
    #print('refresh_unit: ' + refresh_unit)
    
    dcu = dc.DataCatalogUtils(template_id, template_project, template_region)
    template = dcu.get_template()
    
    if action == "Cancel Changes":
        
        return render_template(
            'tag_template.html',
            template_id=template_id,
            template_project=template_project,
            template_region=template_region, 
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
    
    tag_history_option = False
    tag_history_enabled = "OFF"
    
    if "tag_history" in request.form:
        tag_history = request.form.get("tag_history")
    
        if tag_history == "selected":
            tag_history_option = True
            tag_history_enabled = "ON"
            
    tag_stream_option = False
    tag_stream_enabled = "OFF"
    
    if "tag_stream" in request.form:
        tag_stream = request.form.get("tag_stream")
    
        if tag_stream == "selected":
            tag_stream_option = True
            tag_stream_enabled = "ON"
    
    template_uuid = teu.write_tag_template(template_id, template_project, template_region)

    config_uuid, included_assets_uris_hash = teu.write_glossary_asset_config('PENDING', fields, mapping_table, included_assets_uris, excluded_assets_uris, template_uuid,\
                                                             refresh_mode, refresh_frequency, refresh_unit, \
                                                             tag_history_option, tag_stream_option, overwrite)
    if isinstance(config_uuid, str): 
        job_uuid = jm.create_job(config_uuid, 'GLOSSARY_ASSET_TAG')
    else:
        job_uuid = None
    
    if job_uuid != None: 
        job_creation = constants.SUCCESS
    else:
        job_creation = constants.ERROR
     
    # [END process_dynamic_tag]
    # [START render_template]
    return render_template(
        'submitted_glossary_asset_config.html',
        template_id=template_id,
        template_project=template_project,
        template_region=template_region,
        fields=fields,
        mapping_table=mapping_table,
        included_assets_uris=included_assets_uris,
        included_assets_uris_hash=included_assets_uris_hash,
        excluded_assets_uris=excluded_assets_uris,
        refresh_mode=refresh_mode,
        refresh_frequency=refresh_frequency,
        refresh_unit=refresh_unit,
        tag_history=tag_history_enabled,
        tag_stream=tag_stream_enabled,
        status=job_creation)
    # [END render_template]


@app.route('/process_sensitive_column_config', methods=['POST'])
def process_sensitive_column_config():
    template_id = request.form['template_id']
    template_project = request.form['template_project']
    template_region = request.form['template_region']
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
    
    #print('included_tables_uris: ' + included_tables_uris)
    #print('excluded_tables_uris: ' + excluded_tables_uris)
    #print('refresh_mode: ' + refresh_mode)
    #print('refresh_frequency: ' + refresh_frequency)
    #print('refresh_unit: ' + refresh_unit)
    
    dcu = dc.DataCatalogUtils(template_id, template_project, template_region)
    template = dcu.get_template()
    
    if action == "Cancel Changes":
        
        return render_template(
            'tag_template.html',
            template_id=template_id,
            template_project=template_project,
            template_region=template_region, 
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
    
    tag_history_option = False
    tag_history_enabled = "OFF"
    
    if "tag_history" in request.form:
        tag_history = request.form.get("tag_history")
    
        if tag_history == "selected":
            tag_history_option = True
            tag_history_enabled = "ON"
            
    tag_stream_option = False
    tag_stream_enabled = "OFF"
    
    if "tag_stream" in request.form:
        tag_stream = request.form.get("tag_stream")
    
        if tag_stream == "selected":
            tag_stream_option = True
            tag_stream_enabled = "ON"
    
    template_uuid = teu.write_tag_template(template_id, template_project, template_region)

    config_uuid, included_tables_uris_hash = teu.write_sensitive_column_config('PENDING', fields, dlp_dataset, infotype_selection_table, \
                                                                                infotype_classification_table,
                                                                                included_tables_uris, excluded_tables_uris, \
                                                                                create_policy_tags, taxonomy_id, \
                                                                                template_uuid, \
                                                                                refresh_mode, refresh_frequency, refresh_unit, \
                                                                                tag_history_option, tag_stream_option, overwrite)
    if isinstance(config_uuid, str): 
        job_uuid = jm.create_job(config_uuid, 'SENSITIVE_COLUMN_TAG')
    else:
        job_uuid = None
    
    if job_uuid != None: 
        job_creation = constants.SUCCESS
    else:
        job_creation = constants.ERROR
     
    # [END process_sensitive_column_config]
    # [START render_template]
    return render_template(
        'submitted_sensitive_column_config.html',
        template_id=template_id,
        template_project=template_project,
        template_region=template_region,
        fields=fields,
        dlp_dataset=dlp_dataset,
        infotype_selection_table=infotype_selection_table,
        infotype_classification_table=infotype_classification_table,
        included_tables_uris=included_tables_uris,
        included_tables_uris_hash=included_tables_uris_hash,
        excluded_tables_uris=excluded_tables_uris,
        policy_tags=policy_tags,
        taxonomy_id=taxonomy_id,
        refresh_mode=refresh_mode,
        refresh_frequency=refresh_frequency,
        refresh_unit=refresh_unit,
        tag_history=tag_history_enabled,
        tag_stream=tag_stream_enabled,
        status=job_creation)
    # [END render_template]


@app.route('/process_restore_config', methods=['POST'])
def process_restore_config():
    
    template_id = request.form['template_id']
    template_project = request.form['template_project']
    template_region = request.form['template_region']
    action = request.form['action']
    
    dcu = dc.DataCatalogUtils(template_id, template_project, template_region)
    template = dcu.get_template()
    
    if action == "Cancel Changes":
        
        return render_template(
            'tag_template.html',
            template_id=template_id,
            template_project=template_project,
            template_region=template_region, 
            fields=template)
            
    source_template_id = request.form['source_template_id']
    source_template_project = request.form['source_template_project']
    source_template_region = request.form['source_template_region']
    
    target_template_id = request.form['target_template_id']
    target_template_project = request.form['target_template_project']
    target_template_region = request.form['target_template_region']
    
    metadata_export_location = request.form['metadata_export_location']
    
    action = request.form['action']
    
    dcu = dc.DataCatalogUtils(target_template_id, target_template_project, target_template_region)
    template = dcu.get_template()
        
    tag_history_option = False
    tag_history_enabled = "OFF"
    
    if "tag_history" in request.form:
        tag_history = request.form.get("tag_history")
    
        if tag_history == "selected":
            tag_history_option = True
            tag_history_enabled = "ON"
            
    tag_stream_option = False
    tag_stream_enabled = "OFF"
    
    if "tag_stream" in request.form:
        tag_stream = request.form.get("tag_stream")
    
        if tag_stream == "selected":
            tag_stream_option = True
            tag_stream_enabled = "ON"            
        
    source_template_uuid = teu.write_tag_template(source_template_id, source_template_project, source_template_region)
    target_template_uuid = teu.write_tag_template(target_template_id, target_template_project, target_template_region)
    
    overwrite = True
    
    config_uuid = teu.write_restore_config('PENDING', source_template_uuid, source_template_id, source_template_project, source_template_region, \
                                           target_template_uuid, target_template_id, target_template_project, target_template_region, \
                                           metadata_export_location, \
                                           tag_history_option, tag_stream_option, overwrite)                                                      

    if isinstance(config_uuid, str): 
        job_uuid = jm.create_job(config_uuid, 'RESTORE_TAG')
    else:
        job_uuid = None
    
        
    if job_uuid != None: 
        job_creation = constants.SUCCESS
    else:
        job_creation = constants.ERROR
           
    # [END process_restore_config]
    # [START render_template]
    return render_template(
        'submitted_restore_config.html',
        source_template_id=source_template_id,
        source_template_project=source_template_project,
        source_template_region=source_template_region,
        target_template_id=target_template_id,
        target_template_project=target_template_project,
        target_template_region=target_template_region,
        metadata_export_location=metadata_export_location,
        tag_history=tag_history_enabled,
        tag_stream=tag_stream_enabled,
        status=job_creation)
    # [END render_template]


@app.route('/process_import_config', methods=['POST'])
def process_import_config():
    
    template_id = request.form['template_id']
    template_project = request.form['template_project']
    template_region = request.form['template_region']

    metadata_import_location = request.form['metadata_import_location']
    
    action = request.form['action']
    
    dcu = dc.DataCatalogUtils(template_id, template_project, template_region)
    template = dcu.get_template()
    
    if action == "Cancel Changes":
        
        return render_template(
            'tag_template.html',
            template_id=template_id,
            template_project=template_project,
            template_region=template_region, 
            fields=template)
        
    tag_history_option = False
    tag_history_enabled = "OFF"
    
    if "tag_history" in request.form:
        tag_history = request.form.get("tag_history")
    
        if tag_history == "selected":
            tag_history_option = True
            tag_history_enabled = "ON"
            
    tag_stream_option = False
    tag_stream_enabled = "OFF"
    
    if "tag_stream" in request.form:
        tag_stream = request.form.get("tag_stream")
    
        if tag_stream == "selected":
            tag_stream_option = True
            tag_stream_enabled = "ON"            
        
    template_uuid = teu.write_tag_template(template_id, template_project, template_region)
  
    overwrite = True
    
    config_uuid = teu.write_import_config('PENDING', template_uuid, template_id, template_project, template_region, \
                                           metadata_import_location, \
                                           tag_history_option, tag_stream_option, overwrite)                                                      

    if isinstance(config_uuid, str): 
        job_uuid = jm.create_job(config_uuid, 'IMPORT_TAG')
    else:
        job_uuid = None
    
        
    if job_uuid != None: 
        job_creation = constants.SUCCESS
    else:
        job_creation = constants.ERROR
           
    # [END process_import_config]
    # [START render_template]
    return render_template(
        'submitted_import_config.html',
        template_id=template_id,
        template_project=template_project,
        template_region=template_region,
        metadata_import_location=metadata_import_location,
        tag_history=tag_history_enabled,
        tag_stream=tag_stream_enabled,
        status=job_creation)
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
    
    refresh_mode = request.form['refresh_mode']
    refresh_frequency = request.form['refresh_frequency']
    refresh_unit = request.form['refresh_unit']
    
    action = request.form['action']
        
    if action == "Cancel Changes":
        
        return homepage()
            
    # put source projects into a list
    print('source_projects:', source_projects)
    project_list = []
    projects = source_projects.split(',')
    for project in projects:
        project_list.append(project.strip())
    print('project_list:', project_list)
        
    config_uuid = teu.write_export_config('PENDING', project_list, source_folder, source_region, \
                                          target_project, target_dataset, target_region, write_option, \
                                          refresh_mode, refresh_frequency, refresh_unit)                                                      

    if isinstance(config_uuid, str): 
        job_uuid = jm.create_job(config_uuid, 'EXPORT_TAG')
    else:
        job_uuid = None
    
        
    if job_uuid != None: 
        job_creation = constants.SUCCESS
    else:
        job_creation = constants.ERROR
           
    # [END process_export_config]
    # [START render_template]
    return render_template(
        'submitted_export_config.html',
        source_projects=source_projects,
        source_folder=source_folder,
        source_region=source_region,
        target_project=target_project,
        target_dataset=target_dataset,
        target_region=target_region,
        write_option=write_option,
        refresh_mode=refresh_mode,
        refresh_frequency=refresh_frequency,
        refresh_unit=refresh_unit,
        status=job_creation)
    # [END render_template]


##################### INTERNAL METHODS #################

def get_refresh_parameters(json):
    
    refresh_mode = json['refresh_mode']
    refresh_frequency = ''
    refresh_unit = ''
    
    if refresh_mode == 'AUTO':
        if 'refresh_frequency' in json:
            refresh_frequency = json['refresh_frequency']
        else:
            print("config request must include a refresh_frequency when refresh_mode is set to AUTO. This is a required parameter.")
            resp = jsonify(success=False)
            return resp
    
    if refresh_mode == 'AUTO':
        if 'refresh_unit' in json:
            refresh_unit = json['refresh_unit']
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
    tag_history: true if tag history is on, false otherwise 
    tag_stream: true if tag stream is on, false otherwise
Returns:
    job_uuid 
"""
@app.route("/dynamic_table_tags", methods=['POST'])
def dynamic_table_tags():
    json = request.get_json(force=True) 
    #print('json: ' + str(json))
       
    valid_parameters, template_id, template_project, template_region = check_template_parameters('dynamic_table_tags', json)
    
    if valid_parameters != True:
        response = {
                "status": "error",
                "message": "Request JSON is missing some required tag template parameters",
        }
        return jsonify(response), 400
     
    #print('template_id: ' + template_id)
    #print('template_project: ' + template_project)
    #print('template_region: ' + template_region)
    
    template_uuid = teu.write_tag_template(template_id, template_project, template_region)
    
    dcu = dc.DataCatalogUtils(template_id, template_project, template_region)
    included_fields = json['fields']
    fields = dcu.get_template(included_fields=included_fields)
    
    if 'included_tables_uris' in json:
        included_tables_uris = json['included_tables_uris']
    else:
        print("The dynamic_table_tags request requires an included_tables_uris parameter.")
        resp = jsonify(success=False)
        return resp
    
    if 'excluded_tables_uris' in json:
        excluded_tables_uris = json['excluded_tables_uris']
    else:
        excluded_tables_uris = ''
    
    refresh_mode, refresh_frequency, refresh_unit = get_refresh_parameters(json)
    
    if 'tag_history' in json:
        tag_history = json['tag_history']
    else:
        tag_history = None
      
    if 'tag_stream' in json:  
        tag_stream = json['tag_stream']
    else:
        tag_stream = None
    
    config_uuid, included_tables_uris_hash = teu.write_dynamic_table_config('PENDING', fields, included_tables_uris, excluded_tables_uris, \
                                                                     template_uuid,\
                                                                     refresh_mode, refresh_frequency, refresh_unit, \
                                                                     tag_history, tag_stream)                                                      

    if isinstance(config_uuid, str): 
        job_uuid = jm.create_job(config_uuid, 'DYNAMIC_TABLE_TAG')
    else:
        job_uuid = None
    
    return jsonify(job_uuid=job_uuid)
    

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
    tag_history: true if tag history is on, false otherwise 
    tag_stream: true if tag stream is on, false otherwise
Returns:
    job_uuid 
"""
@app.route("/dynamic_column_tags", methods=['POST'])
def dynamic_column_tags():
    json = request.get_json(force=True) 
    #print('json: ' + str(json))
       
    valid_parameters, template_id, template_project, template_region = check_template_parameters('dynamic_column_tags', json)
    
    if valid_parameters != True:
        response = {
                "status": "error",
                "message": "Request JSON is missing some required tag template parameters",
        }
        return jsonify(response), 400
     
    #print('template_id: ' + template_id)
    #print('template_project: ' + template_project)
    #print('template_region: ' + template_region)
    
    template_uuid = teu.write_tag_template(template_id, template_project, template_region)
    
    dcu = dc.DataCatalogUtils(template_id, template_project, template_region)
    included_fields = json['fields']
    fields = dcu.get_template(included_fields=included_fields)

    if 'included_columns_query' in json:
        included_columns_query = json['included_columns_query']
    else:
        print("The dynamic_columns_tags request requires an included_columns_query parameter.")
        resp = jsonify(success=False)
        return resp
    
    if 'included_tables_uris' in json:
        included_tables_uris = json['included_tables_uris']
    else:
        print("The dynamic_table_tags request requires an included_tables_uris parameter.")
        resp = jsonify(success=False)
        return resp
    
    if 'excluded_tables_uris' in json:
        excluded_tables_uris = json['excluded_tables_uris']
    else:
        excluded_tables_uris = ''
    
    refresh_mode, refresh_frequency, refresh_unit = get_refresh_parameters(json)
    
    if 'tag_history' in json:
        tag_history = json['tag_history']
    else:
        tag_history = None
      
    if 'tag_stream' in json:  
        tag_stream = json['tag_stream']
    else:
        tag_stream = None
    
    config_uuid, included_tables_uris_hash = teu.write_dynamic_column_config('PENDING', fields, included_columns_query, \
                                                                            included_tables_uris, excluded_tables_uris, \
                                                                            template_uuid,\
                                                                            refresh_mode, refresh_frequency, refresh_unit, \
                                                                            tag_history, tag_stream)                                                      

    if isinstance(config_uuid, str): 
        job_uuid = jm.create_job(config_uuid, 'DYNAMIC_COLUMN_TAG')
    else:
        job_uuid = None
    
    return jsonify(job_uuid=job_uuid)

        
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
    tag_history: true if tag history is on, false otherwise 
    tag_stream: true if tag stream is on, false otherwise
Returns:
    job_uuid
"""
@app.route("/static_asset_tags", methods=['POST'])
def static_asset_tags():
    json = request.get_json(force=True) 
    print('json: ' + str(json))
       
    valid_parameters, template_id, template_project, template_region = check_template_parameters('static_asset_tags', json)
    
    if valid_parameters != True:
        response = {
                "status": "error",
                "message": "Request JSON is missing some required tag template parameters",
        }
        return jsonify(response), 400
     
    #print('template_id: ' + template_id)
    #print('template_project: ' + template_project)
    #print('template_region: ' + template_region)
    
    template_uuid = teu.write_tag_template(template_id, template_project, template_region)
    
    dcu = dc.DataCatalogUtils(template_id, template_project, template_region)
    included_fields = json['fields']
    fields = dcu.get_template(included_fields=included_fields)

    if 'included_assets_uris' in json:
        included_assets_uris = json['included_assets_uris']
    else:
        print("The static_asset_tags request requires an included_assets_uris parameter.")
        resp = jsonify(success=False)
        return resp
    
    if 'excluded_assets_uris' in json:
        excluded_assets_uris = json['excluded_assets_uris']
    else:
        excluded_assets_uris = ''

    if 'tag_history' in json:
        tag_history = json['tag_history']
    else:
        tag_history = None
      
    if 'tag_stream' in json:  
        tag_stream = json['tag_stream']
    else:
        tag_stream = None
    
    refresh_mode, refresh_frequency, refresh_unit = get_refresh_parameters(json)
    
    # since we are creating a new config, we are overwriting any previously created tags
    overwrite = True
    
    config_uuid, included_assets_uris_hash = teu.write_static_asset_config('PENDING', fields, included_assets_uris, excluded_assets_uris, template_uuid,\
                                                            refresh_mode, refresh_frequency, refresh_unit, \
                                                            tag_history, tag_stream, overwrite)
     
    if isinstance(config_uuid, str): 
        job_uuid = jm.create_job(config_uuid, 'STATIC_ASSET_TAG')
    else:
        job_uuid = None

    return jsonify(job_uuid=job_uuid)
    

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
    tag_history: true if tag history is on, false otherwise 
    tag_stream: true if tag stream is on, false otherwise
Returns:
    job_uuid 
"""
@app.route("/entries", methods=['POST'])
def entries():
    json = request.get_json(force=True) 
    print('json: ' + str(json))
       
    valid_parameters, template_id, template_project, template_region = check_template_parameters('entries', json)
    
    if valid_parameters != True:
        response = {
                "status": "error",
                "message": "Request JSON is missing some required tag template parameters",
        }
        return jsonify(response), 400
     
    #print('template_id: ' + template_id)
    #print('template_project: ' + template_project)
    #print('template_region: ' + template_region)
    
    template_uuid = teu.write_tag_template(template_id, template_project, template_region)
    
    dcu = dc.DataCatalogUtils(template_id, template_project, template_region)
    included_fields = json['fields']
    fields = dcu.get_template(included_fields=included_fields)

    if 'included_assets_uris' in json:
        included_assets_uris = json['included_assets_uris']
    else:
        print("The entry request requires an included_assets_uris parameter.")
        resp = jsonify(success=False)
        return resp

    if 'excluded_assets_uris' in json:
        excluded_assets_uris = json['excluded_assets_uris']
    else:
        excluded_assets_uris = ''
    
    refresh_mode, refresh_frequency, refresh_unit = get_refresh_parameters(json)
    
    if 'tag_history' in json:
        tag_history = json['tag_history']
    else:
        tag_history = None
      
    if 'tag_stream' in json:  
        tag_stream = json['tag_stream']
    else:
        tag_stream = None
    
    config_uuid, included_assets_uris_hash = teu.write_entry_config('PENDING', fields, included_assets_uris, excluded_assets_uris, template_uuid,\
                                                             refresh_mode, refresh_frequency, refresh_unit, \
                                                             tag_history, tag_stream)                                                      

    if isinstance(config_uuid, str): 
        job_uuid = jm.create_job(config_uuid, 'ENTRY')
    else:
        job_uuid = None
    
    return jsonify(job_uuid=job_uuid)


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
    tag_history: true if tag history is on, false otherwise 
    tag_stream: true if tag stream is on, false otherwise
Returns:
    job_uuid 
"""
@app.route("/glossary_asset_tags", methods=['POST'])
def glossary_asset_tags():
    json = request.get_json(force=True) 
    print('json: ' + str(json))
       
    valid_parameters, template_id, template_project, template_region = check_template_parameters('glossary_asset_tags', json)
    
    if valid_parameters != True:
        response = {
                "status": "error",
                "message": "Request JSON is missing some required tag template parameters",
        }
        return jsonify(response), 400
     
    #print('template_id: ' + template_id)
    #print('template_project: ' + template_project)
    #print('template_region: ' + template_region)
    
    template_uuid = teu.write_tag_template(template_id, template_project, template_region)
    
    dcu = dc.DataCatalogUtils(template_id, template_project, template_region)
    included_fields = json['fields']
    fields = dcu.get_template(included_fields=included_fields)
    
    # validate mapping_table field
    if 'mapping_table' in json:
        mapping_table = json['mapping_table']
    else:
        print("glossary_asset_tags request doesn't include a mapping_table field. This is a required parameter.")
        resp = jsonify(success=False)
        return resp
    
    if 'included_assets_uris' in json:
        included_assets_uris = json['included_assets_uris']
    else:
        print("The glossary_asset_tags request requires an included_assets_uris parameter.")
        resp = jsonify(success=False)
        return resp
    
    if 'excluded_assets_uris' in json:
        excluded_assets_uris = json['excluded_assets_uris']
    else:
        excluded_assets_uris = ''
    
    refresh_mode, refresh_frequency, refresh_unit = get_refresh_parameters(json)
    
    if 'tag_history' in json:
        tag_history = json['tag_history']
    else:
        tag_history = None
      
    if 'tag_stream' in json:  
        tag_stream = json['tag_stream']
    else:
        tag_stream = None

    overwrite = True
    
    config_uuid, included_assets_uris_hash = teu.write_glossary_asset_config('PENDING', fields, mapping_table, included_assets_uris, excluded_assets_uris, template_uuid,\
                                                                             refresh_mode, refresh_frequency, refresh_unit, \
                                                                             tag_history, tag_stream, overwrite)                                                      

    if isinstance(config_uuid, str): 
        job_uuid = jm.create_job(config_uuid, 'GLOSSARY_ASSET_TAG')
    else:
        job_uuid = None
    
    return jsonify(job_uuid=job_uuid)


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
    tag_history: true if tag history is on, false otherwise 
    tag_stream: true if tag stream is on, false otherwise
Returns:
    job_uuid 
"""
@app.route("/sensitive_column_tags", methods=['POST'])
def sensitive_column_tags():
    json = request.get_json(force=True) 
    print('json: ' + str(json))
    
    valid_parameters, template_id, template_project, template_region = check_template_parameters('sensitive_column_tags', json)
    
    if valid_parameters != True:
        response = {
                "status": "error",
                "message": "Request JSON is missing some required tag template parameters",
        }
        return jsonify(response), 400
     
    #print('template_id: ' + template_id)
    #print('template_project: ' + template_project)
    #print('template_region: ' + template_region)
    
    template_uuid = teu.write_tag_template(template_id, template_project, template_region)
    
    dcu = dc.DataCatalogUtils(template_id, template_project, template_region)
    included_fields = json['fields']
    fields = dcu.get_template(included_fields=included_fields)

    # validate dlp_dataset parameter
    if 'dlp_dataset' in json:
        dlp_dataset = json['dlp_dataset']
    else:
        print("sensitive_column_config request doesn't include a dlp_dataset field. This is a required parameter.")
        resp = jsonify(success=False)
        return resp
            
    # validate infotype_selection_table parameter
    if 'infotype_selection_table' in json:
        infotype_selection_table = json['infotype_selection_table']
    else:
        print("sensitive_column_config request doesn't include an infotype_selection_table field. This is a required parameter.")
        resp = jsonify(success=False)
        return resp
        
    # validate infotype_classification_table parameter
    if 'infotype_classification_table' in json:
        infotype_classification_table = json['infotype_classification_table']
    else:
        print("sensitive_column_config request doesn't include an infotype_classification_table field. This is a required parameter.")
        resp = jsonify(success=False)
        return resp
    
    if 'included_tables_uris' in json:
        included_tables_uris = json['included_tables_uris']
    else:
        print("The sensitive_column_tags request requires an included_tables_uris parameter.")
        resp = jsonify(success=False)
        return resp
    
    if 'excluded_tables_uris' in json:
        excluded_tables_uris = json['excluded_tables_uris']
    else:
        excluded_tables_uris = ''
    
    # validate create_policy_tags parameter
    if 'create_policy_tags' in json:
        create_policy_tags = json['create_policy_tags']
    else:
        print("The sensitive_column_tags request requires a create_policy_tags field.")
        resp = jsonify(success=False)
        return resp
        
    if create_policy_tags:
        if 'taxonomy_id' in json:
            taxonomy_id = json['taxonomy_id']
        else:
            print("The sensitive_column_tags request requires a taxonomy_id when the create_policy_tags field is true. ")
            resp = jsonify(success=False)
            return resp
        
    refresh_mode, refresh_frequency, refresh_unit = get_refresh_parameters(json)
            
    if 'tag_history' in json:
        tag_history = json['tag_history']
    else:
        tag_history = None
      
    if 'tag_stream' in json:  
        tag_stream = json['tag_stream']
    else:
        tag_stream = None

    overwrite = True
    
    config_uuid, included_tables_uris_hash = teu.write_sensitive_column_config('PENDING', fields, dlp_dataset, infotype_selection_table, \
                                                                                infotype_classification_table, \
                                                                                included_tables_uris, excluded_tables_uris, \
                                                                                create_policy_tags, taxonomy_id, template_uuid, \
                                                                                refresh_mode, refresh_frequency, refresh_unit, \
                                                                                tag_history, tag_stream, overwrite)                                                      

    if isinstance(config_uuid, str): 
        job_uuid = jm.create_job(config_uuid, 'SENSITIVE_COLUMN_TAG')
    else:
        job_uuid = None
    
    return jsonify(job_uuid=job_uuid)


"""
Args:
    source_template_id: The tag template id whose tags are to be restored
    source_template_project: The source tag template's project id 
    source_template_region: The source tag template's region 
    target_template_id: The tag template id whose tags are to be restored
    target_template_project: The source tag template's project id 
    target_template_region: The source tag template's region
    metadata_export_location: The path to the export files on GCS (Cloud Storage)
    tag_history: true if tag history is on, false otherwise 
    tag_stream: true if tag stream is on, false otherwise
Returns:
    job_uuid 
"""
@app.route("/restore_tags", methods=['POST'])
def restore_tags():
    json = request.get_json(force=True) 
    print('json: ' + str(json))
    
    if 'source_template_id' in json:
        source_template_id = json['source_template_id']
    else:
        print("The restore_tags request requires a source_template_id parameter.")
        resp = jsonify(success=False)
        return resp

    if 'source_template_project' in json:
        source_template_project = json['source_template_project']
    else:
        print("The restore_tags request requires a source_template_project parameter.")
        resp = jsonify(success=False)
        return resp
    
    if 'source_template_region' in json:
        source_template_region = json['source_template_region']
    else:
        print("The restore_tags request requires a source_template_region parameter.")
        resp = jsonify(success=False)
        return resp
       

    if 'target_template_id' in json:
        target_template_id = json['target_template_id']
    else:
        print("The restore_tags request requires a target_template_id parameter.")
        resp = jsonify(success=False)
        return resp

    if 'target_template_project' in json:
        target_template_project = json['target_template_project']
    else:
        print("The restore_tags request requires a target_template_project parameter.")
        resp = jsonify(success=False)
        return resp
    
    if 'target_template_region' in json:
        target_template_region = json['target_template_region']
    else:
        print("The restore_tags request requires a target_template_region parameter.")
        resp = jsonify(success=False)
        return resp

    if 'metadata_export_location' in json:
        metadata_export_location = json['metadata_export_location']
    else:
        print("The restore_tags request requires the metadata_export_location parameter.")
        resp = jsonify(success=False)
        return resp

    source_template_uuid = teu.write_tag_template(source_template_id, source_template_project, source_template_region)
    target_template_uuid = teu.write_tag_template(target_template_id, target_template_project, target_template_region)
    
    if 'tag_history' in json:
        tag_history = json['tag_history']
    else:
        tag_history = None
      
    if 'tag_stream' in json:  
        tag_stream = json['tag_stream']
    else:
        tag_stream = None

    overwrite = True
    
    config_uuid = teu.write_restore_config('PENDING', source_template_uuid, source_template_id, source_template_project, source_template_region, \
                                           target_template_uuid, target_template_id, target_template_project, target_template_region, \
                                           metadata_export_location, \
                                           tag_history, tag_stream, overwrite)                                                      

    if isinstance(config_uuid, str): 
        job_uuid = jm.create_job(config_uuid, 'RESTORE_TAG')
    else:
        job_uuid = None
    
    return jsonify(job_uuid=job_uuid)


@app.route("/import_tags", methods=['POST'])
def import_tags():
    json = request.get_json(force=True) 
    print('json: ' + str(json))
       
    valid_parameters, template_id, template_project, template_region = check_template_parameters('import_tags', json)
    
    if valid_parameters != True:
        response = {
                "status": "error",
                "message": "Request JSON is missing some required tag template parameters",
        }
        return jsonify(response), 400
    
    #print('template_id: ', template_id)
    #print('template_project: ', template_project)
    #print('template_region: ', template_region)
    
    template_uuid = teu.write_tag_template(template_id, template_project, template_region)

    if 'metadata_import_location' in json:
        metadata_export_location = json['metadata_import_location']
    else:
        print("import config type requires the metadata_import_location parameter. Please add this parameter to the json object.")
        resp = jsonify(success=False)
        return resp
        
    metadata_import_location = json['metadata_import_location']
           
    if 'tag_history' in json:
        tag_history = json['tag_history']
    else:
        tag_history = None
      
    if 'tag_stream' in json:  
        tag_stream = json['tag_stream']
    else:
        tag_stream = None

    overwrite = True
    
    config_uuid = teu.write_import_config('PENDING', template_uuid, template_id, template_project, template_region, \
                                           metadata_import_location, tag_history, tag_stream, overwrite)                                                      

    if isinstance(config_uuid, str): 
        job_uuid = jm.create_job(config_uuid, 'IMPORT_TAG')
    else:
        job_uuid = None
    
    return jsonify(job_uuid=job_uuid)


@app.route("/export_tags", methods=['POST'])
def export_tags():
    json = request.get_json(force=True) 
    print('json: ' + str(json))
       
    if 'source_projects' in json:
        source_projects = json['source_projects']
    else:
        source_projects = ''
    
    if 'source_folder' in json:
        source_folder = json['source_folder']
    else:
        source_folder = ''
    
    if source_projects == '' and source_folder == '':
        print("The export config requires either a source_projects or source_folder parameter. Please add the parameter to the json object.")
        resp = jsonify(success=False)
        return resp
    
    if 'source_region' in json:
        source_region = json['source_region']
    else:
        print("The export config requires either a source_region parameter. Please add the parameter to the json object.")
        resp = jsonify(success=False)
        return resp
              
    if 'target_project' in json:
        target_project = json['target_project']
    else:
        print("The export config requires a target_project parameter. Please add the parameter to the json object.")
        resp = jsonify(success=False)
        return resp

    if 'target_dataset' in json:
        target_dataset = json['target_dataset']
    else:
        print("The export config requires a target_dataset parameter. Please add the parameter to the json object.")
        resp = jsonify(success=False)
        return resp
    
    if 'target_region' in json:
        target_region = json['target_region']
    else:
        print("The export config requires a target_region parameter. Please add the parameter to the json object.")
        resp = jsonify(success=False)
        return resp
      
    if 'refresh_mode' in json:
        refresh_mode = json['refresh_mode']
    else:
        print("The export config requires a refresh_mode parameter. Please add the parameter to the json object.")
        resp = jsonify(success=False)
        return resp  
    
    if refresh_mode.upper() == 'AUTO':
        
        if 'refresh_frequency' in json:
            refresh_frequency = json['refresh_frequency']
        else:
            print("The export config requires a refresh_frequency parameter when refresh_mode = AUTO. Please add the parameter to the json object.")
            resp = jsonify(success=False)
            return resp
        
        if 'refresh_unit' in json:
            refresh_unit = json['refresh_unit']
        else:
            print("The export config requires a refresh_unit parameter when refresh_mode = AUTO. Please add the parameter to the json object.")
            resp = jsonify(success=False)
            return resp
    else:
        refresh_frequency = None
        refresh_unit = None 
    
    
    if 'write_option' in json:
        write_option = json['write_option']
    else:
        print("The export config requires a write_option parameter. Please add the parameter to the json object.")
        resp = jsonify(success=False)
        return resp
        
    config_uuid = teu.write_export_config('PENDING', source_projects, source_folder, source_region, \
                                          target_project, target_dataset, target_region, write_option, \
                                          refresh_mode, refresh_frequency, refresh_unit)                                                      

    if isinstance(config_uuid, str): 
        job_uuid = jm.create_job(config_uuid, 'EXPORT_TAG')
    else:
        job_uuid = None
    
    return jsonify(job_uuid=job_uuid)


@app.route("/copy_tags", methods=['POST'])
def copy_tags():
    json = request.get_json(force=True) 
    print('json: ' + str(json))
       
    if 'source_project' in json:
        source_project = json['source_project']
    else:
        response = {
                "status": "error",
                "message": "Request JSON is missing a source_project parameter",
        }
        return jsonify(response), 400
    
    if 'source_dataset' in json:
        source_dataset = json['source_dataset']
    else:
        response = {
                "status": "error",
                "message": "Request JSON is missing a source_dataset parameter",
        }
        return jsonify(response), 400
    
    if 'source_table' in json:
         source_table = json['source_table']
    else:
         response = {
             "status": "error",
             "message": "Request JSON is missing a source_table parameter",
     }
         return jsonify(response), 400
 
    if 'target_project' in json:
        target_project = json['target_project']
    else:
        response = {
                "status": "error",
                "message": "Request JSON is missing a target_project parameter",
        }
        return jsonify(response), 400
    
    if 'target_dataset' in json:
        target_dataset = json['target_dataset']
    else:
        response = {
                "status": "error",
                "message": "Request JSON is missing a target_dataset parameter",
        }
        return jsonify(response), 400
    
    if 'target_table' in json:
         target_table = json['target_table']
    else:
         response = {
             "status": "error",
             "message": "Request JSON is missing a target_table parameter",
     }
         return jsonify(response), 400

    dcu = dc.DataCatalogUtils()
    success = dcu.copy_tags(source_project, source_dataset, source_table, target_project, target_dataset, target_table)                                                      
    
    if success:
        response = {"status": "success"}
    else:
        response = {"status": "failure"}
    
    return jsonify(response)


@app.route("/update_tag_subset", methods=['POST'])
def update_tag_subset():
    json = request.get_json(force=True) 
    print('json: ' + str(json))
    valid_parameters, template_id, template_project, template_region = check_template_parameters('update_tag_subset', json)
    
    if valid_parameters != True:
        response = {
                "status": "error",
                "message": "Request JSON is missing some required tag template parameters",
        }
        return jsonify(response), 400   
        
    if 'entry_name' in json:
        entry_name = json['entry_name']
    else:
        response = {
                "status": "error",
                "message": "Request JSON is missing a entry_name parameter",
        }
        return jsonify(response), 400
    
    if 'changed_fields' in json:
         changed_fields = json['changed_fields']
    else:
         response = {
             "status": "error",
             "message": "Request JSON is missing a changed_fields parameter",
     }
         return jsonify(response), 400

    dcu = dc.DataCatalogUtils()
    success = dcu.update_tag_subset(template_id, template_project, template_region, entry_name, changed_fields)

    if success:
        response = {"status": "success"}
    else:
        response = {"status": "failure"}
    
    return jsonify(response)


"""
Args:
    template_id: tag template to use
    template_project: tag template's Google Cloud project 
    template_region: tag template's region 
    included_tables_uris: tag config's included uris or
    included_tables_uris_hash: tag config's md5 hash value (in place of the included_tables_uris)
Note: caller must provide either the included_tables_uris_hash or included_tables_uris
Returns:
    job_uuid = unique identifer for job
"""
@app.route("/ondemand_updates", methods=['POST'])
def ondemand_updates():
    json = request.get_json(force=True)    
    template_id = json['template_id']
    template_project = json['template_project']
    template_region = json['template_region']
        
    template_exists, template_uuid = teu.read_tag_template(template_id, template_project, template_region)
    
    # validate request
    if not template_exists:
        print("tag_template " + template_id + " doesn't exist")
        resp = jsonify(success=False)
        return resp
    
    if 'included_tables_uris' in json:
       included_tables_uris = json['included_tables_uris']
       success, config = teu.lookup_config_by_included_tables_uris(template_uuid, included_tables_uris, None)
          
    elif 'included_tables_uris_hash' in json:
        included_tables_uris_hash = json['included_tables_uris_hash']
        success, config = teu.lookup_config_by_included_tables_uris(template_uuid, None, included_tables_uris_hash)
 
    elif 'included_assets_uris' in json:
       included_assets_uris = json['included_assets_uris']
       success, config = teu.lookup_config_by_included_assets_uris(template_uuid, included_assets_uris, None)
          
    elif 'included_assets_uris_hash' in json:
        included_assets_uris_hash = json['included_assets_uris_hash']
        success, config = teu.lookup_config_by_included_assets_uris(template_uuid, None, included_assets_uris_hash)
    
    else:
        resp = jsonify(success=False, message="Request is missing one of these required fields: included_tables_uris, included_tables_uris_hash, included_assets_uris, or included_assets_uris_hash.")
        return resp
    
    if success != True:
        print("config not found " + str(config))
        resp = jsonify(success=False, message="Config not found.")
        return resp
    
    # validate the matching config, i.e. make sure the scheduling mode is set to ON_DEMAND and not AUTO
    if config['refresh_mode'] == 'AUTO':
        print("config == AUTO: " + str(config))
        resp = jsonify(success=False, message="Config has refresh_mode='AUTO'. Please update your config to refresh_mode='ON_DEMAND' prior to calling this method.")
        return resp
        
    # config is valid, create the job
    if isinstance(config['config_uuid'], str): 
        job_uuid = jm.create_job(config['config_uuid'], config['config_type'])
    else:
        job_uuid = None
    
    return jsonify(job_uuid=job_uuid)
    
    #[END ondemand_updates]
   
"""
Args:
    job_uuid = unique identifer for job
Returns:
    job_status = one of (PENDING, RUNNING, COMPLETE, ERROR)
    task_count = number of tasks associates with this jobs
    tasks_ran = number of tasks that have run
    tasks_completed = number of tasks which have completed
    tasks_failed = number of tasks which have failed
""" 
@app.route("/get_job_status", methods=['POST'])
def get_job_status(): 
    
    json = request.get_json(force=True)
    
    if 'job_uuid' in json:
        job_uuid = json['job_uuid']
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
                       tasks_completed=job['tasks_completed'], tasks_failed=job['tasks_failed'])
    else:
        return jsonify(job_status=job['job_status'], task_count=job['task_count'], tasks_ran=job['tasks_ran'],\
                       tasks_completed=job['tasks_completed'], tasks_failed=job['tasks_failed'])
    

"""
Method called by Cloud Scheduler to update tags set to auto refresh
Args:
    None
Returns:
    True if the request succeeded, False otherwise
""" 
@app.route("/scheduled_auto_updates", methods=['POST'])
def scheduled_auto_updates():
    
    try:    
        print('*** enter scheduled_auto_updates ***')
        
        jobs = []
        
        ready_configs = teu.read_ready_configs()
        
        for config_uuid, config_type in ready_configs:
        
            print('ready config: ', config_uuid, ', ', config_type)
            
            if isinstance(config_uuid, str): 
                teu.update_config_status(config_uuid, config_type, 'PENDING')
                teu.increment_version_next_run(config_uuid, config_type)
                job_uuid = jm.create_job(config_uuid, config_type)
                jobs.append(job_uuid)

        print('created jobs: ' + str(jobs))
        resp = jsonify(success=True, job_ids=json.dumps(jobs))
    
    except Exception as e:
        print('failed scheduled_auto_updates {}'.format(e))
        resp = jsonify(success=False, message='failed scheduled_auto_updates ' + str(e))
    
    return resp


################ INTERNAL PROCESSING METHODS #################

@app.route("/_split_work", methods=['POST'])
def _split_work():
    
    print('*** enter _split_work ***')
    
    json = request.get_json(force=True)
    #print('json: ', json)
    
    job_uuid = json['job_uuid']
    config_uuid = json['config_uuid']
    config_type = json['config_type']

    config = teu.read_config(config_uuid, config_type)
    
    print('config: ', config)
    
    if config == {}:
       resp = jsonify(success=False)
       return resp 
    
    # dynamic table and dynamic column and sensitive column configs
    if 'included_tables_uris' in config:
        uris = list(res.Resources.get_resources(config.get('included_tables_uris'), config.get('excluded_tables_uris', None)))
        
        print('inside _split_work() uris: ', uris)
        
        jm.record_num_tasks(job_uuid, len(uris))
        jm.update_job_running(job_uuid) 
        tm.create_config_uuid_tasks(job_uuid, config_uuid, config_type, uris)
    
    # static asset config and glossary asset config    
    if 'included_assets_uris' in config:
        uris = list(res.Resources.get_resources(config.get('included_assets_uris'), config.get('excluded_assets_uris', None)))
        
        print('inside _split_work() uris: ', uris)
        
        jm.record_num_tasks(job_uuid, len(uris))
        jm.update_job_running(job_uuid) 
        tm.create_config_uuid_tasks(job_uuid, config_uuid, config_type, uris)
    
    # export tag config
    if config_type == 'EXPORT_TAG':
        
        bqu = bq.BigQueryUtils(config['target_region'])
        
        # create report tables if they don't exist
        tables_created = bqu.create_report_tables(config['target_project'], config['target_dataset'])
        print('Info: created report tables:', tables_created)
        
        if tables_created == False and config['write_option'] == 'truncate':
            bqu.truncate_report_tables(config['target_project'], config['target_dataset'])
            print('Info: truncated report tables')

        if config['source_folder']:
            uris = res.Resources.get_resources_by_folder(config['source_folder'])
        else:
            uris = res.Resources.get_resources_by_project(config['source_projects'])
        
        print('Info: Number of uris:', uris)
        print('Info: uris:', uris)
        
        jm.record_num_tasks(job_uuid, len(uris))
        jm.update_job_running(job_uuid) 
        tm.create_config_uuid_tasks(job_uuid, config_uuid, config_type, uris)
    
    # import or restore tag configs
    if config_type == 'IMPORT_TAG' or config_type == 'RESTORE_TAG':
                    
        if config_type == 'IMPORT_TAG':
            csv_files = list(res.Resources.get_resources(config.get('metadata_import_location'), None))
            #print('csv_files: ', csv_files)
        
            extracted_tags = []
        
            for csv_file in csv_files:
                extracted_tags.extend(cp.CsvParser.extract_tags(csv_file))
    
        if config_type == 'RESTORE_TAG':
            bkp_files = list(res.Resources.get_resources(config.get('metadata_export_location'), None))
        
            #print('bkp_files: ', bkp_files)
            extracted_tags = []
        
            for bkp_file in bkp_files:
                extracted_tags.append(bfp.BackupFileParser.extract_tags(config.get('source_template_id'), config.get('source_template_project'), \
                                                                        bkp_file))
             
        # no tags were extracted from the CSV files
        if extracted_tags == [[]]:
           resp = jsonify(success=False)
           return resp
        
        jm.record_num_tasks(job_uuid, len(extracted_tags))
        jm.update_job_running(job_uuid) 
        tm.create_tag_extract_tasks(job_uuid, config_uuid, config_type, extracted_tags)
    

    # update the status of the config, no matter which config type is running
    teu.update_config_status(config_uuid, config_type, 'RUNNING')
    
    resp = jsonify(success=True)
    return resp
    

@app.route("/_run_task", methods=['POST'])
def _run_task():
    
    print('*** enter _run_task ***')
    
    creation_status = constants.ERROR
    
    json = request.get_json(force=True)
    job_uuid = json['job_uuid']
    config_uuid = json['config_uuid']
    config_type = json['config_type']
    shard_uuid = json['shard_uuid']
    task_uuid = json['task_uuid']
    
    if 'uri' in json:
        uri = json['uri']
    else:
        uri = None
        #print('uri: ', uri)
        
    if 'tag_extract' in json:
        tag_extract = json['tag_extract']
        #print('tag_extract: ', tag_extact)
    else:
        tag_extract = None
        
    
    tm.update_task_status(shard_uuid, task_uuid, 'RUNNING')
    
    # retrieve the config 
    config = teu.read_config(config_uuid, config_type)
    print('config: ', config)
      
    if config_type == 'EXPORT_TAG':
        dcu = dc.DataCatalogUtils()
    
    elif config_type == 'IMPORT_TAG':
        
        if 'template_id' not in config or 'template_project' not in config or 'template_region' not in config:
            response = {
                    "status": "error",
                    "message": "Request JSON is missing required template parameters",
            }
            return jsonify(response), 400
        
        dcu = dc.DataCatalogUtils(config['template_id'], config['template_project'], config['template_region'])
    
    elif config_type == 'RESTORE_TAG':
        
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
        
        dcu = dc.DataCatalogUtils(config['target_template_id'], config['target_template_project'], config['target_template_region'])
    else:
        if 'template_uuid' not in config:
            response = {
                    "status": "error",
                    "message": "Request JSON is missing some required template_uuid parameter",
            }
            return jsonify(response), 400
            
        template_config = teu.read_tag_template_config(config['template_uuid'])
        dcu = dc.DataCatalogUtils(template_config['template_id'], template_config['template_project'], template_config['template_region'])
            
    
    if config_type == 'DYNAMIC_TABLE_TAG':
        creation_status = dcu.apply_dynamic_table_config(config['fields'], uri, config['config_uuid'], \
                                                         config['template_uuid'], config['tag_history'], \
                                                         config['tag_stream'])                                               
    if config_type == 'DYNAMIC_COLUMN_TAG':
        creation_status = dcu.apply_dynamic_column_config(config['fields'], config['included_columns_query'], uri, config['config_uuid'], \
                                                          config['template_uuid'], config['tag_history'], \
                                                          config['tag_stream'])
    if config_type == 'STATIC_ASSET_TAG':
        creation_status = dcu.apply_static_asset_config(config['fields'], uri, config['config_uuid'], \
                                                        config['template_uuid'], config['tag_history'], \
                                                        config['tag_stream'], config['overwrite'])                                                   
    if config_type == 'ENTRY':
        creation_status = dcu.apply_entry_config(config['fields'], uri, config['config_uuid'], \
                                                 config['template_uuid'], config['tag_history'], \
                                                 config['tag_stream']) 
    if config_type == 'GLOSSARY_ASSET_TAG':
        creation_status = dcu.apply_glossary_asset_config(config['fields'], config['mapping_table'], uri, config['config_uuid'], \
                                                    config['template_uuid'], config['tag_history'], \
                                                    config['tag_stream'], config['overwrite'])
    if config_type == 'SENSITIVE_COLUMN_TAG':
        creation_status = dcu.apply_sensitive_column_config(config['fields'], config['dlp_dataset'], config['infotype_selection_table'], \
                                                            config['infotype_classification_table'], uri, config['create_policy_tags'], \
                                                            config['taxonomy_id'], config['config_uuid'], \
                                                            config['template_uuid'], config['tag_history'], \
                                                            config['tag_stream'], config['overwrite'])
    if config_type == 'EXPORT_TAG':
        creation_status = dcu.apply_export_config(config['config_uuid'], config['target_project'], config['target_dataset'], config['target_region'], uri)
    
    if config_type == 'IMPORT_TAG':
        creation_status = dcu.apply_import_config(config['config_uuid'], tag_extract, \
                                                  config['tag_history'], config['tag_stream'], config['overwrite'])
    if config_type == 'RESTORE_TAG':
        creation_status = dcu.apply_restore_config(config['config_uuid'], tag_extract, \
                                                   config['tag_history'], config['tag_stream'], config['overwrite'])
                                              
    if creation_status == constants.SUCCESS:
        tm.update_task_status(shard_uuid, task_uuid, 'COMPLETED')
    else:
        tm.update_task_status(shard_uuid, task_uuid, 'FAILED')
    
    # fan-in
    is_success, is_failed, pct_complete = jm.calculate_job_completion(job_uuid)
        
    if pct_complete == 100 and is_success:
        teu.update_config_status(config_uuid, config_type, 'ACTIVE')
        teu.update_scheduling_status(config_uuid, config_type, 'READY')
        teu.update_overwrite_flag(config_uuid, config_type)
        resp = jsonify(success=True)
    elif pct_complete == 100 and is_failed:
        teu.update_config_status(config_uuid, config_type, 'ERROR')
        jm.update_job_failed(job_uuid)
        resp = jsonify(success=False)
    else:
        teu.update_config_status(config_uuid, config_type, 'PROCESSING: {}% complete'.format(pct_complete))
        resp = jsonify(success=True)
    
    return resp
#[END _run_task]

####################### VERSION METHOD ####################################  
    
@app.route("/version", methods=['GET'])
def version():
    return "Welcome to Tag Engine version 1.0.8"
#[END ping]
    
####################### TEST METHOD ####################################  
    
@app.route("/ping", methods=['GET'])
def ping():
    return "Tag Engine is alive"
#[END ping]

@app.errorhandler(500)
def server_error(e):
    # Log the error and stacktrace.
    #logging.exception('An error occurred during a request.')
    return 'An internal error occurred: ' + str(e), 500
# [END app]


if __name__ == "__main__":
    app.run()
