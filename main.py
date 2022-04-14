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
import constants

import JobManager as jobm
import TaskManager as taskm

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
    
    exists, settings = teu.read_default_settings()
    
    if exists:
        template_id = settings['template_id']
        project_id = settings['project_id']
        region = settings['region']
    else:
        template_id = "{your_template_id}"
        project_id = "{your_project_id}"
        region = "{your_region}"
    
    # [END default_settings]
    # [START render_template]
    return render_template(
        'index.html',
        template_id=template_id,
        project_id=project_id,
        region=region)

    
@app.route("/default_settings<int:saved>", methods=["GET"])
def default_settings(saved):
    
    exists, settings = teu.read_default_settings()
    
    if exists:
        template_id = settings['template_id']
        project_id = settings['project_id']
        region = settings['region']
    else:
        template_id = "{your_template_id}"
        project_id = "{your_project_id}"
        region = "{your_region}"
    
    # [END default_settings]
    # [START render_template]
    return render_template(
        'default_settings.html',
        template_id=template_id,
        project_id=project_id,
        region=region,
        settings=saved)
    # [END render_template]
         
@app.route("/coverage_settings<int:saved>")
def coverage_settings(saved):
    
    exists, settings = teu.read_coverage_settings()
    
    if exists:
        project_ids = settings['project_ids']
        datasets = settings['excluded_datasets']
        tables = settings['excluded_tables']
    else:
        project_ids = "{projectA}, {projectB}, {projectC}"
        datasets = "{project.dataset1}, {project.dataset2}, {project.dataset3}"
        tables = "{project.dataset.table1}, {project.dataset.table2}, {project.dataset.view3}"
    
    # [END report_settings]
    # [START render_template]
    return render_template(
        'coverage_settings.html',
        project_ids=project_ids,
        datasets=datasets,
        tables=tables,
        settings=saved)
    # [END render_template]
    
@app.route("/tag_history_settings<int:saved>", methods=["GET"])
def tag_history_settings(saved):
    
    enabled, settings = teu.read_tag_history_settings()
    
    if enabled:
        enabled = settings['enabled']
        project_id = settings['project_id']
        region = settings['region']
        dataset = settings['dataset']
    else:
        project_id = "{your_project_id}"
        region = "{your_region}"
        dataset = "{your_dataset}"
    
    # [END tag_history_settings]
    # [START render_template]
    return render_template(
        'tag_history_settings.html',
        enabled=enabled,
        project_id=project_id,
        region=region,
        dataset=dataset,
        settings=saved)
    # [END render_template]

@app.route("/tag_stream_settings<int:saved>", methods=["GET"])
def tag_stream_settings(saved):
    
    enabled, settings = teu.read_tag_stream_settings()
    
    if enabled:
        enabled = settings['enabled']
        project_id = settings['project_id']
        topic = settings['topic']
    else:
        project_id = "{your_project_id}"
        topic = "{your_topic}"
    
    # [END tag_stream_settings]
    # [START render_template]
    return render_template(
        'tag_stream_settings.html',
        enabled=enabled,
        project_id=project_id,
        topic=topic,
        settings=saved)
    # [END render_template]
    
@app.route("/set_default", methods=['POST'])
def set_default():
    
    template_id = request.form['template_id'].rstrip()
    project_id = request.form['project_id'].rstrip()
    region = request.form['region'].rstrip()
    
    if template_id == "{your_template_id}":
        template_id = None
    if project_id == "{your_project_id}":
        project_id = None
    if region == "{your_region}":
        region = None
    
    if template_id != None or project_id != None or region != None:
        teu.write_default_settings(template_id, project_id, region)
        
    return default_settings(1)
        
        
@app.route("/set_tag_history", methods=['POST'])
def set_tag_history():
    
    enabled = request.form['enabled'].rstrip()
    project_id = request.form['project_id'].rstrip()
    region = request.form['region'].rstrip()
    dataset = request.form['dataset'].rstrip()
    
    print("enabled: " + enabled)
    print("project_id: " + project_id)
    print("region: " + region)
    print("dataset: " + dataset)
    
    if enabled == "on":
        enabled = True
    else:
        enabled = False    
    
    if project_id == "{your_project_id}":
        project_id = None
    if region == "{your_region}":
        region = None
    if dataset == "{your_dataset}":
        dataset = None
    
    # can't be enabled if either of the required fields are NULL
    if enabled and (project_id == None or region == None or dataset == None):
        enabled = False
    
    if project_id != None or region != None or dataset != None:
        teu.write_tag_history_settings(enabled, project_id, region, dataset)
        
        return tag_history_settings(1)
    else:
        return tag_history_settings(0)


@app.route("/set_tag_stream", methods=['POST'])
def set_tag_stream():
    
    enabled = request.form['enabled'].rstrip()
    project_id = request.form['project_id'].rstrip()
    topic = request.form['topic'].rstrip()

    print("enabled: " + enabled)
    print("project_id: " + project_id)
    print("topic: " + topic)
    
    if enabled == "on":
        enabled = True
    else:
        enabled = False    
    
    if project_id == "{your_project_id}":
        project_id = None
    if topic == "{your_topic}":
        topic = None
    
    # can't be enabled if either the required fields are NULL
    if enabled and (project_id == None or topic == None):
        enabled = False
    
    if project_id != None or topic != None:
        teu.write_tag_stream_settings(enabled, project_id, topic)
        
        return tag_stream_settings(1)
    else:
        return tag_stream_settings(0)
        
        
@app.route("/set_coverage", methods=['POST'])
def set_coverage():
    
    project_ids = request.form['project_ids'].rstrip()
    datasets = request.form['datasets'].rstrip()
    tables = request.form['tables'].rstrip()
    
    print("project_ids: " + project_ids)
    print("datasets: " + datasets)
    print("tables: " + tables)
    
    if project_ids == "{projectA}, {projectB}, {projectC}":
        project_ids = None
    if datasets == "{project.dataset1}, {project.dataset2}, {project.dataset3}":
        datasets = None
    if tables == "{project.dataset.table1}, {project.dataset.table2}, {project.dataset.view3}":
        tables = None
    
    if project_ids != None or datasets != None or tables != None:
        teu.write_coverage_settings(project_ids, datasets, tables)
        
    return coverage_settings(1)  
     
@app.route("/coverage_report")
def coverage_report():
    
    summary_report, detailed_report = teu.generate_coverage_report()
    
    print('summary_report: ' + str(summary_report))
    print('detailed_report: ' + str(detailed_report))
    
    exists, settings = teu.read_coverage_settings()
    project_ids = settings['project_ids']
    
    return render_template(
        "coverage_report.html",
        project_ids=project_ids,
        report_headers=summary_report,
        report_data=detailed_report)

# TO DO: re-implement this method using the DC API        
@app.route("/coverage_details<string:res>", methods=['GET'])
def coverage_details(res):
    print("res: " + res)
    
    project_id = res.split('.')[0]
    resource = res.split('.')[1]
    
    tag_configs = teu.read_tag_configs_on_res(res)
    
    return render_template(
        'view_tags_on_res.html',
        resource=res,
        project_id=project_id,
        tag_configs=tag_configs)
                
# [START search_template]
@app.route('/search_template', methods=['POST'])
def search_template():

    template_id = request.form['template_id']
    project_id = request.form['project_id']
    region = request.form['region']
    
    dcu = dc.DataCatalogUtils(template_id, project_id, region)
    fields = dcu.get_template()
    
    print("fields: " + str(fields))
    
    # [END search_template]
    # [START render_template]
    return render_template(
        'tag_template.html',
        template_id=template_id,
        project_id=project_id,
        region=region,
        fields=fields)
    # [END render_template]
    
@app.route('/choose_action', methods=['GET'])
def choose_action():
    
    template_id = request.args.get('template_id')
    project_id = request.args.get('project_id')
    region = request.args.get('region')
    
    dcu = dc.DataCatalogUtils(template_id, project_id, region)
    fields = dcu.get_template()
    
    print("fields: " + str(fields))
    
    # [END search_template]
    # [START render_template]
    return render_template(
        'tag_template.html',
        template_id=template_id,
        project_id=project_id,
        region=region,
        fields=fields)
    # [END render_template]

# [START view_configs]
@app.route('/view_configs', methods=['GET'])
def view_configs():
    
    template_id = request.args.get('template_id')
    project_id = request.args.get('project_id')
    region = request.args.get('region')
    
    print("template_id: " + str(template_id))
    print("project_id: " + str(project_id))
    print("region: " + str(region))
    
    dcu = dc.DataCatalogUtils(template_id, project_id, region)
    template_fields = dcu.get_template()
    
    history_enabled, history_settings = teu.read_tag_history_settings()
    stream_enabled, stream_settings = teu.read_tag_stream_settings()
    
    tag_configs = teu.read_tag_configs(template_id, project_id, region)

    return render_template(
        'view_configs.html',
        template_id=template_id,
        project_id=project_id,
        region=region,
        tag_configs=tag_configs)
    # [END render_template]

# [START display_selected_action]
@app.route('/display_selected_action', methods=['POST'])
def display_selected_action():
    template_id = request.form['template_id']
    project_id = request.form['project_id']
    region = request.form['region']
    action = request.form['action']

    print("template_id: " + str(template_id))
    print("project_id: " + str(project_id))
    print("region: " + str(region))
    print("action: " + str(action))
    
    dcu = dc.DataCatalogUtils(template_id, project_id, region)
    template_fields = dcu.get_template()
    
    history_enabled, history_settings = teu.read_tag_history_settings()
    stream_enabled, stream_settings = teu.read_tag_stream_settings()
    
    if action == "View and Edit Configs":

        tag_configs = teu.read_tag_configs(template_id, project_id, region)

        return render_template(
            'view_configs.html',
            template_id=template_id,
            project_id=project_id,
            region=region,
            tag_configs=tag_configs)
        
    elif action == "Create Static Config":
        return render_template(
            'static_config.html',
            template_id=template_id,
            project_id=project_id,
            region=region,
            fields=template_fields,
            current_time=datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
            display_tag_history=history_enabled,
            display_tag_stream=stream_enabled)
            
    elif action == "Create Dynamic Config":
        return render_template(
            'dynamic_config.html',
            template_id=template_id,
            project_id=project_id,
            region=region,
            fields=template_fields,
            display_tag_history=history_enabled,
            display_tag_stream=stream_enabled)
            
    else: 
        # this option is currently hidden as tag propagation is in alpha
        return render_template(
            'propagation_tag.html',
            template_id=template_id,
            project_id=project_id,
            region=region,
            fields=template_fields,
            display_tag_history=history_enabled,
            display_tag_stream=stream_enabled)
            
    # [END render_template]

@app.route('/update_config', methods=['POST'])
def update_config():
    template_id = request.form['template_id']
    project_id = request.form['project_id']
    region = request.form['region']
    tag_uuid = request.form['tag_uuid']
    config_type = request.form['config_type']
    
    print("template_id: " + str(template_id))
    print("project_id: " + str(project_id))
    print("region: " + str(region))
    print("tag_uuid: " + str(tag_uuid))
    print("config_type: " + str(config_type))
    
    tag_config = teu.read_tag_config(tag_uuid)
    print("tag_config: " + str(tag_config))
    
    dcu = dc.DataCatalogUtils(template_id, project_id, region)
    template_fields = dcu.get_template()
    print("fields: " + str(template_fields))
    
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

    
    if config_type == "STATIC":
        # [END update_tag]
        # [START render_template]
        return render_template(
            'update_static_config.html',
            template_id=template_id,
            project_id=project_id,
            region=region,
            fields=template_fields,
            tag_config=tag_config, 
            current_time=datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
            display_tag_history_option=tag_history,
            display_tag_stream_option=tag_stream)
    else:
        # [END display_action]
        # [START render_template]
        return render_template(
            'update_dynamic_config.html',
            template_id=template_id,
            project_id=project_id,
            region=region,
            fields=template_fields,
            tag_config=tag_config,
            display_tag_history_option=tag_history,
            display_tag_stream_option=tag_stream)
    # [END render_template]
    

@app.route('/process_update_static_config', methods=['POST'])
def process_update_static_config():
    
    template_id = request.form['template_id']
    project_id = request.form['project_id']
    region = request.form['region']
    old_tag_uuid = request.form['tag_uuid']
    included_uris = request.form['included_uris'].rstrip()
    excluded_uris = request.form['excluded_uris'].rstrip()
    action = request.form['action']
    
    job_creation = constants.SUCCESS
    
    #print("action: " + str(action))

    dcu = dc.DataCatalogUtils(template_id, project_id, region)
    template_fields = dcu.get_template()
    
    if action == "Submit Tag":
        
        fields = []
    
        selected_fields = request.form.getlist("selected")
        #print("selected_fields: " + str(selected_fields))
    
        for selected_field in selected_fields:
            selected_value = request.form.get(selected_field)
            selected_type = request.form.get(selected_field + "_datatype")
            print(selected_field + ", " + selected_value + ", " + selected_type)
            
            for template_field in template_fields:
                if template_field['field_id'] != selected_field:
                    continue
                
                is_required = template_field['is_required']
                field = {'field_id': selected_field, 'field_value': selected_value, 'field_type': selected_type, 'is_required': is_required}
                fields.append(field)
                break
    
        #print('fields: ' + str(fields))
        
        tag_history = False
    
        if "tag_history" in request.form:
            tag_history_option = request.form.get("tag_history")
    
            if tag_history_option == "selected":
                tag_history = True
        
        tag_stream = False
                
        if "tag_stream" in request.form:
            tag_stream_option = request.form.get("tag_stream")
    
            if tag_stream_option == "selected":
                tag_stream = True
    
        template_exists, template_uuid = teu.read_tag_template(template_id, project_id, region)
        new_tag_uuid = teu.update_tag_config(old_tag_uuid, 'STATIC', 'PENDING', fields, included_uris, excluded_uris,\
                                                 template_uuid, None, None, None, tag_history, tag_stream)
        
        #print('new_tag_uuid: ' + new_tag_uuid)
        
        if isinstance(new_tag_uuid, str): 
            job_uuid = jm.create_job(new_tag_uuid)
        else:
            job_uuid = None
	
        if job_uuid == None: 
            job_creation = constants.ERROR
         
    template_fields = dcu.get_template()  
    tag_configs = teu.read_tag_configs(template_id, project_id, region)
    
    print('template_fields: ' + str(template_fields))
      
     # [END process_update_static_tag]
     # [START render_template]
    return render_template(
         'view_configs.html',
         template_id=template_id,
         project_id=project_id,
         region=region,
         fields=template_fields,
         tag_configs=tag_configs,
         status=job_creation)  


@app.route('/process_update_dynamic_config', methods=['POST'])
def process_update_dynamic_config():
    template_id = request.form['template_id']
    project_id = request.form['project_id']
    region = request.form['region']
    old_tag_uuid = request.form['tag_uuid']
    included_uris = request.form['included_uris'].rstrip()
    excluded_uris = request.form['excluded_uris'].rstrip()
    refresh_mode = request.form['refresh_mode']
    refresh_frequency = request.form['refresh_frequency'].rstrip()
    refresh_unit = request.form['refresh_unit']
    action = request.form['action']
    
    job_creation = constants.SUCCESS
    
    #print('old_tag_uuid: ' + old_tag_uuid)
    #print("action: " + str(action))
    
    dcu = dc.DataCatalogUtils(template_id, project_id, region)
    template_fields = dcu.get_template()
    
    if action == "Submit Tag":
        
        fields = []
    
        selected_fields = request.form.getlist("selected")
        print("selected_fields: " + str(selected_fields))
    
        for selected_field in selected_fields:
            query_expression = request.form.get(selected_field)
            print("query_expression: " + query_expression)
            selected_field_type = request.form.get(selected_field + "_datatype")
            print(selected_field + ", " + query_expression + ", " + selected_field_type)
            
            for template_field in template_fields:
                if template_field['field_id'] != selected_field:
                    continue
                
                is_required = template_field['is_required']
        
                field = {'field_id': selected_field, 'query_expression': query_expression, 'field_type': selected_field_type,\
                         'is_required': is_required}
                fields.append(field)
                break
    
        print('fields: ' + str(fields))
        
        tag_history = False
    
        if "tag_history" in request.form:
            tag_history_option = request.form.get("tag_history")
    
            if tag_history_option == "selected":
                tag_history = True
        
        tag_stream = False
                
        if "tag_stream" in request.form:
            tag_stream_option = request.form.get("tag_stream")
    
            if tag_stream_option == "selected":
                tag_stream = True
    
        template_exists, template_uuid = teu.read_tag_template(template_id, project_id, region)
        new_tag_uuid = teu.update_tag_config(old_tag_uuid, 'DYNAMIC', 'PENDING', fields, included_uris, excluded_uris,\
                                                  template_uuid, refresh_mode, refresh_frequency, refresh_unit, \
                                                  tag_history, tag_stream)
        
        job_uuid = jm.create_job(new_tag_uuid)
    
        if job_uuid == None: 
            job_creation = constants.ERROR
             
    template_fields = dcu.get_template()  
    print('template_fields: ' + str(template_fields))
    
    tag_configs = teu.read_tag_configs(template_id, project_id, region)
      
     # [END process_update_dynamic_config]
     # [START render_template]
    return render_template(
         'view_configs.html',
         template_id=template_id,
         project_id=project_id,
         region=region,
         fields=template_fields,
         tag_configs=tag_configs,
         status=job_creation)  


@app.route('/process_static_config', methods=['POST'])
def process_static_config():
    template_id = request.form['template_id']
    project_id = request.form['project_id']
    region = request.form['region']
    included_uris = request.form['included_uris'].rstrip()
    excluded_uris = request.form['excluded_uris'].rstrip()
    action = request.form['action']
    
    print('included_uris: ' + included_uris)
    print('excluded_uris: ' + excluded_uris)
    
    dcu = dc.DataCatalogUtils(template_id, project_id, region)
    template = dcu.get_template()
    
    if action == "Cancel Changes":
        
        return render_template(
            'tag_template.html',
            template_id=template_id,
            project_id=project_id,
            region=region, 
            fields=template)
    
    fields = []
    
    selected_fields = request.form.getlist("selected")
    print("selected_fields: " + str(selected_fields))
    
    for selected_field in selected_fields:
        selected_value = request.form.get(selected_field)
        selected_type = request.form.get(selected_field + "_datatype")
        print(selected_field + ", " + selected_value + ", " + selected_type)
        
        for template_field in template:
            
            if template_field['field_id'] != selected_field:
                continue
            
            is_required = template_field['is_required']
            field = {'field_id': selected_field, 'field_value': selected_value, 'field_type': selected_type, 'is_required': is_required}
            fields.append(field)
            break
    
    print('fields: ' + str(fields))
    
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
        
    template_uuid = teu.write_tag_template(template_id, project_id, region)
    tag_uuid, included_uris_hash = teu.write_static_config('PENDING', fields, included_uris, excluded_uris, template_uuid,\
                                                             tag_history_option, tag_stream_option)
    
    if isinstance(tag_uuid, str):
        job_uuid = jm.create_job(tag_uuid)
    
    if job_uuid != None: 
        job_creation = constants.SUCCESS
    else:
        job_creation = constants.ERROR
            
    # [END process_static_tag]
    # [START render_template]
    return render_template(
        'submitted_static_config.html',
        template_id=template_id,
        project_id=project_id,
        region=region,
        fields=fields,
        included_uris=included_uris,
        excluded_uris=excluded_uris,
        tag_history=tag_history_enabled,
        tag_stream=tag_stream_enabled,
        status=job_creation)
    # [END render_template]


@app.route('/process_dynamic_config', methods=['POST'])
def process_dynamic_config():
    template_id = request.form['template_id']
    project_id = request.form['project_id']
    region = request.form['region']
    included_uris = request.form['included_uris'].rstrip()
    excluded_uris = request.form['excluded_uris'].rstrip()
    refresh_mode = request.form['refresh_mode']
    refresh_frequency = request.form['refresh_frequency']
    refresh_unit = request.form['refresh_unit']
    action = request.form['action']
    
    #print('included_uris: ' + included_uris)
    #print('excluded_uris: ' + excluded_uris)
    #print('refresh_mode: ' + refresh_mode)
    #print('refresh_frequency: ' + refresh_frequency)
    #print('refresh_unit: ' + refresh_unit)
    
    dcu = dc.DataCatalogUtils(template_id, project_id, region)
    template = dcu.get_template()
    
    if action == "Cancel Changes":
        
        return render_template(
            'tag_template.html',
            template_id=template_id,
            project_id=project_id,
            region=region, 
            fields=template)

    fields = []
    
    selected_fields = request.form.getlist("selected")
    #print("selected_fields: " + str(selected_fields))
    
    for selected_field in selected_fields:
        query_expression = request.form.get(selected_field)
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
    
    template_uuid = teu.write_tag_template(template_id, project_id, region)
    tag_uuid, included_uris_hash = teu.write_dynamic_config('PENDING', fields, included_uris, excluded_uris, template_uuid,\
                                                             refresh_mode, refresh_frequency, refresh_unit, \
                                                             tag_history_option, tag_stream_option)
    if isinstance(tag_uuid, str): 
        job_uuid = jm.create_job(tag_uuid)
    else:
        job_uuid = None
    
    if job_uuid != None: 
        job_creation = constants.SUCCESS
    else:
        job_creation = constants.ERROR
     
    # [END process_dynamic_tag]
    # [START render_template]
    return render_template(
        'submitted_dynamic_config.html',
        template_id=template_id,
        project_id=project_id,
        region=region,
        fields=fields,
        included_uris=included_uris,
        included_uris_hash=included_uris_hash,
        excluded_uris=excluded_uris,
        refresh_mode=refresh_mode,
        refresh_frequency=refresh_frequency,
        refresh_unit=refresh_unit,
        tag_history=tag_history_enabled,
        tag_stream=tag_stream_enabled,
        status=job_creation)
    # [END render_template]


##################### API METHODS #################

"""
Args:
    template_id: tag template to use
    project_id: tag template's Google Cloud project 
    region: tag template's region 
    included_uris: The included_uris value
    excluded_uris: The excluded_uris value (optional)
    fields: list of selected fields containing field name, field type and query expression
    refresh_mode: AUTO or ON_DEMAND
    refresh_frequency: positive integer
    refresh_unit: minutes or hours
    tag_history: true if tag history is on, false otherwise 
    tag_stream: true if tag stream is on, false otherwise
Returns:
    job_uuid 
"""
@app.route("/dynamic_create", methods=['POST'])
def dynamic_create():
    json = request.get_json(force=True) 
    print('json: ' + str(json))
       
    template_id = json['template_id']
    project_id = json['project_id']
    region = json['region']
    
    print('template_id: ' + template_id)
    print('project_id: ' + project_id)
    print('region: ' + region)
    
    template_uuid = teu.write_tag_template(template_id, project_id, region)
    
    fields = json['fields']
    excluded_uris = json['excluded_uris']
    included_uris = json['included_uris']
    refresh_mode = json['refresh_mode']
    
    if 'refresh_frequency' in json:
        refresh_frequency = json['refresh_frequency']
    else:
        refresh_frequency = ''
    
    if 'refresh_unit' in json:
        refresh_unit = json['refresh_unit']
    else:
        refresh_unit = ''
    
    tag_history = json['tag_history']
    tag_stream = json['tag_stream']
    
    tag_uuid, included_uris_hash = teu.write_dynamic_config('PENDING', fields, included_uris, excluded_uris, template_uuid,\
                                                             refresh_mode, refresh_frequency, refresh_unit, \
                                                             tag_history, tag_stream)                                                      

    if isinstance(tag_uuid, str): 
        job_uuid = jm.create_job(tag_uuid)
    else:
        job_uuid = None
    
    return jsonify(job_uuid=job_uuid)
    
        
"""
Args:
    template_id: tag template to use
    project_id: tag template's Google Cloud project 
    region: tag template's region 
    included_uris: The included_uris value
    excluded_uris: The excluded_uris value (optional)
    fields: list of selected fields containing field name, field type and query expression
    tag_history: true if tag history is on, false otherwise 
    tag_stream: true if tag stream is on, false otherwise
Returns:
    job_uuid
"""
@app.route("/static_create", methods=['POST'])
def static_create():
    json = request.get_json(force=True) 
    print('json: ' + str(json))
       
    template_id = json['template_id']
    project_id = json['project_id']
    region = json['region']
    
    print('template_id: ' + template_id)
    print('project_id: ' + project_id)
    print('region: ' + region)
    
    template_uuid = teu.write_tag_template(template_id, project_id, region)
    
    fields = json['fields']
    excluded_uris = json['excluded_uris']
    included_uris = json['included_uris']
    tag_history = json['tag_history']
    tag_stream = json['tag_stream']
    
    tag_uuid, included_uris_hash = teu.write_static_config('PENDING', fields, included_uris, excluded_uris, template_uuid,\
                                                             tag_history, tag_stream)
     
    if isinstance(tag_uuid, str): 
        job_uuid = jm.create_job(tag_uuid)
    else:
        job_uuid = None
    
    return jsonify(job_uuid=job_uuid)
    

"""
Args:
    template_id: tag template to use
    project_id: tag template's Google Cloud project 
    region: tag template's region 
    included_uris: tag config's included uris or
    included_uris_hash: tag config's md5 hash value (in place of the included_uris)
Note: caller must provide either the included_uris_hash or included_uris
Returns:
    job_uuid = unique identifer for job
"""
@app.route("/dynamic_ondemand_update", methods=['POST'])
def dynamic_ondemand_update():
    json = request.get_json(force=True)    
    template_id = json['template_id']
    project_id = json['project_id']
    region = json['region']
        
    template_exists, template_uuid = teu.read_tag_template(template_id, project_id, region)
    
    if not template_exists:
        print("tag_template " + template_id + " doesn't exist")
        resp = jsonify(success=False)
        return resp
    
    if 'included_uris' in json:
       included_uris = json['included_uris']
       success, tag_config = teu.lookup_tag_config_by_included_uris(template_uuid, included_uris, None)
          
    elif 'included_uris_hash' in json:
        included_uris_hash = json['included_uris_hash']
        success, tag_config = teu.lookup_tag_config_by_included_uris(template_uuid, None, included_uris_hash)
    
    else:
        resp = jsonify(success=False, message="Request is missing required parameter included_uris or included_uris_hash.")
        return resp
    
    if success != True:
        print("tag config not found " + str(tag_config))
        resp = jsonify(success=False, message="Tag config not found.")
        return resp
    
    # process the update request
    if tag_config['refresh_mode'] == 'AUTO':
        print("tag config == AUTO" + str(tag_config))
        resp = jsonify(success=False, message="Tag config has refresh_mode='AUTO'. Update config to refresh_mode='ON-DEMAND' prior to calling this method.")
        return resp
    
    if isinstance(tag_config['tag_uuid'], str): 
        job_uuid = jm.create_job(tag_config['tag_uuid'])
    else:
        job_uuid = None
    
    return jsonify(job_uuid=job_uuid)
    
    #[END dynamic_ondemand_update]
   

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
    job_uuid = json['job_uuid']
    
    job = jm.get_job_status(job_uuid)
    
    if job is None:
        return jsonify(success=False, message="job_uuid " + job_uuid + " cannot be found.")
    else:
        return jsonify(success=True, job_status=job['job_status'], task_count=job['task_count'], tasks_ran=job['tasks_ran'],\
                       tasks_completed=job['tasks_completed'], tasks_failed=job['tasks_failed'])
    

"""
Method called by Cloud Scheduler entry to update the tags from all active dynamic configs which are set to auto
Args:
    None
Returns:
    True if the request succeeded, False otherwise
""" 
@app.route("/dynamic_auto_update", methods=['POST'])
def dynamic_auto_update():
    
    try:    
        print('*** enter dynamic_auto_update ***')
        
        jobs = []
        
        configs = teu.read_ready_configs()
        
        for tag_uuid in configs:
        
            if isinstance(tag_uuid, str): 
                
                teu.update_config_status(tag_uuid, 'PENDING')
                teu.increment_version_next_run(tag_uuid)
                job_uuid = jm.create_job(tag_uuid)
                jobs.append(job_uuid)

        print('created jobs: ' + str(jobs))
        resp = jsonify(success=True, job_ids=json.dumps(jobs))
    
    except Exception as e:
        print('failed dynamic_auto_update {}'.format(e))
        resp = jsonify(success=False, message='failed dynamic_auto_update ' + str(e))
    
    return resp


################ INTERNAL PROCESSING METHODS #################

@app.route("/_split_work", methods=['POST'])
def _split_work():
    
    print('*** enter _split_work ***')
    
    json = request.get_json(force=True)
    job_uuid = json['job_uuid']
    tag_uuid = json['tag_uuid']
    
    config = teu.read_tag_config(tag_uuid)
    uris = list(res.Resources.get_resources(config['included_uris'], config['excluded_uris']))
    #print('uris: ' + str(uris))

    jm.record_num_tasks(job_uuid, len(uris))
    jm.update_job_running(job_uuid) 
    tm.create_work_tasks(job_uuid, tag_uuid, uris)
    teu.update_config_status(tag_uuid, 'RUNNING')
    
    resp = jsonify(success=True)
    return resp
    

@app.route("/_run_task", methods=['POST'])
def _run_task():
    
    print('*** enter _run_task ***')
    
    json = request.get_json(force=True)
    job_uuid = json['job_uuid']
    tag_uuid = json['tag_uuid']
    uri = json['uri']
    shard_uuid = json['shard_uuid']
    task_uuid = json['task_uuid']
    
    #print('task_uuid: ' + task_uuid)
    
    tm.update_task_status(shard_uuid, task_uuid, 'RUNNING')
    
    # retrieve tag config and template
    tag_config = teu.read_tag_config(tag_uuid)
    tem_config = teu.read_template_config(tag_config['template_uuid'])
    
    dcu = dc.DataCatalogUtils(tem_config['template_id'], tem_config['project_id'], tem_config['region'])
    
    creation_status = constants.ERROR
    
    if tag_config['config_type'] == 'DYNAMIC':

        creation_status = dcu.create_update_dynamic_configs(tag_config['fields'], None, None, uri, tag_config['tag_uuid'], \
                                                         tag_config['template_uuid'], tag_config['tag_history'], \
                                                         tag_config['tag_stream'])
    if tag_config['config_type'] == 'STATIC':
        
        creation_status = dcu.create_update_static_configs(tag_config['fields'], None, None, uri, tag_config['tag_uuid'], \
                                                        tag_config['template_uuid'], tag_config['tag_history'], \
                                                        tag_config['tag_stream'])
    if creation_status == constants.SUCCESS:
        tm.update_task_status(shard_uuid, task_uuid, 'COMPLETED')
        is_success, is_failed, pct_complete = jm.calculate_job_completion(job_uuid)
        
        if is_success:
            teu.update_config_status(tag_uuid, 'ACTIVE')
            teu.update_scheduling_status(tag_uuid, 'READY')
        elif is_failed:
            teu.update_config_status(tag_uuid, 'ERROR')
        else:
            teu.update_config_status(tag_uuid, 'PROCESSING: {}% complete'.format(pct_complete))
        
        resp = jsonify(success=True)
    else:
        tm.update_task_status(shard_uuid, task_uuid, 'FAILED')
        jm.update_job_failed(job_uuid)
        teu.update_config_status(tag_uuid, 'ERROR')
        resp = jsonify(success=False)
    
    return resp
#[END _run_task]
    
  
################ TAG PROPAGATION METHODS #################
 
@app.route("/propagation_settings<int:saved>")
def propagation_settings(saved):
    
    exists, settings = teu.read_propagation_settings()
    
    if exists:
        source_project_ids = settings['source_project_ids']
        dest_project_ids = settings['dest_project_ids']
        excluded_datasets = settings['excluded_datasets']
        job_frequency = settings['job_frequency']
    else:
        source_project_ids = "{projectA}, {projectB}, {projectC}"
        dest_project_ids = "{projectD}, {projectE}, {projectF}"
        excluded_datasets = "{projectA}.{dataset1}, {projectB}.{dataset2}"
        job_frequency = "24"
    
    # [END propagation_settings]
    # [START render_template]
    return render_template(
        'propagation_settings.html',
        source_project_ids=source_project_ids,
        dest_project_ids=dest_project_ids,
        excluded_datasets=excluded_datasets,
        job_frequency=job_frequency,
        settings=saved)
    # [END render_template]
  
    
@app.route("/set_propagation", methods=['POST'])
def set_propagation():
    
    source_project_ids = request.form['source_project_ids'].rstrip()
    dest_project_ids = request.form['dest_project_ids'].rstrip()
    excluded_datasets = request.form['excluded_datasets'].rstrip()
    job_frequency = request.form['job_frequency'].rstrip()
    
    if source_project_ids == "{projectA}, {projectB}, {projectC}":
        source_project_ids = None
    if dest_project_ids == "{projectD}, {projectE}, {projectF}":
        dest_project_ids = None
    if excluded_datasets == "{projectA}.{dataset1}, {projectB}.{dataset2}":
        dest_project_ids = None
    
    if source_project_ids != None or dest_project_ids != None:
        teu.write_propagation_settings(source_project_ids, dest_project_ids, excluded_datasets, job_frequency)
        
    return propagation_settings(1)  
  
@app.route("/propagation_report", methods=['GET', 'POST'])
def propagation_report():
    
    exists, settings = teu.read_propagation_settings()
    method = request.method
    
    if method == 'POST':
       run_propagation() 
    
    if exists == True:
        source_project_ids = settings['source_project_ids']
        dest_project_ids = settings['dest_project_ids']
        excluded_datasets = settings['excluded_datasets']
        project_ids = source_project_ids
        
        project_list = dest_project_ids.split(",")
        for dest_project in project_list:
            if dest_project not in project_ids:
                project_ids = project_ids + ", " + dest_project
        
        report_data, last_run = teu.generate_propagation_report() 
        
        if last_run is not None:
            last_run = last_run.strftime('%Y-%m-%d %H:%M:%S')
            #print('last_run: ' + str(last_run))
        else:
            last_run = 'Never'
            
        return render_template(
           "propagation_report.html",
            project_ids=project_ids,
            report_data=report_data, 
            last_run=last_run)
    else:
        
        return render_template(
           "propagation_settings.html")


@app.route("/run_propagation", methods=['POST'])
def run_propagation():
    
    exists, settings = teu.read_propagation_settings()
    
    if exists == True:
        source_project_ids = settings['source_project_ids']
        dest_project_ids = settings['dest_project_ids']
        excluded_datasets = settings['excluded_datasets']
        
        teu.run_propagation_job(source_project_ids, dest_project_ids, excluded_datasets) 
        
        resp = jsonify(success=True)
        
    else:
        resp = jsonify(success=False)
    
    return resp
     
        
@app.route("/propagated_details", methods=['POST'])
def propagated_details():
    
    template_uuid = request.form['template_uuid']
    view_tag_uuid = request.form['view_tag_uuid']
    source_res = request.form['source_res']
    view_res = request.form['view_res']
    
    print("template_uuid: " + template_uuid)
    print("view_tag_uuid: " + view_tag_uuid)

    propagated_tag_config = teu.read_propagated_config(view_tag_uuid)
    template_config = teu.read_template_config(template_uuid)
    
    source_res_list = propagated_tag_config['source_res']
    source_res_full = ','.join(source_res_list)
    
    view_res_full = propagated_tag_config['view_res']
    
    # construct included_uris from propagated_tag_config
    if 'cols' in propagated_tag_config.keys():
        included_uris = ""
        for col in propagated_tag_config['cols']:
            if col != "":
                included_uris = included_uris + view_res + "/" + col + ", "
            else:
                included_uris = included_uris + view_res + ", "
    
        included_uris = included_uris[0:-2]
        print("included_uris: " + included_uris)
    else:
        included_uris = 'bigquery/project/' + propagated_tag_config['view_res']
    print("included_uris: " + included_uris)
    
    return render_template(
        'view_propagated_tag_on_res.html',
        source_res_full=source_res_full,
        view_res_full=view_res_full,
        template_id=template_config['template_id'],
        propagated_tag_config=propagated_tag_config, 
        included_uris=included_uris)

@app.route('/update_propagated_tag', methods=['POST']) 
def update_propagated_tag():
    template_uuid = request.form['template_uuid']
    tag_uuid = request.form['tag_uuid']
    config_type = request.form['config_type']
    
    print("template_uuid: " + str(template_uuid))
    print("tag_uuid: " + str(tag_uuid))
    print("config_type: " + str(config_type))
    
    propagated_tag_config = teu.read_propagated_tag_config(tag_uuid)
    print("propagated_tag_config: " + str(propagated_tag_config))
    
    view_res = propagated_tag_config['view_res'].replace('/datasets', '').replace('/tables', '')
    source_res_list = propagated_tag_config['source_res']
    source_res = ','.join(source_res_list)
    source_res = source_res.replace('/datasets', '').replace('/tables', '')
    
    # construct included_uris from propagated_tag_config
    if 'cols' in propagated_tag_config.keys():
        included_uris = ""
        for col in propagated_tag_config['cols']:
            if col != "":
                included_uris = included_uris + view_res + "/" + col + ", "
            else:
                included_uris = included_uris + view_res + ", "
    
        included_uris = included_uris[0:-2]
        print("included_uris: " + included_uris)
    else:
        included_uris = 'bigquery/project/' + propagated_tag_config['view_res'] 
    
    template_config = teu.read_template_config(template_uuid)
    template_id = template_config['template_id']
    project_id = template_config['project_id']
    region = template_config['region']
    dcu = dc.DataCatalogUtils(template_id, project_id, region)
    template_fields = dcu.get_template()
    print("fields: " + str(template_fields))    
        
    if config_type == "STATIC":
        # [END update_tag]
        # [START render_template]
        return render_template(
            'override_static_config.html',
            template_id=template_id,
            project_id=project_id,
            region=region,
            view_res=view_res,
            source_res=source_res,
            fields=template_fields,
            propagated_tag_config=propagated_tag_config, 
            included_uris=included_uris,
            current_time=datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'))
    else:
        # [END display_action]
        # [START render_template]
        return render_template(
            'override_dynamic_config.html',
            template_id=template_id,
            project_id=project_id,
            region=region,
            view_res=view_res,
            source_res=source_res,
            fields=template_fields,
            propagated_tag_config=propagated_tag_config,
            included_uris=included_uris)
    # [END render_template]


@app.route('/override_propagated_dynamic_tag', methods=['POST'])
def override_propagated_dynamic_tag():
    template_id = request.form['template_id']
    project_id = request.form['project_id']
    region = request.form['region']
    tag_uuid = request.form['tag_uuid']
    included_uris = request.form['included_uris'].rstrip()
    refresh_frequency = request.form['refresh_frequency'].rstrip()
    action = request.form['action']
    
    print('tag_uuid: ' + tag_uuid)
    print("action: " + str(action))
    
    dcu = dc.DataCatalogUtils(template_id, project_id, region)
    template_fields = dcu.get_template()
    
    if action != "Cancel Changes":
        
        fields = []
    
        selected_fields = request.form.getlist("selected")
        #print("selected_fields: " + str(selected_fields))
    
        for selected_field in selected_fields:
            query_expression = request.form.get(selected_field)
            #print("query_expression: " + query_expression)
            selected_field_type = request.form.get(selected_field + "_datatype")
            print(selected_field + ", " + query_expression + ", " + selected_field_type)
            
            for template_field in template_fields:
                if template_field['field_id'] != selected_field:
                    continue
                
                is_required = template_field['is_required']
        
                field = {'field_id': selected_field, 'query_expression': query_expression, 'field_type': selected_field_type, 'is_required': is_required}
                fields.append(field)
                break
    
        #print('fields: ' + str(fields))
    
        template_exists, template_uuid = teu.read_tag_template(template_id, project_id, region)
        
        # TO DO: process included_uris changes
        # for now, assume columns is empty
        columns = []
        
        if action == 'Fork Tag and Save Changes':
            config_status = 'CONFLICT AND FORKED'
        else:
            config_status = 'PROPAGATED AND FORKED'
        
        propagated_tag_config = teu.fork_propagated_tag(tag_uuid, config_status, fields, refresh_frequency)
        
        source_res = propagated_tag_config['source_res']
        view_res = propagated_tag_config['view_res']
        fields = propagated_tag_config['fields']
        source_tag_uuid = propagated_tag_config['source_tag_uuid']
        view_tag_uuid = propagated_tag_config['view_tag_uuid']
        template_uuid = propagated_tag_config['template_uuid']
        
        print('source_res: ' + str(source_res))
        print('view_res: ' + view_res)
        print('fields: ' + str(fields))
        print('source_tag_uuid: ' + str(source_tag_uuid))
        print('view_tag_uuid: ' + view_tag_uuid)
        print('template_uuid: ' + template_uuid)
    
        update_status = dcu.create_update_dynamic_propagated_tag(config_status, source_res, view_res, columns, fields, source_tag_uuid, view_tag_uuid, template_uuid)
    
        if update_status == constants.SUCCESS:
            print('override_propagated_dynamic_tags SUCCEEDED.')
        else:
            print('override_propagated_dynamic_tags FAILED.')
             
    else:
    
        propagated_tag_config = teu.read_propagated_config(tag_uuid)
    
    view_res = propagated_tag_config['view_res'].replace('/datasets', '').replace('/tables', '')
    included_uris = 'bigquery/project/' + propagated_tag_config['view_res']
    
    source_res_list = propagated_tag_config['source_res']
    source_res_full = ','.join(source_res_list)
    source_res_full = source_res_full.replace('/datasets', '').replace('/tables', '') 
      
     # [END override_propagated_dynamic_tag]
     # [START render_template]
    return render_template(
        'view_propagated_tag_on_res.html',
        source_res_full=source_res_full,
        view_res=view_res,
        included_uris=included_uris,
        template_id=template_id,
        propagated_tag_config=propagated_tag_config) 


@app.route('/override_propagated_static_tag', methods=['POST'])
def override_propagated_static_tag():
    template_id = request.form['template_id']
    project_id = request.form['project_id']
    region = request.form['region']
    tag_uuid = request.form['tag_uuid']
    included_uris = request.form['included_uris'].rstrip()
    action = request.form['action']
    
    print('tag_uuid: ' + tag_uuid)
    print("action: " + str(action))
    
    dcu = dc.DataCatalogUtils(template_id, project_id, region)
    template_fields = dcu.get_template()
    
    if action != "Cancel Changes": 
        
        fields = []
    
        selected_fields = request.form.getlist("selected")
        print("selected_fields: " + str(selected_fields))
    
        for selected_field in selected_fields:
            selected_value = request.form.get(selected_field)
            selected_type = request.form.get(selected_field + "_datatype")
            print(selected_field + ", " + selected_value + ", " + selected_type)
            
            for template_field in template_fields:
                if template_field['field_id'] != selected_field:
                    continue
                
                is_required = template_field['is_required']
        
                field = {'field_id': selected_field, 'field_value': selected_value, 'field_type': selected_type, 'is_required': is_required}
                fields.append(field)
                break
    
        #print('fields: ' + str(fields))
    
        template_exists, template_uuid = teu.read_tag_template(template_id, project_id, region)
        
        # TO DO: process included_uris changes (compare values to cols)
        # for now assume that columns is empty
        columns = []
        
        if action == 'Fork Tag and Save Changes':
            config_status = 'CONFLICT AND FORKED'
        else:
            config_status = 'PROPAGATED AND FORKED'
        
        propagated_tag_config = teu.fork_propagated_tag(tag_uuid, config_status, fields, refresh_frequency=None)
        
        source_res = propagated_tag_config['source_res']
        view_res = propagated_tag_config['view_res']
        fields = propagated_tag_config['fields']
        source_tag_uuid = propagated_tag_config['source_tag_uuid']
        view_tag_uuid = propagated_tag_config['view_tag_uuid']
        template_uuid = propagated_tag_config['template_uuid']
        
        update_status = dcu.create_update_static_propagated_tag(config_status, source_res, view_res, columns, fields, source_tag_uuid, view_tag_uuid, template_uuid)
    
        if update_status == constants.SUCCESS:
            print('override_propagated_static_tags SUCCEEDED.')
        else:
            print('override_propagated_static_tags FAILED.')
             
    else:
    
        propagated_tag_config = teu.read_propagated_config(tag_uuid)
    
    
    view_res = propagated_tag_config['view_res'].replace('/datasets', '').replace('/tables', '')
    included_uris = 'bigquery/project/' + propagated_tag_config['view_res']
    
    source_res_list = propagated_tag_config['source_res']
    source_res_full = ','.join(source_res_list)
    source_res_full = source_res_full.replace('/datasets', '').replace('/tables', '')
    
     # [END override_propagated_static_tag]
     # [START render_template]
    return render_template(
        'view_propagated_tag_on_res.html',
        source_res_full=source_res_full,
        view_res=view_res,
        included_uris=included_uris,
        template_id=template_id,
        propagated_tag_config=propagated_tag_config) 


###########################################################  
    
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
