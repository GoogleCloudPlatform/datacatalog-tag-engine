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

from flask import Flask, render_template, request, redirect, url_for, jsonify
import datetime, configparser
from google.cloud import firestore
import DataCatalogUtils as dc
import TagEngineUtils as te
import Resources as res
import constants

import TagScheduler as sched
import JobManager as jobm
import TaskManager as taskm

from google.cloud import tasks_v2
from google.protobuf import timestamp_pb2
import datetime, time

config = configparser.ConfigParser()
config.read("tagengine.ini")

app = Flask(__name__)
teu = te.TagEngineUtils()

# handles scheduled update requests (i.e. config contains refresh_mode = AUTO)
ts = sched.TagScheduler(config['DEFAULT']['TAG_ENGINE_PROJECT'], config['DEFAULT']['QUEUE_REGION'], config['DEFAULT']['WORK_QUEUE'], "/_dynamic_auto_update")

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
        
        # redirect to propagation settings 
        
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
    tag_type = request.form['tag_type']
    
    print("template_uuid: " + str(template_uuid))
    print("tag_uuid: " + str(tag_uuid))
    print("tag_type: " + str(tag_type))
    
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
        
    if tag_type == "STATIC":
        # [END update_tag]
        # [START render_template]
        return render_template(
            'override_static_tag.html',
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
            'override_dynamic_tag.html',
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
    
    if action == "View and Edit Tags":

        tag_configs = teu.read_tag_configs(template_id, project_id, region)

        return render_template(
            'view_tags.html',
            template_id=template_id,
            project_id=project_id,
            region=region,
            tag_configs=tag_configs)
        
    elif action == "Create Static Tag":
        return render_template(
            'static_tag.html',
            template_id=template_id,
            project_id=project_id,
            region=region,
            fields=template_fields,
            current_time=datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
            display_tag_history=history_enabled,
            display_tag_stream=stream_enabled)
            
    elif action == "Create Dynamic Tag":
        return render_template(
            'dynamic_tag.html',
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

@app.route('/update_tag', methods=['POST'])
def update_tag():
    template_id = request.form['template_id']
    project_id = request.form['project_id']
    region = request.form['region']
    tag_uuid = request.form['tag_uuid']
    tag_type = request.form['tag_type']
    
    print("template_id: " + str(template_id))
    print("project_id: " + str(project_id))
    print("region: " + str(region))
    print("tag_uuid: " + str(tag_uuid))
    print("tag_type: " + str(tag_type))
    
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

    
    if tag_type == "STATIC":
        # [END update_tag]
        # [START render_template]
        return render_template(
            'update_static_tag.html',
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
            'update_dynamic_tag.html',
            template_id=template_id,
            project_id=project_id,
            region=region,
            fields=template_fields,
            tag_config=tag_config,
            display_tag_history_option=tag_history,
            display_tag_stream_option=tag_stream)
    # [END render_template]
    
@app.route('/process_update_static_tag', methods=['POST'])
def process_update_static_tag():
    
    template_id = request.form['template_id']
    project_id = request.form['project_id']
    region = request.form['region']
    old_tag_uuid = request.form['tag_uuid']
    included_uris = request.form['included_uris'].rstrip()
    excluded_uris = request.form['excluded_uris'].rstrip()
    action = request.form['action']
    
    update_status = 0
    
    print("action: " + str(action))

    dcu = dc.DataCatalogUtils(template_id, project_id, region)
    template_fields = dcu.get_template()
    
    if action == "Submit Tag":
        
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
        new_tag_uuid = teu.update_tag_config(old_tag_uuid, 'STATIC', 'ACTIVE', fields, included_uris, excluded_uris,\
                                                 template_uuid, None, None, None, tag_history, tag_stream)
        
        update_status = dcu.create_update_static_tags(fields, included_uris, excluded_uris, None, new_tag_uuid, template_uuid,\
                                                 tag_history, tag_stream)
    
        if update_status == constants.SUCCESS:
            print('update_static_tags SUCCEEDED.')
        else:
            print('update_static_tags FAILED.')
         
    template_fields = dcu.get_template()  
    tag_configs = teu.read_tag_configs(template_id, project_id, region)
    
    print('template_fields: ' + str(template_fields))
      
     # [END process_update_static_tag]
     # [START render_template]
    return render_template(
         'view_tags.html',
         template_id=template_id,
         project_id=project_id,
         region=region,
         fields=template_fields,
         tag_configs=tag_configs,
         status=update_status)  

@app.route('/process_update_dynamic_tag', methods=['POST'])
def process_update_dynamic_tag():
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
    
    update_status = 0
    
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
        new_tag_uuid = teu.update_tag_config(old_tag_uuid, 'DYNAMIC', 'ACTIVE', fields, included_uris, excluded_uris,\
                                                  template_uuid, refresh_mode, refresh_frequency, refresh_unit, \
                                                  tag_history, tag_stream)
        
        update_status = dcu.create_update_dynamic_tags(fields, included_uris, excluded_uris, None, new_tag_uuid, template_uuid,\
                                                  tag_history, tag_stream)
    
        if update_status == constants.SUCCESS:
            print('update_dynamic_tags SUCCEEDED.')
        else:
            print('update_dynamic_tags FAILED.')
    
         
    template_fields = dcu.get_template()  
    print('template_fields: ' + str(template_fields))
    
    tag_configs = teu.read_tag_configs(template_id, project_id, region)
      
     # [END process_update_dynamic_tag]
     # [START render_template]
    return render_template(
         'view_tags.html',
         template_id=template_id,
         project_id=project_id,
         region=region,
         fields=template_fields,
         tag_configs=tag_configs,
         status=update_status)  


@app.route('/process_static_tag', methods=['POST'])
def process_static_tag():
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
    tag_uuid = teu.write_static_tag('ACTIVE', fields, included_uris, excluded_uris, template_uuid,\
                                         tag_history_option, tag_stream_option)
    
    creation_status = dcu.create_update_static_tags(fields, included_uris, excluded_uris, None, tag_uuid, template_uuid,\
                                                    tag_history_option, tag_stream_option)
    
    if creation_status == constants.SUCCESS:
        print('create_update_static_tags SUCCEEDED.')
    else:
        print('create_update_static_tags FAILED.')
            
    # [END process_static_tag]
    # [START render_template]
    return render_template(
        'submitted_static_tag.html',
        template_id=template_id,
        project_id=project_id,
        region=region,
        fields=fields,
        included_uris=included_uris,
        excluded_uris=excluded_uris,
        tag_history=tag_history_enabled,
        tag_stream=tag_stream_enabled,
        status=creation_status)
    # [END render_template]


@app.route('/process_dynamic_tag', methods=['POST'])
def process_dynamic_tag():
    template_id = request.form['template_id']
    project_id = request.form['project_id']
    region = request.form['region']
    included_uris = request.form['included_uris'].rstrip()
    excluded_uris = request.form['excluded_uris'].rstrip()
    refresh_mode = request.form['refresh_mode']
    refresh_frequency = request.form['refresh_frequency']
    refresh_unit = request.form['refresh_unit']
    action = request.form['action']
    
    print('included_uris: ' + included_uris)
    print('excluded_uris: ' + excluded_uris)
    print('refresh_mode: ' + refresh_mode)
    print('refresh_frequency: ' + refresh_frequency)
    print('refresh_unit: ' + refresh_unit)
    
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
        query_expression = request.form.get(selected_field)
        print("query_expression: " + query_expression)
        selected_field_type = request.form.get(selected_field + "_datatype")
        print("selected_field_type: " + selected_field_type)
        print(selected_field + ", " + query_expression + ", " + selected_field_type)
        
        for template_field in template:
            
            if template_field['field_id'] != selected_field:
                continue
        
            is_required = template_field['is_required']
            field = {'field_id': selected_field, 'query_expression': query_expression, 'field_type': selected_field_type,\
                     'is_required': is_required}
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
    tag_uuid, included_uris_hash = teu.write_dynamic_tag('ACTIVE', fields, included_uris, excluded_uris, template_uuid,\
                                                             refresh_mode, refresh_frequency, refresh_unit, \
                                                             tag_history_option, tag_stream_option)
     
    creation_status = dcu.create_update_dynamic_tags(fields, included_uris, excluded_uris, None, tag_uuid, template_uuid,\
                                                     tag_history_option, tag_stream_option)
    
    if creation_status == constants.SUCCESS:
        print('create_update_dynamic_tags SUCCEEDED.')
    else:
        print('create_update_dynamic_tags FAILED.')
     
    # [END process_dynamic_tag]
    # [START render_template]
    return render_template(
        'submitted_dynamic_tag.html',
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
        status=creation_status)
    # [END render_template]


##################### API METHODS #################

@app.route("/run_ready_jobs", methods=['POST'])
def run_ready_jobs():
    try:
        print('scan_for_updates')
        ts.scan_for_update_jobs()
        print('finished scan')
    except Exception as e:
        print('failed run ready jobs {}'.format(e))

    resp = jsonify(success=True)

    return resp
#[End run_ready_jobs]

@app.route("/clear_stale_jobs", methods=['POST'])
def reset_stale_jobs():
    try:
        ts.reset_stale_jobs()
    except Exception as e:
        print('failed reset job {}'.format(e))
        
    resp = jsonify(success=True)
    return resp
#[End reset stale jobs]


"""
Args:
    template_id: tag template to use
    project_id: tag template's Google Cloud project 
    region: tag template's region 
    included_uris: tag config's included uris or
    included_uris_hash: tag config's md5 hash value (in place of the included_uris)
    async: true if request is asynchronous, false otherwise. Defaults to false
Note: caller must provide either the included_uris_hash or included_uris
Returns:
    job_uuid = unique identifer for job, if async = true or
    status_code = 200 if update is successful, otherwise error with message
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
    
    if 'async' in json:
        async_req = json['async']
    else:
        async_req = False     
    
    if async_req == True:
        job_uuid = jm.create_job(tag_uuid)
        return jsonify(success=True, job_uuid=job_uuid)
    else: 
        dcu = dc.DataCatalogUtils(template_id, project_id, region)
        creation_status = dcu.create_update_dynamic_tags(tag_config.get('fields'), tag_config.get('included_uris'),\
                                    tag_config.get('excluded_uris'), None, tag_config.get('tag_uuid'),\
                                    tag_config.get('template_uuid'), tag_config.get('tag_history'), tag_config.get('tag_stream'))
    
        if creation_status == constants.SUCCESS:
            teu.increment_tag_config_version(tag_config.get('tag_uuid'), tag_config.get('version'))
            return jsonify(success=True)
        else:
            return jsonify(success=False, message="Error encountered while updating dynamic tags.")
    
    #[END dynamic_ondemand_update]
    

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
    async: true if this request is to be run asynchronously, false otherwise. Defaults to false. 
Returns:
    job_uuid if request is asynchronous or
    status_code = 200 if successful, otherwise error
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
    
    template_exists, template_uuid = teu.read_tag_template(template_id, project_id, region)
    
    if not template_exists:
        print("tag_template " + template_id + " doesn't exist")
        resp = jsonify(success=False, message="Tag template " + template_id + " doesn't exist in project " + project_id + " and region " + region + ".")
        return resp 
    
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
    
    tag_uuid, included_uris_hash = teu.write_dynamic_tag('ACTIVE', fields, included_uris, excluded_uris, template_uuid,\
                                                             refresh_mode, refresh_frequency, refresh_unit, \
                                                             tag_history, tag_stream)                                                      
    if 'async' in json:
        async_req = json['async']
    else:
        async_req = False     
    
    if async_req == True:
        job_uuid = jm.create_job(tag_uuid)
        return jsonify(job_uuid=job_uuid)
    else: 
        dcu = dc.DataCatalogUtils(template_id, project_id, region)
        creation_status = dcu.create_update_dynamic_tags(fields, included_uris, excluded_uris, None, tag_uuid, template_uuid,\
                                                         tag_history, tag_stream)
    
        if creation_status == constants.SUCCESS:
            return jsonify(success=True)
        else:
            return jsonify(success=False)
    
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
    job_uuid if request is asynchronous or
    status_code = 200 if successful, otherwise error
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
    
    template_exists, template_uuid = teu.read_tag_template(template_id, project_id, region)
    
    if not template_exists:
        print("tag_template " + template_id + " doesn't exist")
        resp = jsonify(success=False, message="Tag template " + template_id + " doesn't exist in project " + project_id + " and region " + region + ".")
        return resp 
    
    fields = json['fields']
    excluded_uris = json['excluded_uris']
    included_uris = json['included_uris']
    tag_history = json['tag_history']
    tag_stream = json['tag_stream']
    
    tag_uuid, included_uris_hash = teu.write_static_tag('ACTIVE', fields, included_uris, excluded_uris, template_uuid,\
                                                             tag_history, tag_stream)
     
    if 'async' in json:
        async_req = json['async']
    else:
        async_req = False     
    
    if async_req == True:
        job_uuid = jm.create_job(tag_uuid)
        return jsonify(job_uuid=job_uuid)
    else: 
        dcu = dc.DataCatalogUtils(template_id, project_id, region)
        creation_status = dcu.create_update_static_tags(fields, included_uris, excluded_uris, None, tag_uuid, template_uuid,\
                                                         tag_history, tag_stream)
    
        if creation_status == constants.SUCCESS:
            return jsonify(success=True)
        else:
            return jsonify(success=False)

 
@app.route("/get_job_status", methods=['POST'])
def get_job_status(): 
    
    json = request.get_json(force=True)
    job_uuid = json['job_uuid']
    
    job = jm.get_job_status(job_uuid)
    
    if job is None:
        return jsonify(success=False, message="job_uuid " + job_uuid + " cannot be found.")
    else:
        return jsonify(success=True, job_status=job['job_status'], job_tasks=job['job_tasks'], tasks_ran=job['tasks_ran'],\
                       tasks_completed=job['tasks_completed'], tasks_failed=job['tasks_failed'])
    

################ INTERNAL PROCESSING METHODS #################

@app.route("/_split_work", methods=['POST'])
def _split_work():
    
    print('*** enter _split_work ***')
    
    json = request.get_json(force=True)
    job_uuid = json['job_uuid']
    tag_uuid = json['tag_uuid']
    
    config = teu.read_tag_config(tag_uuid)
    uris = res.Resources.get_resources(config['included_uris'], config['excluded_uris'])
    
    #print('uris: ' + str(uris))

    jm.record_num_tasks(job_uuid, len(uris))
    jm.update_job_running(job_uuid) 
    tm.create_work_tasks(job_uuid, tag_uuid, uris)
    
    resp = jsonify(success=True)


@app.route("/_run_task", methods=['POST'])
def _run_task():
    
    print('*** enter _run_task ***')
    
    json = request.get_json(force=True)
    job_uuid = json['job_uuid']
    tag_uuid = json['tag_uuid']
    uri = json['uri']
    task_uuid = json['task_uuid']
    
    print('task_uuid: ' + task_uuid)
    
    tm.update_task_status(task_uuid, 'RUNNING')
    
    # retrieve tag config and template
    tag_config = teu.read_tag_config(tag_uuid)
    tem_config = teu.read_template_config(tag_config['template_uuid'])
    
    dcu = dc.DataCatalogUtils(tem_config['template_id'], tem_config['project_id'], tem_config['region'])
    
    creation_status = constants.ERROR
    
    if tag_config['tag_type'] == 'DYNAMIC':

        creation_status = dcu.create_update_dynamic_tags(tag_config['fields'], None, None, uri, tag_config['tag_uuid'], \
                                                         tag_config['template_uuid'], tag_config['tag_history'], \
                                                         tag_config['tag_stream'])
    if tag_config['tag_type'] == 'STATIC':
        
        creation_status = dcu.create_update_static_tags(tag_config['fields'], None, None, uri, tag_config['tag_uuid'], \
                                                        tag_config['template_uuid'], tag_config['tag_history'], \
                                                        tag_config['tag_stream'])
    if creation_status == constants.SUCCESS:
        tm.update_task_status(task_uuid, 'COMPLETED')
        jm.update_job_completion(job_uuid)
        resp = jsonify(success=True)
    else:
        tm.update_task_status(task_uuid, 'FAILED')
        jm.update_job_failed(job_uuid)
        resp = jsonify(success=False)
    
    return resp
#[END _run_task]
    
  
@app.route("/_dynamic_auto_update", methods=['POST'])
def _dynamic_auto_update():
    json = request.get_json(force=True)
    doc_id = json['doc_id']
    version = json['version']
    tag, tem = ts.get_config_and_template(doc_id)
    
    print('tag: ' + str(tag))
    print('tem: ' + str(tem))

    if version == tag.get('version'):
        dcu = dc.DataCatalogUtils(
            tem.get('template_id'), tem.get('project_id'), tem.get('region'))
        dcu.create_update_dynamic_tags(
            tag.get('fields'), tag.get('included_uris'), tag.get('excluded_uris'), None, tag.get('tag_uuid'),\
            tem.get('template_uuid'), tag.get('tag_history'), tag.get('tag_stream'))
        #update the document's scheduling information
        ts.schedule_job(doc_id)
    resp = jsonify(success=True)
    return resp
#[END _dynamic_auto_update]  
 
    
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
