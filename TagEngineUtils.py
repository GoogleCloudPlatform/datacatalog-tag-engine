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

import uuid, pytz, os, requests
import configparser, difflib, hashlib
from datetime import datetime
from datetime import timedelta
import DataCatalogUtils as dc
import BigQueryUtils as bq
from google.cloud import bigquery
from google.cloud import firestore
import constants

class TagEngineUtils:
    
    def __init__(self):
        
        self.db = firestore.Client()
        
        config = configparser.ConfigParser()
        config.read("tagengine.ini")
        
    def read_default_settings(self):
        
        settings = {}
        exists = False
        
        doc_ref = self.db.collection('settings').document('default_tag_template')
        doc = doc_ref.get()
        
        if doc.exists:
            settings = doc.to_dict()
            exists = True 
        
        return exists, settings

    def read_tag_history_settings(self):
        
        settings = {}
        enabled = False
        
        doc_ref = self.db.collection('settings').document('tag_history')
        doc = doc_ref.get()
        
        if doc.exists:
            settings = doc.to_dict()
            
            if settings['enabled']:
                enabled = True
            
        return enabled, settings


    def read_tag_stream_settings(self):
        
        settings = {}
        enabled = False
        
        doc_ref = self.db.collection('settings').document('tag_stream')
        doc = doc_ref.get()
        
        if doc.exists:
            settings = doc.to_dict()
            
            if settings['enabled']:
                enabled = True
            
        return enabled, settings
   
   
    def read_coverage_settings(self):
        
        settings = {}
        exists = False
        
        doc_ref = self.db.collection('settings').document('coverage')
        doc = doc_ref.get()
        
        if doc.exists:
            settings = doc.to_dict()
            exists = True 
        
        return exists, settings


    def write_default_settings(self, template_id, project_id, region):
        
        report_settings = self.db.collection('settings')
        doc_ref = report_settings.document('default_tag_template')
        doc_ref.set({
            'template_id': template_id,
            'project_id':  project_id,
            'region': region
        })
        
        print('Saved default settings.')
    
    
    def write_tag_history_settings(self, enabled, project_id, region, dataset):
        
        history_settings = self.db.collection('settings')
        doc_ref = history_settings.document('tag_history')
        doc_ref.set({
            'enabled': bool(enabled),
            'project_id': project_id,
            'region':  region,
            'dataset': dataset
        })
        
        print('Saved tag history settings.')
        
        # assume that the BQ dataset exists
        #bqu = bq.BigQueryUtils()
        #bqu.create_dataset(project_id, region, dataset)

    
    def write_tag_stream_settings(self, enabled, project_id, topic):
        
        history_settings = self.db.collection('settings')
        doc_ref = history_settings.document('tag_stream')
        doc_ref.set({
            'enabled': bool(enabled),
            'project_id': project_id,
            'topic':  topic
        })
        
        print('Saved tag stream settings.')


    def write_coverage_settings(self, project_ids, datasets, tables):
        
        report_settings = self.db.collection('settings')
        doc_ref = report_settings.document('coverage')
        doc_ref.set({
            'project_ids': project_ids,
            'excluded_datasets':  datasets,
            'excluded_tables': tables
        })
        
        print('Saved coverage settings.')
        
        
    def generate_coverage_report(self):    
    
        summary_report = []
        detailed_report = []
        
        exists, settings = self.read_coverage_settings()
        project_ids = settings['project_ids']
        excluded_datasets = settings['excluded_datasets']
        excluded_tables = settings['excluded_tables']
        
        print('project_ids: ' + project_ids)
        print('excluded_datasets: ' + excluded_datasets)
        print('excluded_tables: ' + excluded_tables)
        
        log_ref = self.db.collection('logs')
        
        # list datasets and tables for chosen projects
        for project in project_ids.split(','):
            project_id = project.strip()
            bq_client = bigquery.Client(project=project_id)
            datasets = list(bq_client.list_datasets())
            
            total_tags = 0
            
            for dataset in datasets:
                
                dataset_id = dataset.dataset_id

                if project_id + "." + dataset_id in excluded_datasets:
                    #print('skipping ' + project_id + "." + dataset_id)
                    continue
               
                print("dataset: " + dataset_id)
                
                qualified_dataset = project_id + "." + dataset_id
                overall_sum = 0    
                table_list = []
                tables = list(bq_client.list_tables(dataset_id))
                
                dcu = dc.DataCatalogUtils()
                linked_resources = dcu.search_catalog(project_id, dataset_id)
                
                print('linked_resources: ' + str(linked_resources))
            
                for table in tables:
                    print("full_table_id: " + str(table.full_table_id))
                
                    table_path_full = table.full_table_id.replace(':', '/datasets/').replace('.', '/tables/')
                    table_path_short = table.full_table_id.replace(':', '.')
                    table_name = table_path_full.split('/')[4]
                
                    print('table_path_full: ' + table_path_full)
                    print('table_path_short: ' + table_path_short)
                    print('table_name: ' + table_name)
                
                    if table_path_short in project_id + '.' + excluded_tables:
                        print('skipping ' + table_path_short)
                        continue
                    
                    if table_name in linked_resources:
                        tag_count = linked_resources[table_name]
                        overall_sum = overall_sum + tag_count
                        print("tag_count = " + str(tag_count))
                        print("overall_sum = " + str(overall_sum))
                
                        # add the table name and tag count to a list 
                        table_list.append((table_name, tag_count))

                # add record to summary report
                summary_record = (qualified_dataset, overall_sum)
                summary_report.append(summary_record)
                detailed_record = {qualified_dataset: table_list}
                detailed_report.append(detailed_record)
        
        return summary_report, detailed_report
      
      
    def update_config_status(self, tag_uuid, status):
    
        config_ref = self.db.collection('tag_config').document(tag_uuid)
        config_ref.update({'config_status': status})
     
    
    def update_scheduling_status(self, tag_uuid, status):
    
        config_ref = self.db.collection('tag_config').document(tag_uuid)
        config_ref.update({'scheduling_status': status})
                         
    
    def increment_version_next_run(self, tag_uuid):
        
        config = self.read_tag_config(tag_uuid)
        
        version = config.get('version', 0) + 1
        delta = config.get('refresh_frequency', 24)
        unit = config.get('refresh_unit', 'hours')
        
        if unit == 'hours':
            next_run = datetime.utcnow() + timedelta(hours=delta)
        if unit == 'days':
            next_run = datetime.utcnow() + timedelta(days=delta)
        
        self.db.collection('tag_config').document(tag_uuid).update({
            'version': version,
            'next_run' : next_run
        })
            
                                                                         
    def read_template_config(self, template_uuid):
                
        tag_config = {}
        
        template_ref = self.db.collection('tag_template').document(template_uuid)
        doc = template_ref.get()
        
        if doc.exists:
            template_config = doc.to_dict()
            #print(str(tag_config))
            
        return template_config
    
    
    def read_tag_template(self, template_id, project_id, region):
        
        template_exists = False
        template_uuid = ""
        
        # check to see if this template already exists
        template_ref = self.db.collection('tag_template')
        query = template_ref.where('template_id', '==', template_id).where('project_id', '==', project_id).where('region', '==', region)
        
        matches = query.get()
        
        # should either be a single matching template or no matching templates
        if len(matches) == 1:
            if matches[0].exists:
                print('Tag Template exists. Template uuid: ' + str(matches[0].id))
                template_uuid = matches[0].id
                template_exists = True
        
        return (template_exists, template_uuid)
        
    
    def write_tag_template(self, template_id, project_id, region):
        
        template_exists, template_uuid = self.read_tag_template(template_id, project_id, region)
        
        if template_exists == False:    
            print('tag template {} doesn\'t exist. Creating new template'.format(template_id))
             
            template_uuid = uuid.uuid1().hex
                     
            doc_ref = self.db.collection('tag_template').document(template_uuid)
            doc_ref.set({
                'template_uuid': template_uuid, 
                'template_id': template_id,
                'project_id': project_id,
                'region': region
            })
                                   
        return template_uuid
        
        
    def write_static_config(self, config_status, fields, included_uris, excluded_uris, template_uuid, tag_history, tag_stream):
        
        # hash the included_uris string
        included_uris_hash = hashlib.md5(included_uris.encode()).hexdigest()
        
        # check to see if this tag config already exists
        tag_ref = self.db.collection('tag_config')
        query = tag_ref.where('template_uuid', '==', template_uuid).where('included_uris_hash', '==',\
                            included_uris_hash).where('config_type', '==', 'STATIC').where('config_status', '!=', 'INACTIVE')
       
        matches = query.get()
       
        for match in matches:
            if match.exists:
                tag_uuid_match = match.id
                #print('Tag config already exists. Tag_uuid: ' + str(tag_uuid_match))
                
                # update status to INACTIVE 
                self.db.collection('tag_config').document(tag_uuid_match).update({
                    'config_status' : "INACTIVE"
                })
                #print('Updated status to INACTIVE.')
        
       
        tag_uuid = uuid.uuid1().hex
       
        tag_config = self.db.collection('tag_config')
        doc_ref = tag_config.document(tag_uuid)
        doc_ref.set({
            'tag_uuid': tag_uuid,
            'config_type': 'STATIC',
            'config_status': config_status, 
            'creation_time': datetime.utcnow(), 
            'fields': fields,
            'included_uris': included_uris,
            'included_uris_hash': included_uris_hash,
            'excluded_uris': excluded_uris,
            'template_uuid': template_uuid,
            'tag_history': tag_history,
            'tag_stream': tag_stream
        })
        
        return tag_uuid, included_uris_hash
    
    
    def write_dynamic_config(self, config_status, fields, included_uris, excluded_uris, template_uuid, refresh_mode,\
                          refresh_frequency, refresh_unit, tag_history, tag_stream):
        
        included_uris_hash = hashlib.md5(included_uris.encode()).hexdigest()
        
        # check to see if this config already exists
        tag_ref = self.db.collection('tag_config')
        query = tag_ref.where('template_uuid', '==', template_uuid).where('included_uris_hash', '==', included_uris_hash).where('config_type', '==', 'DYNAMIC').where('config_status', '!=', 'INACTIVE')
       
        matches = query.get()
       
        for match in matches:
            if match.exists:
                tag_uuid_match = match.id
                #print('Tag config already exists. Tag_uuid: ' + str(tag_uuid_match))
                
                # update status to INACTIVE 
                self.db.collection('tag_config').document(tag_uuid_match).update({
                    'config_status' : "INACTIVE"
                })
                print('Updated status to INACTIVE.')
       
        tag_uuid = uuid.uuid1().hex
        tag_config = self.db.collection('tag_config')
        doc_ref = tag_config.document(tag_uuid)
        
        if refresh_mode == 'AUTO':
            if type(refresh_frequency) is int: 
                if refresh_frequency > 0:
                    delta = refresh_frequency
                else:
                    delta = 24
            
            if type(refresh_frequency) is str:
                if refresh_frequency.isdigit():
                    delta = int(refresh_frequency)
                else:
                    delta = 24
                
            if refresh_unit == 'hours':
                next_run = datetime.utcnow() + timedelta(hours=delta)
            if refresh_unit == 'days':
                next_run = datetime.utcnow() + timedelta(days=delta)
            
            doc_ref.set({
                'tag_uuid': tag_uuid,
                'config_type': 'DYNAMIC',
                'config_status': config_status, 
                'creation_time': datetime.utcnow(), 
                'fields': fields,
                'included_uris': included_uris,
                'included_uris_hash': included_uris_hash,
                'excluded_uris': excluded_uris,
                'template_uuid': template_uuid,
                'refresh_mode': refresh_mode, # AUTO refresh mode
                'refresh_frequency': delta,
                'refresh_unit': refresh_unit,
                'tag_history': tag_history,
                'tag_stream': tag_stream,
                'scheduling_status': 'PENDING',
                'next_run': next_run,
                'version': 1
            })
            
        else:
            doc_ref.set({
                'tag_uuid': tag_uuid,
                'config_type': 'DYNAMIC',
                'config_status': config_status, 
                'creation_time': datetime.utcnow(), 
                'fields': fields,
                'included_uris': included_uris,
                'included_uris_hash': included_uris_hash,
                'excluded_uris': excluded_uris,
                'template_uuid': template_uuid,
                'refresh_mode': refresh_mode, # ON_DEMAND refresh mode
                'refresh_frequency': 0,
                'tag_history': tag_history,
                'tag_stream': tag_stream,
                'version': 1
            })
        
        print('Created new dynamic config.')
        
        return tag_uuid, included_uris_hash
        
    
    def write_entry_config(self, config_status, fields, included_uris, excluded_uris, template_uuid, refresh_mode,\
                           refresh_frequency, refresh_unit, tag_history, tag_stream):
        
        included_uris_hash = hashlib.md5(included_uris.encode()).hexdigest()
        
        # check to see if this config already exists
        tag_ref = self.db.collection('tag_config')
        query = tag_ref.where('template_uuid', '==', template_uuid).where('included_uris_hash', '==', included_uris_hash).where('config_type', '==', 'ENTRY').where('config_status', '!=', 'INACTIVE')
       
        matches = query.get()
       
        for match in matches:
            if match.exists:
                tag_uuid_match = match.id
                #print('Tag config already exists. Tag_uuid: ' + str(tag_uuid_match))
                
                # update status to INACTIVE 
                self.db.collection('tag_config').document(tag_uuid_match).update({
                    'config_status' : "INACTIVE"
                })
                print('Updated status to INACTIVE.')
       
        tag_uuid = uuid.uuid1().hex
        tag_config = self.db.collection('tag_config')
        doc_ref = tag_config.document(tag_uuid)
        
        if refresh_mode == 'AUTO':
            if type(refresh_frequency) is int: 
                if refresh_frequency > 0:
                    delta = refresh_frequency
                else:
                    delta = 24
            
            if type(refresh_frequency) is str:
                if refresh_frequency.isdigit():
                    delta = int(refresh_frequency)
                else:
                    delta = 24
                
            if refresh_unit == 'hours':
                next_run = datetime.utcnow() + timedelta(hours=delta)
            if refresh_unit == 'days':
                next_run = datetime.utcnow() + timedelta(days=delta)
            
            doc_ref.set({
                'tag_uuid': tag_uuid,
                'config_type': 'ENTRY',
                'config_status': config_status, 
                'creation_time': datetime.utcnow(), 
                'fields': fields,
                'included_uris': included_uris,
                'included_uris_hash': included_uris_hash,
                'excluded_uris': excluded_uris,
                'template_uuid': template_uuid,
                'refresh_mode': refresh_mode, # AUTO refresh mode
                'refresh_frequency': delta,
                'refresh_unit': refresh_unit,
                'tag_history': tag_history,
                'tag_stream': tag_stream,
                'scheduling_status': 'PENDING',
                'next_run': next_run,
                'version': 1
            })
            
        else:
            doc_ref.set({
                'tag_uuid': tag_uuid,
                'config_type': 'ENTRY',
                'config_status': config_status, 
                'creation_time': datetime.utcnow(), 
                'fields': fields,
                'included_uris': included_uris,
                'included_uris_hash': included_uris_hash,
                'excluded_uris': excluded_uris,
                'template_uuid': template_uuid,
                'refresh_mode': refresh_mode, # ON_DEMAND refresh mode
                'refresh_frequency': 0,
                'tag_history': tag_history,
                'tag_stream': tag_stream,
                'version': 1
            })
        
        print('Created new entry config.')
        
        return tag_uuid, included_uris_hash
     
        
    def write_log_entry(self, dc_op, resource_type, resource, column, config_type, tag_uuid, tag_id, template_uuid):
                    
        log_entry = {}
        log_entry['ts'] = datetime.utcnow()
        log_entry['dc_op'] = dc_op
        log_entry['res_type'] = resource_type
        log_entry['config_type'] = 'MANUAL'
        log_entry['res'] = resource
        
        if len(column) > 0:
            log_entry['col'] = column
        
        log_entry['config_type'] = config_type
        log_entry['tag_uuid'] = tag_uuid
        log_entry['dc_tag_id'] = tag_id
        log_entry['template_uuid'] = template_uuid

        self.db.collection('logs').add(log_entry)
        #print('Wrote log entry.')
        
    
    def write_tag_op_error(self, op, uri, col, tag_uuid, template_uuid, msg):
                    
        error = {}
        error['ts'] = datetime.utcnow()
        error['op'] = op
        error['uri'] = uri
        error['col'] = col
        error['tag_uuid'] = tag_uuid
        error['template_uuid'] = template_uuid
        error['msg'] = msg
        
        self.db.collection('tag_op_error').add(error)
        #print('Wrote error entry.')


    def write_tag_value_error(self, msg):
                    
        error = {}
        error['ts'] = datetime.utcnow()
        error['error'] = msg
        
        self.db.collection('tag_value_error').add(error)
        #print('Wrote error entry.')    
    
    
    def read_tag_configs(self, template_id, project_id, region):
        
        tag_configs = []
        
        template_exists, template_uuid = self.read_tag_template(template_id, project_id, region)
        
        tag_ref = self.db.collection('tag_config')
        docs = tag_ref.where('template_uuid', '==', template_uuid).where('config_status', '!=', 'INACTIVE').stream()
        
        for doc in docs:
            tag_config = doc.to_dict()
            tag_configs.append(tag_config)  
                
        #print(str(tag_configs))
        return tag_configs
        
    
    def read_tag_config(self, tag_uuid):
                
        tag_config = {}
        
        tag_ref = self.db.collection('tag_config').document(tag_uuid)
        doc = tag_ref.get()
        
        if doc.exists:
            tag_config = doc.to_dict()
            #print(str(tag_config))
            
        return tag_config
        
      
    def read_ready_configs(self):
        
        tag_uuids = []
        
        config_ref = self.db.collection('tag_config')
        config_ref = config_ref.where("refresh_mode", "==", "AUTO")
        config_ref = config_ref.where("scheduling_status", "==", "READY")
        config_ref = config_ref.where("config_status", "==", "ACTIVE")
        config_ref = config_ref.where("next_run", "<=", datetime.utcnow())
        
        ready_configs = list(config_ref.stream())
        
        for ready_config in ready_configs:
            
            config = ready_config.to_dict()
            tag_uuids.append(config['tag_uuid'])
            
        return tag_uuids  
        
        
    def lookup_tag_config_by_included_uris(self, template_uuid, included_uris, included_uris_hash):
        
        success = False
        tag_config = {}
        
        tag_ref = self.db.collection('tag_config')
        
        if included_uris is not None:
            docs = tag_ref.where('template_uuid', '==', template_uuid).where('config_status', '==', 'ACTIVE')\
            .where('included_uris', '==', included_uris).stream()
        if included_uris_hash is not None:
            docs = tag_ref.where('template_uuid', '==', template_uuid).where('config_status', '==', 'ACTIVE')\
            .where('included_uris_hash', '==', included_uris_hash).stream()
        
        for doc in docs:
            tag_config = doc.to_dict()
            break
        
        print('tag_config: ' + str(tag_config))
        
        if tag_config:
            success = True     

        return success, tag_config
        
    
    def update_tag_config(self, old_tag_uuid, config_type, config_status, fields, included_uris, excluded_uris, template_uuid, \
                          refresh_mode, refresh_frequency, refresh_unit, tag_history, tag_stream):
        
        self.db.collection('tag_config').document(old_tag_uuid).update({
            'config_status' : "INACTIVE"
        })
        
        if config_type == 'STATIC':
            new_tag_uuid, included_uris_hash = self.write_static_config(config_status, fields, included_uris, excluded_uris, template_uuid, \
                                                                         tag_history, tag_stream)
        
        if config_type == 'DYNAMIC':
            new_tag_uuid, included_uris_hash = self.write_dynamic_config(config_status, fields, included_uris, excluded_uris, \
                                                                     template_uuid, refresh_mode, refresh_frequency, refresh_unit,\
                                                                     tag_history, tag_stream)
        if config_type == 'ENTRY':
            new_tag_uuid, included_uris_hash = self.write_entry_config(config_status, fields, included_uris, excluded_uris, \
                                                                     template_uuid, refresh_mode, refresh_frequency, refresh_unit,\
                                                                     tag_history, tag_stream)
        # note: don't need to return the included_uris_hash
            
        return new_tag_uuid
    
    
##### ##### ##### ##### ##### ##### ##### #####      
#####  Tag Propagation Methods  #####  
        
    def read_propagation_settings(self):
        
        settings = {}
        exists = False
        
        doc_ref = self.db.collection('settings').document('propagation')
        doc = doc_ref.get()
        
        if doc.exists:
            settings = doc.to_dict()
            exists = True 
        
        return exists, settings
        
        
    def write_propagation_settings(self, source_project_ids, dest_project_ids, excluded_datasets, job_frequency):
        
        report_settings = self.db.collection('settings')
        doc_ref = report_settings.document('propagation')
        doc_ref.set({
            'source_project_ids': source_project_ids,
            'dest_project_ids':  dest_project_ids,
            'excluded_datasets': excluded_datasets,
            'job_frequency': job_frequency
        })
        
        print('Saved tag propagation settings.')
    
    def read_propagated_tag_config(self, tag_uuid):
                
        propagated_tag_config = {}
        
        propagated_tag_ref = self.db.collection('propagated_config').document(tag_uuid)
        doc = propagated_tag_ref.get()
        
        if doc.exists:
            propagated_tag_config = doc.to_dict()
            
        return propagated_tag_config
  
    
    def run_propagation_job(self, source_project_ids, dest_project_ids, excluded_datasets):
     
     #print('*** enter run_propagation_job ***')
     #print("source_project_ids: " + source_project_ids)
     #print("dest_project_ids: " + dest_project_ids)
     #print("excluded_datasets: " + excluded_datasets)
     
     for dest_project in dest_project_ids.split(','):
         dest_project_id = dest_project.strip()
         
         print("dest_project_id: " + dest_project_id)
         
         bq_client = bigquery.Client(project=dest_project_id)
         datasets = list(bq_client.list_datasets())
     
         for dataset in datasets:
             print("dataset_id: " + dataset.dataset_id)
             
             # filter out excluded datasets
             if dest_project_id + "." + dataset.dataset_id in excluded_datasets:
                 print("excluding " + dest_project_id + "." + dataset.dataset_id + " from propagation")
                 continue

             query_str = """
                     select table_name as view_name, view_definition
                     from `""" + dest_project_id + "." + dataset.dataset_id + "." + """INFORMATION_SCHEMA.VIEWS`
                     """
             query_job = bq_client.query(query_str)
         
             for view in query_job:
                 view_name = view["view_name"]
                 view_def = view["view_definition"]
                 
                 print('view_name: ' + view_name)
                 print('view_def: ' + view_def)
                 
                 view_res = dest_project_id + '/datasets/' + dataset.dataset_id + '/views/' + view_name
                 print('view_res: ' + view_res)
             
                 source_tables = self.source_tables_from_view(view_def)
                 print('source_tables: ' + str(source_tables))
                 
                 view_source_tables_configs = [] # contains list of all source tag configs per view
                 
                 for source in source_tables:
                 
                     source_split = source.split(".")

                     if len(source_split) == 3:
                         source_project = source_split[0]
                         source_dataset = source_split[1]
                         source_table = source_split[2]
                     elif len(source_split) == 2:
                         source_project = dest_project_id
                         source_dataset = source_split[0]
                         source_table = source_split[1]
                     else:
                         print("Opps, something went wrong, couldn't parse view definition") 
                         continue
                     
                     source_res = source_project + '/datasets/' + source_dataset + '/tables/' + source_table
                     print('source_res: ' + source_res)
                                     
                     tag_configs = self.read_tag_configs_on_res(source_res)
                     
                     if len(tag_configs) == 0:
                         self.write_unpropagated_log_entry(source_res, view_res, 'PROPAGATED', 'NONE', '')
                     else:
                         self.add_source_to_configs(tag_configs, source_res)
                         view_source_tables_configs.append(tag_configs)
                 
                 # end for source in source tables

                 if len(view_source_tables_configs) == 0:
                     # source tables have zero tags, and we have already logged these events, move on to next view
                     continue
                 else:
                     # go through all tags attached to this view's source tables and triage them
                     # returns list of configs which are tagged as CONFLICT or AGREE
                     reconciled_configs = self.triage_tag_configs(view_res, view_source_tables_configs)
                         
                 for config in reconciled_configs:
                     
                     config_status = config['config_status']
                     source_tag_uuid = config['tag_uuid']
                     source_res = config['source_res']
                     
                     if isinstance(source_res, list) == False:
                         source_res = [source_res]
                     
                     config_type = config['config_type']
                     fields = config['fields']
                     included_uris = config['included_uris']
                     template_uuid = config['template_uuid']
                     
                     print('source_res: ' + str(source_res))
                     print('fields: ' + str(fields))
                     
                     # check if tag has been forked, we don't want to override it, if it has
                     if self.is_forked_tag(template_uuid, source_res, view_res):
                         continue
                     
                     template_config = self.read_template_config(template_uuid)
                     template_id = template_config['template_id']
                     project_id = template_config['project_id']
                     region = template_config['region']
                     dcu = dc.DataCatalogUtils(template_id, project_id, region)
                 
                     # parse the included_uris field, matching the source_res against it. 
                     # extract table-level and column-level tags from the included_uris field
                     columns = self.extract_tagged_columns(source_res, view_res, included_uris)
                     #print('columns: ' + str(columns))
                     
                     # create or update propagated_config 
                     view_tag_uuid = self.create_or_update_propagated_config(source_tag_uuid, source_res, view_res, config_status, columns, view_def, \
                                         config_type, fields, template_uuid)
                     
                     if config_status == 'CONFLICT':
                         self.write_unpropagated_log_entry(source_res, view_res, 'PROPAGATED', config_status, template_uuid)
                     elif config_status == 'PROPAGATED':
                         if config_type == "STATIC":
                             dcu.create_update_static_propagated_tag('PROPAGATED', source_res, view_res, columns, fields, source_tag_uuid, view_tag_uuid,\
                                  template_uuid)
                         
                         if config_type == "DYNAMIC":
                             dcu.create_update_dynamic_propagated_tag('PROPAGATED', source_res, view_res, columns, fields, source_tag_uuid, view_tag_uuid,\
                                  template_uuid)
 
    def is_forked_tag(self, template_uuid, source_res, view_res):
        
        config_ref = self.db.collection('propagated_config')
        query = config_ref.where('template_uuid', '==', template_uuid).where('source_res', '==', source_res).where('view_res', '==', view_res).where('config_status', 'in', ['PROPAGATED AND FORKED', 'CONFLICT AND FORKED'])
        
        results = query.stream()
        
        for doc in results:
            if doc.exists:
                return True

        return False
        

    def create_or_update_propagated_config(self, source_tag_uuid, source_res, view_res, config_status, columns, view_def, config_type, fields,\
                                           template_uuid):
        
        #print('enter create_or_update_propagated_config')
        
        # check to see if we have an active config 
        tag_ref = self.db.collection('propagated_config')
        query = tag_ref.where('template_uuid', '==', template_uuid).where('view_res', '==', view_res).where('source_res', 'array_contains_any', source_res)
        results = query.stream()
        
        doc_exists = False
        
        for doc in results:
            
            if doc.exists:
            
                prop_config = doc.to_dict() 
                
                if len(columns) > 0:
                    if prop_config['cols'] == columns:
                        doc_exists = True
                else:
                    doc_exists = True
                
                if doc_exists == False:
                    break
                    
                view_tag_uuid = doc.id
                print('Config already exists. Tag_uuid: ' + str(view_tag_uuid))
                
                if prop_config['config_status'] == 'FORKED':
                    return view_tag_uuid
                
                if prop_config['fields'] != fields:
                    self.db.collection('propagated_config').document(view_tag_uuid).update({
                     'config_status' : config_status,
                     'fields' : fields,
                     'last_modified_time' : datetime.utcnow()
                 })
                    print('Updated propagated_config.')
                else:
                    self.db.collection('propagated_config').document(view_tag_uuid).update({
                        'last_modified_time' : datetime.utcnow()
                    })
                    
                    print('Propagated config fields are equal, updated last_modified_time only.')
                
        if doc_exists == False:
            view_tag_uuid = uuid.uuid1().hex
       
            prop_config = self.db.collection('propagated_config')
            doc_ref = prop_config.document(view_tag_uuid)
            
            doc = {
                'view_tag_uuid': view_tag_uuid,
                'source_tag_uuid': source_tag_uuid,
                'config_type': config_type,
                'config_status': config_status, 
                'creation_time': datetime.utcnow(), 
                'fields': fields,
                'source_res': source_res,
                'view_res': view_res, 
                'view_def': view_def,
                'template_uuid': template_uuid}
            
            if len(columns) > 0:
                doc['cols'] = columns 
            
            doc_ref.set(doc)
            print('Created new propagated tag config.')
        
        return view_tag_uuid
          

    def write_propagated_log_entry(self, config_status, dc_op, res_type, source_res, view_res, column, config_type, source_tag_uuid, view_tag_uuid, tag_id, template_uuid):
        
        log_entry = {}
        log_entry['ts'] = datetime.utcnow()
        log_entry['dc_op'] = dc_op
        log_entry['res_type'] = res_type
        log_entry['config_type'] = 'PROPAGATED'
        log_entry['config_status'] = config_status
        log_entry['config_type'] = config_type
        log_entry['source_res'] = source_res
        log_entry['view_res'] = view_res
        
        if len(column) > 0:
           log_entry['col'] = column 
        
        log_entry['config_type'] = config_type
        log_entry['source_tag_uuid'] = source_tag_uuid
        log_entry['view_tag_uuid'] = view_tag_uuid
        log_entry['dc_tag_id'] = tag_id
        log_entry['template_uuid'] = template_uuid

        self.db.collection('logs').add(log_entry)
        #print('Wrote log entry.')
    
    def write_unpropagated_log_entry(self, source_res_list, view_res, config_type, config_status, template_uuid):
        log_entry = {}
        log_entry['source_res'] = source_res_list
        log_entry['view_res'] = view_res
        log_entry['config_type'] = config_type
        log_entry['config_status'] = config_status
        
        if template_uuid != "":
            log_entry['template_uuid'] = template_uuid
        
        log_entry['ts'] = datetime.utcnow()
        
        self.db.collection('logs').add(log_entry)
        #print('Wrote log entry.')
        
    def generate_propagation_report(self):    
    
        #print("*** enter generate_propagation_report ***")
        
        report = []
        last_run = None
        source_view_set = set()
        
        prop_configs = self.db.collection('propagated_config').stream()

        for config in prop_configs:  
                      
            prop_entry = config.to_dict()
            print("prop_entry: " + str(prop_entry))
            
            view_res_pretty = prop_entry['view_res'].replace('/datasets', '').replace('/views', '')
            prop_entry['view_res'] = view_res_pretty
                    
            source_res_list = prop_entry['source_res']
            source_res_pretty = source_res_list[0]
            if len(source_res_list) > 1:
                source_res_pretty = source_res_pretty + '...' 
            
            source_res_pretty = source_res_pretty.replace('/datasets', '').replace('/tables', '') 
            prop_entry['source_res'] = source_res_pretty
                        
            template_config = self.read_template_config(prop_entry['template_uuid'])
            prop_entry['template_id'] = template_config['template_id']
                
            if last_run is None:
                
                if 'last_modified_time' in prop_entry:
                    last_run = prop_entry['last_modified_time']
                else:
                    last_run = prop_entry['creation_time']
                #print('last_run: ' + str(last_run))
            
            report.append(prop_entry)
        
        last_hour_ts = datetime.utcnow() - timedelta(hours = 1)
        
        logs = self.db.collection('logs').where('config_type', '==', 'PROPAGATED').where('config_status', '==', 'NONE').where('ts', '>=',\
                last_hour_ts).order_by('ts', direction=firestore.Query.DESCENDING).stream()

        for log in logs:
            
            has_missing = True
            
            log_entry = log.to_dict()
            
            view_res_pretty = log_entry['view_res'].replace('/datasets', '').replace('/tables', '')     
            
            source_res_pretty = log_entry['source_res'].replace('/datasets', '').replace('/tables', '')
            
            source_view_pair = source_res_pretty + '&' + view_res_pretty
            
            if source_view_pair not in source_view_set:
                source_view_set.add(source_view_pair)            
                        
                report_entry = {}
                report_entry['config_status'] = log_entry['config_status']
                report_entry['source_res'] = source_res_pretty  
                report_entry['view_res'] = view_res_pretty  
                report.append(report_entry)     
        
            if last_run is None:
                last_run = log_entry['ts']
                #print('last_run: ' + str(last_run))
        
        return report, last_run 
    
    
    def read_propagated_configs_on_res(self, source_res, view_res, template_id):
            
        tag_config_results = []
        
        view_res_full = view_res.replace('.', '/datasets/', 1).replace('.', '/tables/', 1)
        print('view_res_full: ' + view_res_full)
        
        prop_config_ref = self.db.collection('propagated_config')
        tag_config_ref = self.db.collection('tag_config')
         
        log_ref = self.db.collection('logs')
        query1 = log_ref.where('template_uuid', '==', template_uuid).where('source_res', '==', source_res_full).where('view_res', '==',\
                 view_res_full).order_by('ts', direction=firestore.Query.DESCENDING).limit(1)
        prop_results = query1.stream()
        
        for prop_record in prop_results:
            print('found prop log id ' + prop_record.id)
            record = prop_record.to_dict()
            view_tag_uuid = record['tag_uuid']
             
            view_config = prop_config_ref.document(view_tag_uuid)         
            if view_config.exists:
                view_record = view_config.to_dict()
                
            # get tag_config for parent
            source_config = self.read_tag_configs_on_res(source_res)
        
        return view_config, source_config, template_id
    
    
    def source_tables_from_view(self, view_def):
        
        source_tables = []
        payload = {"sql": view_def}
        
        zeta = config['DEFAULT']['ZETA_URL']
        response = requests.post(zeta, json=payload)
        print('zeta response: ' + str(response))
        
        resp_dict = response.json()
        
        for resp in resp_dict:
            
            source_res = '.'.join(resp)
            source_tables.append(source_res)
        
        return source_tables
    
    
    def read_propagated_config(self, tag_uuid):
                
        propagated_config = {}
        
        propagated_ref = self.db.collection('propagated_config').document(tag_uuid)
        doc = propagated_ref.get()
        
        if doc.exists:
            propagated_config = doc.to_dict()
            print("propagated_config: " + str(propagated_config))
            
        return propagated_config

    
    def fork_propagated_tag(self, tag_uuid, config_status, fields, refresh_frequency):
        
        transaction = self.db.transaction()
        config_ref = self.db.collection('propagated_config').document(tag_uuid)

        self.update_in_transaction(transaction, config_ref, config_status, fields, refresh_frequency)
        updated_config = config_ref.get().to_dict()
        return updated_config
 
    
    @firestore.transactional
    def update_in_transaction(transaction, config_ref, config_status, fields, refresh_frequency):
        snapshot = config_ref.get(transaction=transaction)
        
        if refresh_frequency != None:
            transaction.update(config_ref, {
                'fields': fields,
                'refresh_frequency': refresh_frequency,
                'config_status': config_status
            })
        else:
            transaction.update(config_ref, {
                'fields': fields,
                'config_status': config_status
            })
                
    def triage_tag_configs(self, view_res, source_tables_tag_configs):
        
        #print('enter triage_tag_configs')
        #print('view_res: ' + view_res)
        #print('source_tables_tag_configs: ' + str(source_tables_tag_configs))
        
        reconciled_tags = [] # tracks configs which conflict or/and agree  
        overlapping_tags = [] # tracks configs which overlap and still need to be reconciled   
        template_tag_mapping = {} # key == tag_template_uuid, val == [tag_uuid]

        for source_table_tag_configs in source_tables_tag_configs:
            
            for tag_config in source_table_tag_configs:
            
                print('tag_config: ' + str(tag_config))
                
                template_uuid = tag_config['template_uuid']
                tag_uuid = tag_config['tag_uuid']
                    
                if template_uuid in template_tag_mapping:
                    tag_uuid_list = template_tag_mapping[template_uuid]
                    tag_uuid_list.append(tag_uuid)
                    
                    reconciled_tags, overlapping_tags = self.swap_elements(template_uuid, reconciled_tags, overlapping_tags)
                    tag_config['config_status'] = 'OVERLAP'
                    overlapping_tags.append(tag_config)

                else:
                    tag_uuid_list = []
                    tag_uuid_list.append(tag_uuid)
                    template_tag_mapping[template_uuid] = tag_uuid_list
                    tag_config['config_status'] = 'PROPAGATED'
                    reconciled_tags.append(tag_config)

        
        if len(overlapping_tags) == 0:
            print('we have no overlapping tags')
            print('reconciled_tags: ' + str(reconciled_tags))
            return reconciled_tags
        
        # we have some overlapping tags    
        tag_uuid_lists = template_tag_mapping.values()
            
        for tag_uuid_list in tag_uuid_lists:
            
            if len(tag_uuid_list) > 1:
                
                agreeing_tag, conflicting_tag = self.run_diff(tag_uuid_list, overlapping_tags)
    
                if len(conflicting_tag) > 0:
                    
                    print('we have a conflicting tag')
                    print('conflictings_tag: ' + str(conflicting_tag))
                    conflicting_tag['config_status'] = 'CONFLICT'
                    conflicting_tag['tag_uuid'] = tag_uuid_list
                    
                    reconciled_tags.append(conflicting_tag)
                                        
                if len(agreeing_tag) > 0:
                    
                    print('we have an agreeing tag')
                    print('agreeing_tag: ' + str(agreeing_tag))
                    agreeing_tag['config_status'] = 'PROPAGATED'
                    agreeing_tag['tag_uuid'] = tag_uuid_list
                     
                    reconciled_tags.append(agreeing_tag)
                
                print('reconciled_tags: ' + str(reconciled_tags))
        
        return reconciled_tags
    
    
    def add_source_to_configs(self, tag_configs, source_res):
        
        for tag_config in tag_configs:
            
            tag_config['source_res'] = source_res
            
    
    def extract_source_res_list(self, tag_configs):
        
        source_res_list = []
        
        for tag_config in tag_configs:
            source_res_list.append(tag_config['source_res'])
            
        return source_res_list  
            
    
    def swap_elements(self, template_uuid, reconciled_tags, overlapping_tags):
        
        purge_tag_configs = []
        
        # reconciled_tags and overlapping_tags may both contain more than one config
        
        for tag_config in reconciled_tags:
            if template_uuid in tag_config['template_uuid']:
                overlapping_tags.append(tag_config)
                purge_tag_configs.append(tag_config)
                
        for purge_config in purge_tag_configs:
            reconciled_tags.remove(purge_config)
            
        for tag_config in reconciled_tags:
            if template_uuid in tag_config['template_uuid']:
                tag_config['config_status'] = 'OVERLAP'
            
        return reconciled_tags, overlapping_tags
        
     
    def run_diff(self, tag_uuid_list, overlapping_tags):
        
        #print('enter run_diff')
        #print('tag_uuid_list: ' + str(tag_uuid_list))
        #print('overlapping_tags: ' + str(overlapping_tags)) 
        
        # get template fields
        template_uuid = overlapping_tags[0]['template_uuid']
        template_config = self.read_template_config(template_uuid)
        dcu = dc.DataCatalogUtils(template_config['template_id'], template_config['project_id'], template_config['region'])
        template_fields = dcu.get_template()
        
        status = constants.TAGS_AGREE
        config_type = ""
        output_fields = []
        
        # for each template field
        for field in template_fields:
            field_id = field['field_id']
            field_type = field['field_type']
            field_values = []

            for tag in overlapping_tags:
                for tagged_field in tag['fields']:
                    if field_id in tagged_field['field_id']:
                        if tag['config_type'] in 'DYNAMIC':
                            config_type = constants.DYNAMIC_TAG
                            field_values.append(tagged_field['query_expression'])
                        if tag['config_type'] in 'STATIC':
                            field_values.append(tagged_field['field_value'])
                            config_type = constants.STATIC_TAG
                        #print('field_values: ' + str(field_values)) 
                        continue

            # we've collected all the values for a given field and added them to field_values
            # values assigned to a field are all equal if the set has one element 
            if len(set(field_values)) == 1:
                # field values all match
                if config_type == constants.DYNAMIC_TAG:
                    matching_field = {'field_id': field_id, 'field_type': field_type, 'status': 'AGREE', 'query_expression': field_values[0]}
                if config_type == constants.STATIC_TAG:
                    matching_field = {'field_id': field_id, 'field_type': field_type, 'status': 'AGREE', 'field_value': field_values[0]}
                output_fields.append(matching_field) 
            else:      
                if len(field_values) > 0:
                    if config_type == constants.DYNAMIC_TAG:
                        conflicting_field = {'field_id': field_id, 'field_type': field_type, 'status': 'CONFLICT', 'query_expression': ', '.join(field_values)}
                    if config_type == constants.STATIC_TAG:
                        conflicting_field = {'field_id': field_id, 'field_type': field_type, 'status': 'CONFLICT', 'field_value': ', '.join(field_values)}                        
                    output_fields.append(conflicting_field)
                    status = constants.TAGS_CONFLICT
        
        #print('output_fields: ' + str(output_fields))
        
        agreeing_tag = [] # output
        conflicting_tag = [] # output
        
        source_res_list = self.extract_source_res_list(overlapping_tags)
        overlapping_tags[0]['source_res'] = source_res_list
        overlapping_tags[0]['fields'] = output_fields
                        
        if status == constants.TAGS_CONFLICT:
            conflicting_tag = overlapping_tags[0]
        
        if status == constants.TAGS_AGREE:
            agreeing_tag = overlapping_tags[0]
        
        return agreeing_tag, conflicting_tag 
 

    def extract_tagged_columns(self, source_res_list, view_res, included_uris):
                
        #print('enter extract_tagged_columns')
        #print('source_res_list: ' + str(source_res_list))
        #print('view_res: ' + view_res)
        #print('included: ' + included_uris)
        
        view_res_split = view_res.split("/")
        project = view_res_split[0]
        dataset = view_res_split[2]
        view = view_res.split("/")[4]
        
        tagged_columns = []
        
        for source_res in source_res_list:
            
            print('source_res: ' + source_res)
            
            source_res_full = "bigquery/project/" + source_res.replace('datasets', 'dataset').replace('tables/', '')
            
            print('source_res_full: ' + source_res_full)
        
            included_uri_split = included_uris.split(",")
        
            for included_uri in included_uri_split:
            
                uri = included_uri.strip()
                print('uri: ' + uri)
            
                # we may have a column
                if len(uri) > len(source_res_full):
                    start_index = uri.rfind("/")
                    column = uri[start_index+1:]
                    exists = self.column_exists(project, dataset, view, column)
                
                    if exists: 
                        tagged_columns.append(column)
        
        return tagged_columns
    
    
    def column_exists(self, project, dataset, view, column):
        
        column_exists = False
    
        query_str = """
                select count(*) as count
                from `""" + project + "." + dataset + "." + """INFORMATION_SCHEMA.COLUMN_FIELD_PATHS`
                where table_name='""" + view + """'
                and column_name='""" + column + """'
                """

        #print("query_str: " + query_str)
        
        bq_client = bigquery.Client(project=project)      
        query_job = bq_client.query(query_str)

        for row in query_job:
            count = row["count"] 
            
            if count == 1:
                column_exists = True

        return column_exists
        

    def read_tag_configs_on_res(self, res):
        
        #print("*** enter read_tag_configs_on_res ***")
            
        template_uuid_set = set()
        
        tag_config_results = []
        
        table_path_full = res.replace('.', '/datasets/', 1).replace('.', '/tables/', 1)
        #print('table_path_full: ' + table_path_full)
         
        log_ref = self.db.collection('logs')
        query = log_ref.where('res', '==', table_path_full).where('config_type', '==', 'MANUAL').order_by('ts', direction=firestore.Query.DESCENDING)
        
        create_update_entries = query.stream()
        
        for create_update_entry in create_update_entries:
                 
            entry = create_update_entry.to_dict()
            template_uuid = entry['template_uuid']
            
            if template_uuid not in template_uuid_set:
                template_uuid_set.add(template_uuid) 
                
                tag_uuid = entry['tag_uuid']
                tag_config = self.read_tag_config(tag_uuid)
                
                template_config = self.read_template_config(template_uuid)
                tag_config['template_id'] = template_config['template_id']
                
                tag_config_results.append(tag_config)
            else:
                continue
                
        return tag_config_results

    
if __name__ == '__main__':
    
    config = configparser.ConfigParser()
    config.read("tagengine.ini")
    
    te = TagEngineUtils();
    te.write_template('quality_template', config['DEFAULT']['PROJECT'], config['DEFAULT']['REGION'], 'ACTIVE')