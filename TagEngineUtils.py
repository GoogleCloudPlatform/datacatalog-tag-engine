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
      
      
    def update_config_status(self, config_uuid, config_type, status):
    
        coll_name = self.get_config_collection(config_type)
        self.db.collection(coll_name).document(config_uuid).update({
            'config_status': status
        })
    
    def update_scheduling_status(self, config_uuid, config_type, status):
    
        coll_name = self.get_config_collection(config_type)
        config_ref = self.db.collection(coll_name).document(config_uuid)
        doc = config_ref.get()
        
        if doc.exists:
            config = doc.to_dict()
            
            if 'scheduling_status' in config:
                config_ref.update({'scheduling_status': status})
                         
    
    def increment_version_next_run(self, config_uuid, config_type):
        
        config = self.read_config(config_uuid, config_type)
        
        version = config.get('version', 0) + 1
        delta = config.get('refresh_frequency', 24)
        unit = config.get('refresh_unit', 'hours')
        
        if unit == 'minutes':
            next_run = datetime.utcnow() + timedelta(minutes=delta)
        elif unit == 'hours':
            next_run = datetime.utcnow() + timedelta(hours=delta)
        if unit == 'days':
            next_run = datetime.utcnow() + timedelta(days=delta)
        
        coll_name = self.get_config_collection(config_type)
        self.db.collection(coll_name).document(config_uuid).update({
            'version': version,
            'next_run' : next_run
        })
            
    def update_overwrite_flag(self, config_uuid, config_type):
        
        coll_name = self.get_config_collection(config_type)
        self.db.collection(coll_name).document(config_uuid).update({
            'overwrite': False
        })
                                                                             
    def read_template_config(self, template_uuid):
                
        template_ref = self.db.collection('tag_templates').document(template_uuid)
        doc = template_ref.get()
        
        if doc.exists:
            template_config = doc.to_dict()
            #print(str(config))
            
        return template_config
    
    
    def read_tag_template(self, template_id, project_id, region):
        
        template_exists = False
        template_uuid = ""
        
        # check to see if this template already exists
        template_ref = self.db.collection('tag_templates')
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
                     
            doc_ref = self.db.collection('tag_templates').document(template_uuid)
            doc_ref.set({
                'template_uuid': template_uuid, 
                'template_id': template_id,
                'project_id': project_id,
                'region': region
            })
                                   
        return template_uuid
        
        
    def write_static_config(self, config_status, fields, included_uris, excluded_uris, template_uuid, \
                            refresh_mode, refresh_frequency, refresh_unit, tag_history, tag_stream, overwrite=False):
        
        # hash the included_uris string
        included_uris_hash = hashlib.md5(included_uris.encode()).hexdigest()
        
        # check to see if this static config already exists
        configs_ref = self.db.collection('static_configs')
        query = configs_ref.where('template_uuid', '==', template_uuid).where('included_uris_hash', '==',\
                            included_uris_hash).where('config_type', '==', 'STATIC').where('config_status', '!=', 'INACTIVE')
       
        matches = query.get()
       
        for match in matches:
            if match.exists:
                config_uuid_match = match.id
                #print('Static config already exists. Config_uuid: ' + str(config_uuid_match))
                
                # update status to INACTIVE 
                self.db.collection('static_configs').document(config_uuid_match).update({
                    'config_status' : "INACTIVE"
                })
                #print('Updated config status to INACTIVE.')
        
        config_uuid = uuid.uuid1().hex
        
        if refresh_mode == 'AUTO':
            
            delta, next_run = self.validate_auto_refresh(refresh_frequency, refresh_unit)
       
            config = self.db.collection('static_configs')
            doc_ref = config.document(config_uuid)
            doc_ref.set({
                'config_uuid': config_uuid,
                'config_type': 'STATIC',
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
                'version': 1,
                'overwrite': overwrite
            })
            
        else:
            
            config = self.db.collection('static_configs')
            doc_ref = config.document(config_uuid)
            doc_ref.set({
                'config_uuid': config_uuid,
                'config_type': 'STATIC',
                'config_status': config_status, 
                'creation_time': datetime.utcnow(), 
                'fields': fields,
                'included_uris': included_uris,
                'included_uris_hash': included_uris_hash,
                'excluded_uris': excluded_uris,
                'template_uuid': template_uuid,
                'refresh_mode': refresh_mode, # ON_DEMAND refresh mode
                'refresh_frequency': 0, # N/A
                'tag_history': tag_history,
                'tag_stream': tag_stream,
                'version': 1,
                'overwrite': overwrite
            })
        
        return config_uuid, included_uris_hash
    
    
    def write_dynamic_config(self, config_status, fields, included_uris, excluded_uris, template_uuid, refresh_mode,\
                          refresh_frequency, refresh_unit, tag_history, tag_stream):
        
        included_uris_hash = hashlib.md5(included_uris.encode()).hexdigest()
        
        # check to see if this config already exists
        configs_ref = self.db.collection('dynamic_configs')
        query = configs_ref.where('template_uuid', '==', template_uuid).where('included_uris_hash', '==', included_uris_hash).where('config_type', '==', 'DYNAMIC').where('config_status', '!=', 'INACTIVE')
       
        matches = query.get()
       
        for match in matches:
            if match.exists:
                config_uuid_match = match.id
                #print('Config already exists. Config_uuid: ' + str(config_uuid_match))
                
                # update status to INACTIVE 
                self.db.collection('dynamic_configs').document(config_uuid_match).update({
                    'config_status' : "INACTIVE"
                })
                print('Updated status to INACTIVE.')
       
        config_uuid = uuid.uuid1().hex
        config = self.db.collection('dynamic_configs')
        doc_ref = config.document(config_uuid)
        
        if refresh_mode == 'AUTO':
            
            delta, next_run = self.validate_auto_refresh(refresh_frequency, refresh_unit)
            
            doc_ref.set({
                'config_uuid': config_uuid,
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
                'config_uuid': config_uuid,
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
        
        return config_uuid, included_uris_hash
  
        
    def validate_auto_refresh(self, refresh_frequency, refresh_unit):
        
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
        
        if refresh_unit == 'minutes':
            next_run = datetime.utcnow() + timedelta(minutes=delta)    
        elif refresh_unit == 'hours':
            next_run = datetime.utcnow() + timedelta(hours=delta)
        elif refresh_unit == 'days':
            next_run = datetime.utcnow() + timedelta(days=delta)
        else:
            next_run = datetime.utcnow() + timedelta(days=delta) # default to days
            
        return delta, next_run
    
    
    def write_entry_config(self, config_status, fields, included_uris, excluded_uris, template_uuid, refresh_mode,\
                           refresh_frequency, refresh_unit, tag_history, tag_stream):
        
        print('** enter write_entry_config **')
        print('refresh_mode: ', refresh_mode)
        print('refresh_frequency: ', refresh_frequency)
        print('refresh_unit: ', refresh_unit)
        
        included_uris_hash = hashlib.md5(included_uris.encode()).hexdigest()
        
        # check to see if this config already exists
        configs_ref = self.db.collection('entry_configs')
        query = configs_ref.where('template_uuid', '==', template_uuid).where('included_uris_hash', '==', included_uris_hash).where('config_type', '==', 'ENTRY').where('config_status', '!=', 'INACTIVE')
       
        matches = query.get()
       
        for match in matches:
            if match.exists:
                config_uuid_match = match.id
                #print('Tag config already exists. Tag_uuid: ' + str(config_uuid_match))
                
                # update status to INACTIVE 
                self.db.collection('entry_configs').document(config_uuid_match).update({
                    'config_status' : "INACTIVE"
                })
                print('Updated status to INACTIVE.')
       
        config_uuid = uuid.uuid1().hex
        config = self.db.collection('entry_configs')
        doc_ref = config.document(config_uuid)
        
        if refresh_mode == 'AUTO':
            
            delta, next_run = self.validate_auto_refresh(refresh_frequency, refresh_unit)
            
            doc_ref.set({
                'config_uuid': config_uuid,
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
                'config_uuid': config_uuid,
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
        
        return config_uuid, included_uris_hash

    
    def write_glossary_config(self, config_status, fields, mapping_table, included_uris, excluded_uris, template_uuid, \
                             refresh_mode, refresh_frequency, refresh_unit, tag_history, tag_stream, overwrite=False):
        
        print('** enter write_glossary_config **')
        
        included_uris_hash = hashlib.md5(included_uris.encode()).hexdigest()
        
        # check to see if this config already exists
        configs_ref = self.db.collection('glossary_configs')
        query = configs_ref.where('template_uuid', '==', template_uuid).where('included_uris_hash', '==', included_uris_hash).where('config_type', '==', 'GLOSSARY').where('config_status', '!=', 'INACTIVE')
       
        matches = query.get()
       
        for match in matches:
            if match.exists:
                config_uuid_match = match.id
                #print('config already exists. Found config_uuid: ' + str(config_uuid_match))
                
                # update status to INACTIVE 
                self.db.collection('glossary_configs').document(config_uuid_match).update({
                    'config_status' : "INACTIVE"
                })
                print('Updated status to INACTIVE.')
       
        config_uuid = uuid.uuid1().hex
        config = self.db.collection('glossary_configs')
        doc_ref = config.document(config_uuid)
        
        if refresh_mode == 'AUTO':
            
            delta, next_run = self.validate_auto_refresh(refresh_frequency, refresh_unit)
            
            doc_ref.set({
                'config_uuid': config_uuid,
                'config_type': 'GLOSSARY',
                'config_status': config_status, 
                'creation_time': datetime.utcnow(), 
                'fields': fields,
                'mapping_table': mapping_table,
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
                'version': 1,
                'overwrite': overwrite
            })
            
        else:
            doc_ref.set({
                'config_uuid': config_uuid,
                'config_type': 'GLOSSARY',
                'config_status': config_status, 
                'creation_time': datetime.utcnow(), 
                'fields': fields,
                'mapping_table': mapping_table,
                'included_uris': included_uris,
                'included_uris_hash': included_uris_hash,
                'excluded_uris': excluded_uris,
                'template_uuid': template_uuid,
                'refresh_mode': refresh_mode, # ON_DEMAND refresh mode
                'refresh_frequency': 0,
                'tag_history': tag_history,
                'tag_stream': tag_stream,
                'version': 1,
                'overwrite': overwrite
            })
        
        print('Created new glossary config.')
        
        return config_uuid, included_uris_hash


    def write_sensitive_config(self, config_status, fields, dlp_dataset, mapping_table, included_uris, excluded_uris, \
                               create_policy_tags, taxonomy_id, \
                               template_uuid, \
                               refresh_mode, refresh_frequency, refresh_unit, tag_history, tag_stream, overwrite=False):
        
        print('** enter write_sensitive_config **')
        
        included_uris_hash = hashlib.md5(included_uris.encode()).hexdigest()
        
        # check to see if this config already exists
        configs_ref = self.db.collection('sensitive_configs')
        query = configs_ref.where('template_uuid', '==', template_uuid).where('included_uris_hash', '==', included_uris_hash).where('config_type', '==', 'SENSITIVE').where('config_status', '!=', 'INACTIVE')
       
        matches = query.get()
       
        for match in matches:
            if match.exists:
                config_uuid_match = match.id
                #print('config already exists. Found config_uuid: ' + str(config_uuid_match))
                
                # update status to INACTIVE 
                self.db.collection('sensitive_configs').document(config_uuid_match).update({
                    'config_status' : "INACTIVE"
                })
                print('Updated status to INACTIVE.')
       
        config_uuid = uuid.uuid1().hex
        configs = self.db.collection('sensitive_configs')
        doc_ref = configs.document(config_uuid)
        
        if refresh_mode == 'AUTO':
            
            delta, next_run = self.validate_auto_refresh(refresh_frequency, refresh_unit)
            
            doc_ref.set({
                'config_uuid': config_uuid,
                'config_type': 'SENSITIVE',
                'config_status': config_status, 
                'creation_time': datetime.utcnow(), 
                'fields': fields,
                'dlp_dataset': dlp_dataset,
                'mapping_table': mapping_table,
                'included_uris': included_uris,
                'included_uris_hash': included_uris_hash,
                'excluded_uris': excluded_uris,
                'create_policy_tags': create_policy_tags, 
                'taxonomy_id': taxonomy_id,
                'template_uuid': template_uuid,
                'refresh_mode': refresh_mode, # AUTO refresh mode
                'refresh_frequency': delta,
                'refresh_unit': refresh_unit,
                'tag_history': tag_history,
                'tag_stream': tag_stream,
                'scheduling_status': 'PENDING',
                'next_run': next_run,
                'version': 1,
                'overwrite': overwrite
            })
            
        else:
            doc_ref.set({
                'config_uuid': config_uuid,
                'config_type': 'SENSITIVE',
                'config_status': config_status, 
                'creation_time': datetime.utcnow(), 
                'fields': fields,
                'dlp_dataset': dlp_dataset,
                'mapping_table': mapping_table,
                'included_uris': included_uris,
                'included_uris_hash': included_uris_hash,
                'excluded_uris': excluded_uris,
                'create_policy_tags': create_policy_tags, 
                'taxonomy_id': taxonomy_id,
                'template_uuid': template_uuid,
                'refresh_mode': refresh_mode, # ON_DEMAND refresh mode
                'refresh_frequency': 0,
                'tag_history': tag_history,
                'tag_stream': tag_stream,
                'version': 1,
                'overwrite': overwrite
            })
        
        print('Created new sensitive config.')
        
        return config_uuid, included_uris_hash

    
    def write_restore_config(self, config_status, source_template_uuid, source_template_id, source_template_project, source_template_region, \
                             target_template_uuid, target_template_id, target_template_project, target_template_region, \
                             metadata_export_location, tag_history, tag_stream, overwrite=False):
                                    
        print('** write_restore_config **')
        
        # check to see if this config already exists
        configs_ref = self.db.collection('restore_configs')
        query = configs_ref.where('source_template_uuid', '==', source_template_uuid).where('target_template_uuid', '==', target_template_uuid).where('config_status', '!=', 'INACTIVE')
       
        matches = query.get()
       
        for match in matches:
            if match.exists:
                config_uuid_match = match.id
                print('config already exists. Found config_uuid: ' + str(config_uuid_match))
                
                # update status to INACTIVE 
                self.db.collection('restore_configs').document(config_uuid_match).update({
                    'config_status' : "INACTIVE"
                })
                print('Updated status to INACTIVE.')
       
        config_uuid = uuid.uuid1().hex
        configs = self.db.collection('restore_configs')
        doc_ref = configs.document(config_uuid)
        
        doc_ref.set({
            'config_uuid': config_uuid,
            'config_type': 'RESTORE',
            'config_status': config_status, 
            'creation_time': datetime.utcnow(), 
            'source_template_uuid': source_template_uuid,
            'source_template_id': source_template_id, 
            'source_template_project': source_template_project,
            'source_template_region': source_template_region,
            'target_template_uuid': target_template_uuid,
            'target_template_id': target_template_id,
            'target_template_project': target_template_project,
            'target_template_region': target_template_region,
            'metadata_export_location': metadata_export_location,
            'tag_history': tag_history,
            'tag_stream': tag_stream,
            'overwrite': overwrite
        })
        
        return config_uuid
        

    def write_import_config(self, config_status, template_uuid, template_id, template_project, template_region, \
                            metadata_import_location, tag_history, tag_stream, overwrite=False):
                                    
        print('** write_import_config **')
        
        # check to see if this config already exists
        configs_ref = self.db.collection('import_configs')
        query = configs_ref.where('template_uuid', '==', template_uuid).where('metadata_import_location', '==', metadata_import_location).where('config_status', '!=', 'INACTIVE')
       
        matches = query.get()
       
        for match in matches:
            if match.exists:
                config_uuid_match = match.id
                print('config already exists. Found config_uuid: ' + str(config_uuid_match))
                
                # update status to INACTIVE 
                self.db.collection('import_configs').document(config_uuid_match).update({
                    'config_status' : "INACTIVE"
                })
                print('Updated status to INACTIVE.')
       
        config_uuid = uuid.uuid1().hex
        configs = self.db.collection('import_configs')
        doc_ref = configs.document(config_uuid)
        
        doc_ref.set({
            'config_uuid': config_uuid,
            'config_type': 'IMPORT',
            'config_status': config_status, 
            'creation_time': datetime.utcnow(), 
            'template_uuid': template_uuid,
            'template_id': template_id, 
            'template_project': template_project,
            'template_region': template_region,
            'metadata_import_location': metadata_import_location,
            'tag_history': tag_history,
            'tag_stream': tag_stream,
            'overwrite': overwrite
        })
        
        return config_uuid

         
    def write_log_entry(self, dc_op, resource_type, resource, column, config_type, config_uuid, tag_id, template_uuid):
                    
        log_entry = {}
        log_entry['ts'] = datetime.utcnow()
        log_entry['dc_op'] = dc_op
        log_entry['res_type'] = resource_type
        log_entry['config_type'] = 'MANUAL'
        log_entry['res'] = resource
        
        if len(column) > 0:
            log_entry['col'] = column
        
        log_entry['config_type'] = config_type
        log_entry['config_uuid'] = config_uuid
        log_entry['dc_tag_id'] = tag_id
        log_entry['template_uuid'] = template_uuid

        self.db.collection('logs').add(log_entry)
        #print('Wrote log entry.')
        
    
    def write_tag_op_error(self, op, config_uuid, config_type, msg):
                    
        error = {}
        error['ts'] = datetime.utcnow()
        error['op'] = op
        error['config_uuid'] = config_uuid
        error['config_type'] = config_type
        error['msg'] = msg
        
        self.db.collection('tag_op_error').add(error)
        #print('Wrote error entry.')


    def write_tag_value_error(self, msg):
                    
        error = {}
        error['ts'] = datetime.utcnow()
        error['error'] = msg
        
        self.db.collection('tag_value_error').add(error)
        #print('Wrote error entry.')    
    
    
    def get_config_collection(self, config_type):
        
        if config_type == 'STATIC':
            coll = 'static_configs'
        if config_type == 'DYNAMIC':
            coll = 'dynamic_configs'   
        if config_type == 'ENTRY':
            coll = 'entry_configs' 
        if config_type == 'GLOSSARY':
            coll = 'glossary_configs'
        if config_type == 'SENSITIVE':
            coll = 'sensitive_configs'
        if config_type == 'RESTORE':
            coll = 'restore_configs'
        if config_type == 'IMPORT':
            coll = 'import_configs'
        
        return coll
    
    
    def read_configs(self, template_id, project_id, region, config_type='ALL'):
        
        colls = []
        config_results = []
        
        if config_type == 'ALL':
            colls = ['dynamic_configs', 'static_configs', 'entry_configs', 'glossary_configs', 'sensitive_configs', 'restore_configs', 'import_configs']
        else:
            colls.append(self.get_config_collection(config_type))
        
        template_exists, template_uuid = self.read_tag_template(template_id, project_id, region)
        
        for coll_name in colls:
            configs_ref = self.db.collection(coll_name)
            
            if coll_name == 'restore_configs':
                docs = configs_ref.where('target_template_uuid', '==', template_uuid).where('config_status', '!=', 'INACTIVE').stream()
            else:
                docs = configs_ref.where('template_uuid', '==', template_uuid).where('config_status', '!=', 'INACTIVE').stream()
                
            for doc in docs:
                config = doc.to_dict()
                config_results.append(config)  
                
        #print(str(configs))
        return config_results
        
    
    def read_config(self, config_uuid, config_type):
                
        config_result = {}
        coll_name = self.get_config_collection(config_type)
        
        config_ref = self.db.collection(coll_name).document(config_uuid)
        doc = config_ref.get()
        
        if doc.exists:
            config_result = doc.to_dict()
            
        return config_result
        
      
    def read_ready_configs(self):
        
        ready_configs = []
        colls = ['static_configs', 'dynamic_configs', 'entry_configs', 'glossary_configs']
        
        for coll_name in colls:
            config_ref = self.db.collection(coll_name)
            config_ref = config_ref.where("refresh_mode", "==", "AUTO")
            config_ref = config_ref.where("scheduling_status", "==", "READY")
            config_ref = config_ref.where("config_status", "==", "ACTIVE")
            config_ref = config_ref.where("next_run", "<=", datetime.utcnow())
        
            config_stream = list(config_ref.stream())
        
            for ready_config in config_stream:
            
                config_dict = ready_config.to_dict()
                ready_configs.append((config_dict['config_uuid'], config_dict['config_type']))
            
        return ready_configs  
        
        
    def lookup_config_by_included_uris(self, template_uuid, included_uris, included_uris_hash):
        
        success = False
        config_result = {}
        
        colls = ['dynamic_configs', 'static_configs', 'entry_configs', 'glossary_configs', 'sensitive_configs']
        
        for coll_name in colls:
            
            config_ref = self.db.collection(coll_name)
        
            if included_uris is not None:
                docs = config_ref.where('template_uuid', '==', template_uuid).where('config_status', '==', 'ACTIVE')\
                .where('included_uris', '==', included_uris).stream()
            if included_uris_hash is not None:
                docs = config_ref.where('template_uuid', '==', template_uuid).where('config_status', '==', 'ACTIVE')\
                .where('included_uris_hash', '==', included_uris_hash).stream()
        
            for doc in docs:
                config_result = doc.to_dict()
                success = True
                return success, config_result
                
        
        return success, config_result
        
    
    def update_config(self, old_config_uuid, config_type, config_status, fields, included_uris, excluded_uris, template_uuid, \
                      refresh_mode, refresh_frequency, refresh_unit, tag_history, tag_stream, overwrite=False, mapping_table=None):
        
        coll_name = self.get_config_collection(config_type)
        self.db.collection(coll_name).document(old_config_uuid).update({
            'config_status' : "INACTIVE"
        })
        
        if config_type == 'STATIC':
            new_config_uuid, included_uris_hash = self.write_static_config(config_status, fields, included_uris, excluded_uris, template_uuid, \
                                                                           refresh_mode, refresh_frequency, refresh_unit, \
                                                                           tag_history, tag_stream, overwrite)
        
        if config_type == 'DYNAMIC':
            new_config_uuid, included_uris_hash = self.write_dynamic_config(config_status, fields, included_uris, excluded_uris, \
                                                                            template_uuid, refresh_mode, refresh_frequency, refresh_unit,\
                                                                            tag_history, tag_stream)
        if config_type == 'ENTRY':
            new_config_uuid, included_uris_hash = self.write_entry_config(config_status, fields, included_uris, excluded_uris, \
                                                                          template_uuid, refresh_mode, refresh_frequency, refresh_unit,\
                                                                          tag_history, tag_stream)
                                                                     
        if config_type == 'GLOSSARY':
            new_config_uuid, included_uris_hash = self.write_glossary_config(config_status, fields, mapping_table, included_uris, excluded_uris, \
                                                                            template_uuid, refresh_mode, refresh_frequency, refresh_unit,\
                                                                            tag_history, tag_stream, overwrite)
        # note: no need to return the included_uris_hash
            
        return new_config_uuid
    

    def update_sensitive_config(self, old_config_uuid, config_status, dlp_dataset, mapping_table, included_uris, excluded_uris, 
                                create_policy_tags, taxonomy_id, template_uuid, refresh_mode, refresh_frequency, refresh_unit, 
                                tag_history, tag_stream, overwrite):
        
        self.db.collection('sensitive_configs').document(old_config_uuid).update({
            'config_status' : "INACTIVE"
        })
        
        config = self.read_config(old_config_uuid, 'SENSITIVE')
        
        new_config_uuid, included_uris_hash = self.write_sensitive_config(config_status, config['fields'], dlp_dataset, mapping_table, included_uris, excluded_uris, \
                                                                          create_policy_tags, taxonomy_id, template_uuid, \
                                                                          refresh_mode, refresh_frequency, refresh_unit, tag_history, tag_stream, overwrite)
        
        
        return new_config_uuid
                     

    def update_restore_config(self, old_config_uuid, config_status, source_template_uuid, source_template_id, source_template_project, 
                              source_template_region, target_template_uuid, target_template_id, target_template_project, \
                              target_template_region, metadata_export_location, tag_history, tag_stream, overwrite=False):
        
        self.db.collection('restore_configs').document(old_config_uuid).update({
            'config_status' : "INACTIVE"
        })
        
        new_config_uuid = self.write_restore_config(config_status, source_template_uuid, source_template_id, source_template_project, \
                                                    source_template_region, target_template_uuid, target_template_id, \
                                                    target_template_project, target_template_region, \
                                                    metadata_export_location, tag_history, tag_stream, overwrite=False)
                    
        return new_config_uuid
    
    
if __name__ == '__main__':
    
    config = configparser.ConfigParser()
    config.read("tagengine.ini")
    
    te = TagEngineUtils();
    te.write_template('quality_template', config['DEFAULT']['PROJECT'], config['DEFAULT']['REGION'], 'ACTIVE')