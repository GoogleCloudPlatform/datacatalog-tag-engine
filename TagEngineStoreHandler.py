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

import uuid, pytz, os, requests
import configparser, difflib, hashlib
from datetime import datetime
from datetime import timedelta
from google.api_core.gapic_v1.client_info import ClientInfo
from google.cloud import bigquery
from google.cloud import firestore
from google.cloud.firestore_v1.base_query import FieldFilter
import DataCatalogController as controller
import ConfigType as ct
import constants

USER_AGENT = 'cloud-solutions/datacatalog-tag-engine-v2'

class TagEngineStoreHandler:
    
    def __init__(self):
        
        self.db = firestore.Client(client_info=ClientInfo(user_agent=USER_AGENT))
        
        config = configparser.ConfigParser()
        config.read("tagengine.ini")
        
    def read_default_settings(self, user_email):
        
        settings = {}
        exists = False
        
        doc_ref = self.db.collection('settings').document(user_email)
        doc = doc_ref.get()
        
        if doc.exists:
            settings = doc.to_dict()
            exists = True 
        
        return exists, settings
        
    
    def write_default_settings(self, user_email, template_id, template_project, template_region, service_account):
        
        settings = self.db.collection('settings')
        doc_ref = settings.document(user_email)
        doc_ref.set({
            'template_id': template_id,
            'template_project':  template_project,
            'template_region': template_region,
            'service_account': service_account
        })
        
        print('Saved default settings.')
    

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

    
    def write_tag_history_settings(self, enabled, bigquery_project, bigquery_region, bigquery_dataset):
        
        write_status = True
        history_settings = self.db.collection('settings')
        doc_ref = history_settings.document('tag_history')
        
        try:
            doc_ref.set({
                'enabled': bool(enabled),
                'bigquery_project': bigquery_project,
                'bigquery_region':  bigquery_region,
                'bigquery_dataset': bigquery_dataset
            })
        except Exception as e:
            print('Error occurred during write_tag_history_settings:', e)
            write_status = False
        
        return write_status


    def write_job_metadata_settings(self, enabled, bigquery_project, bigquery_region, bigquery_dataset):
        
        write_status = True
        settings = self.db.collection('settings')
        doc_ref = settings.document('job_metadata')
        
        try:
            doc_ref.set({
                'enabled': bool(enabled),
                'bigquery_project': bigquery_project,
                'bigquery_region':  bigquery_region,
                'bigquery_dataset': bigquery_dataset
            })
        except Exception as e:
            print('Error occurred during write_job_metadata_settings:', e)
            write_status = False
        
        return write_status
        
    
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
    
        
    def read_job_metadata_settings(self):
        
        settings = {}
        exists = False
        
        doc_ref = self.db.collection('settings').document('job_metadata')
        doc = doc_ref.get()
        
        if doc.exists:
            settings = doc.to_dict()
            exists = True 
        
        return exists, settings

    
    def write_coverage_report_settings(self, included_bigquery_projects, excluded_bigquery_datasets, excluded_bigquery_tables):
        
        report_settings = self.db.collection('settings')
        doc_ref = report_settings.document('coverage_report')
        doc_ref.set({
            'included_bigquery_projects': included_bigquery_projects,
            'excluded_bigquery_datasets':  excluded_bigquery_datasets,
            'excluded_bigquery_tables': excluded_bigquery_tables
        })
        
        print('Saved coverage report settings.')
        
        
    def generate_coverage_report(self, credentials):    
    
        summary_report = []
        detailed_report = []
        
        exists, settings = self.read_coverage_report_settings()
        included_bigquery_projects = settings['included_bigquery_projects']
        excluded_bigquery_datasets = settings['excluded_bigquery_datasets']
        excluded_bigquery_tables = settings['excluded_bigquery_tables']
        
        print('included_bigquery_projects: ' + included_bigquery_projects)
        print('excluded_bigquery_datasets: ' + excluded_bigquery_datasets)
        print('excluded_bigquery_tables: ' + excluded_bigquery_tables)
        
        log_ref = self.db.collection('logs')
        
        # list datasets and tables for chosen projects
        for project in included_bigquery_projects.split(','):
            project_id = project.strip()
            bq_client = bigquery.Client(project=project_id)
            datasets = list(bq_client.list_datasets())
            
            total_tags = 0
            
            for dataset in datasets:
                
                dataset_id = dataset.dataset_id

                if project_id + "." + dataset_id in excluded_bigquery_datasets:
                    #print('skipping ' + project_id + "." + dataset_id)
                    continue
               
                print("dataset: " + dataset_id)
                
                qualified_dataset = project_id + "." + dataset_id
                overall_sum = 0    
                table_list = []
                tables = list(bq_client.list_tables(dataset_id))
                
                dcc = controller.DataCatalogController(credentials)
                linked_resources = dcc.search_catalog(project_id, dataset_id)
                
                print('linked_resources: ' + str(linked_resources))
            
                for table in tables:
                    print("full_table_id: " + str(table.full_table_id))
                
                    table_path_full = table.full_table_id.replace(':', '/datasets/').replace('.', '/tables/')
                    table_path_short = table.full_table_id.replace(':', '.')
                    table_name = table_path_full.split('/')[4]
                
                    print('table_path_full: ' + table_path_full)
                    print('table_path_short: ' + table_path_short)
                    print('table_name: ' + table_name)
                
                    if table_path_short in project_id + '.' + excluded_bigquery_tables:
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
      
      
    def check_active_config(self, config_uuid, config_type):
        
        coll_name = self.lookup_config_collection(config_type)
        doc_ref = self.db.collection(coll_name).document(config_uuid)
        
        if doc_ref.get().exists:
            return True
        else:
            return False
            
      
    def update_job_status(self, config_uuid, config_type, status):
    
        coll_name = self.lookup_config_collection(config_type)
        self.db.collection(coll_name).document(config_uuid).update({
            'job_status': status
        })
    
    
    def update_scheduling_status(self, config_uuid, config_type, status):
    
        coll_name = self.lookup_config_collection(config_type)
        config_ref = self.db.collection(coll_name).document(config_uuid)
        doc = config_ref.get()
        
        if doc.exists:
            config = doc.to_dict()
            
            if 'scheduling_status' in config:
                config_ref.update({'scheduling_status': status})
                         
    
    def increment_version_next_run(self, service_account, config_uuid, config_type):
        
        config = self.read_config(service_account, config_uuid, config_type)
        
        version = config.get('version', 0) + 1
        delta = config.get('refresh_frequency', 24)
        unit = config.get('refresh_unit', 'hours')
        
        if unit == 'minutes':
            next_run = datetime.utcnow() + timedelta(minutes=delta)
        elif unit == 'hours':
            next_run = datetime.utcnow() + timedelta(hours=delta)
        if unit == 'days':
            next_run = datetime.utcnow() + timedelta(days=delta)
        
        coll_name = self.lookup_config_collection(config_type)
        self.db.collection(coll_name).document(config_uuid).update({
            'version': version,
            'next_run' : next_run
        })
                                                                                  
    def read_tag_template_config(self, template_uuid):
                
        template_ref = self.db.collection('tag_templates').document(template_uuid)
        doc = template_ref.get()
        
        if doc.exists:
            template_config = doc.to_dict()
            #print(str(config))
            
        return template_config
    
    
    def read_tag_template(self, template_id, template_project, template_region):
        
        template_exists = False
        template_uuid = ""
        
        # check to see if this template already exists
        template_ref = self.db.collection('tag_templates')
        query = template_ref.where(filter=FieldFilter('template_id', '==', template_id))
        query = query.where(filter=FieldFilter('template_project', '==', template_project))
        query = query.where(filter=FieldFilter('template_region', '==', template_region))
        
        matches = query.get()
        
        # should either be a single matching template or no matching templates
        if len(matches) == 1:
            if matches[0].exists:
                print('Tag Template exists. Template uuid: ' + str(matches[0].id))
                template_uuid = matches[0].id
                template_exists = True
        
        return (template_exists, template_uuid)
        
    
    def write_tag_template(self, template_id, template_project, template_region):
        
        template_exists, template_uuid = self.read_tag_template(template_id, template_project, template_region)
        
        if template_exists == False:    
            print('tag template {} doesn\'t exist. Creating new template'.format(template_id))
             
            template_uuid = uuid.uuid1().hex
                     
            doc_ref = self.db.collection('tag_templates').document(template_uuid)
            doc_ref.set({
                'template_uuid': template_uuid, 
                'template_id': template_id,
                'template_project': template_project,
                'template_region': template_region
            })
                                   
        return template_uuid
        
        
    def write_static_asset_config(self, service_account, fields, included_assets_uris, excluded_assets_uris, template_uuid, \
                                  template_id, template_project, template_region, refresh_mode, refresh_frequency, refresh_unit, \
                                  tag_history, overwrite=False):
        
        print('*** enter write_static_asset_config ***')
        
        # hash the included_assets_uris string
        included_assets_uris_hash = hashlib.md5(included_assets_uris.encode()).hexdigest()
        
        # check to see if a static config already exists
        configs_ref = self.db.collection('static_asset_configs')
        query = configs_ref.where(filter=FieldFilter('template_uuid', '==', template_uuid))
        query = query.where(filter=FieldFilter('included_assets_uris_hash', '==', included_assets_uris_hash))
        query = query.where(filter=FieldFilter('config_type', '==', 'STATIC_TAG_ASSET'))
        query = query.where(filter=FieldFilter('config_status', '!=', 'INACTIVE'))
       
        matches = query.get()
       
        for match in matches:
            if match.exists:
                config_uuid_match = match.id
                print('Static config already exists. Config_uuid: ' + str(config_uuid_match))
                
                # update status to INACTIVE 
                self.db.collection('static_asset_configs').document(config_uuid_match).update({
                    'config_status' : "INACTIVE"
                })
                print('Updated config status to INACTIVE.')
        
        config_uuid = uuid.uuid1().hex
        
        if refresh_mode == 'AUTO':
            
            delta, next_run = self.validate_auto_refresh(refresh_frequency, refresh_unit)
       
            config = self.db.collection('static_asset_configs')
            doc_ref = config.document(config_uuid)
            doc_ref.set({
                'config_uuid': config_uuid,
                'config_type': 'STATIC_TAG_ASSET',
                'config_status': 'ACTIVE', 
                'creation_time': datetime.utcnow(), 
                'fields': fields,
                'included_assets_uris': included_assets_uris,
                'included_assets_uris_hash': included_assets_uris_hash,
                'excluded_assets_uris': excluded_assets_uris,
                'template_uuid': template_uuid,
                'template_id': template_id,
                'template_project': template_project,
                'template_region': template_region,
                'refresh_mode': refresh_mode, # AUTO refresh mode
                'refresh_frequency': delta,
                'refresh_unit': refresh_unit,
                'tag_history': tag_history,
                'scheduling_status': 'ACTIVE',
                'next_run': next_run,
                'version': 1,
                'overwrite': overwrite,
                'service_account': service_account
            })
            
        else:
            
            config = self.db.collection('static_asset_configs')
            doc_ref = config.document(config_uuid)
            doc_ref.set({
                'config_uuid': config_uuid,
                'config_type': 'STATIC_TAG_ASSET',
                'config_status': 'ACTIVE', 
                'creation_time': datetime.utcnow(), 
                'fields': fields,
                'included_assets_uris': included_assets_uris,
                'included_assets_uris_hash': included_assets_uris_hash,
                'excluded_assets_uris': excluded_assets_uris,
                'template_uuid': template_uuid,
                'template_id': template_id,
                'template_project': template_project,
                'template_region': template_region,
                'refresh_mode': refresh_mode, # ON_DEMAND refresh mode
                'refresh_frequency': 0, # N/A
                'tag_history': tag_history,
                'version': 1,
                'overwrite': overwrite,
                'service_account': service_account
            })
        
        print('Created static asset config.')
        
        return config_uuid
    
    
    def write_dynamic_table_config(self, service_account, fields, included_tables_uris, excluded_tables_uris, template_uuid, \
                                   template_id, template_project, template_region, refresh_mode, refresh_frequency, \
                                   refresh_unit, tag_history):
        
        included_tables_uris_hash = hashlib.md5(included_tables_uris.encode()).hexdigest()
        
        # check to see if this config already exists
        configs_ref = self.db.collection('dynamic_table_configs')
        query = configs_ref.where(filter=FieldFilter('template_uuid', '==', template_uuid))
        query = query.where(filter=FieldFilter('included_tables_uris_hash', '==', included_tables_uris_hash))
        query = query.where(filter=FieldFilter('config_type', '==', 'DYNAMIC_TAG_TABLE'))
        query = query.where(filter=FieldFilter('config_status', '!=', 'INACTIVE'))
       
        matches = query.get()
       
        for match in matches:
            if match.exists:
                config_uuid_match = match.id
                #print('Config already exists. Config_uuid: ' + str(config_uuid_match))
                
                # update status to INACTIVE 
                self.db.collection('dynamic_table_configs').document(config_uuid_match).update({
                    'config_status' : "INACTIVE"
                })
                print('Updated status to INACTIVE.')
       
        config_uuid = uuid.uuid1().hex
        config = self.db.collection('dynamic_table_configs')
        doc_ref = config.document(config_uuid)
        
        if refresh_mode == 'AUTO':
            
            delta, next_run = self.validate_auto_refresh(refresh_frequency, refresh_unit)
            
            doc_ref.set({
                'config_uuid': config_uuid,
                'config_type': 'DYNAMIC_TAG_TABLE',
                'config_status': 'ACTIVE', 
                'creation_time': datetime.utcnow(), 
                'fields': fields,
                'included_tables_uris': included_tables_uris,
                'included_tables_uris_hash': included_tables_uris_hash,
                'excluded_tables_uris': excluded_tables_uris,
                'template_uuid': template_uuid,
                'template_id': template_id,
                'template_project': template_project,
                'template_region': template_region,
                'refresh_mode': refresh_mode, # AUTO refresh mode
                'refresh_frequency': delta,
                'refresh_unit': refresh_unit,
                'tag_history': tag_history,
                'scheduling_status': 'ACTIVE',
                'next_run': next_run,
                'version': 1,
                'service_account': service_account
            })
            
        else:
            doc_ref.set({
                'config_uuid': config_uuid,
                'config_type': 'DYNAMIC_TAG_TABLE',
                'config_status': 'ACTIVE', 
                'creation_time': datetime.utcnow(), 
                'fields': fields,
                'included_tables_uris': included_tables_uris,
                'included_tables_uris_hash': included_tables_uris_hash,
                'excluded_tables_uris': excluded_tables_uris,
                'template_uuid': template_uuid,
                'template_id': template_id,
                'template_project': template_project,
                'template_region': template_region,
                'refresh_mode': refresh_mode, # ON_DEMAND refresh mode
                'refresh_frequency': 0,
                'tag_history': tag_history,
                'version': 1,
                'service_account': service_account
            })
        
        print('Created dynamic table config.')
        
        return config_uuid

    
    def write_dynamic_column_config(self, service_account, fields, included_columns_query, included_tables_uris, excluded_tables_uris, \
                                    template_uuid, template_id, template_project, template_region, \
                                    refresh_mode, refresh_frequency, refresh_unit, tag_history):
        
        included_tables_uris_hash = hashlib.md5(included_tables_uris.encode()).hexdigest()
        
        # check to see if this config already exists
        configs_ref = self.db.collection('dynamic_column_configs')
        query = configs_ref.where(filter=FieldFilter('template_uuid', '==', template_uuid))
        query = query.where(filter=FieldFilter('included_tables_uris_hash', '==', included_tables_uris_hash))
        query = query.where(filter=FieldFilter('config_type', '==', 'DYNAMIC_TAG_COLUMN'))
        query = query.where(filter=FieldFilter('config_status', '!=', 'INACTIVE'))
       
        matches = query.get()
       
        for match in matches:
            if match.exists:
                config_uuid_match = match.id
                #print('Config already exists. Config_uuid: ' + str(config_uuid_match))
                
                # update status to INACTIVE 
                self.db.collection('dynamic_column_configs').document(config_uuid_match).update({
                    'config_status' : "INACTIVE"
                })
                print('Updated status to INACTIVE.')
       
        config_uuid = uuid.uuid1().hex
        config = self.db.collection('dynamic_column_configs')
        doc_ref = config.document(config_uuid)
        
        if refresh_mode == 'AUTO':
            
            delta, next_run = self.validate_auto_refresh(refresh_frequency, refresh_unit)
            
            doc_ref.set({
                'config_uuid': config_uuid,
                'config_type': 'DYNAMIC_TAG_COLUMN',
                'config_status': 'ACTIVE', 
                'creation_time': datetime.utcnow(), 
                'fields': fields,
                'included_columns_query': included_columns_query,
                'included_tables_uris': included_tables_uris,
                'included_tables_uris_hash': included_tables_uris_hash,
                'excluded_tables_uris': excluded_tables_uris,
                'template_uuid': template_uuid,
                'template_id': template_id,
                'template_project': template_project,
                'template_region': template_region,
                'refresh_mode': refresh_mode, # AUTO refresh mode
                'refresh_frequency': delta,
                'refresh_unit': refresh_unit,
                'tag_history': tag_history,
                'scheduling_status': 'ACTIVE',
                'next_run': next_run,
                'version': 1,
                'service_account': service_account
            })
            
        else:
            doc_ref.set({
                'config_uuid': config_uuid,
                'config_type': 'DYNAMIC_TAG_COLUMN',
                'config_status': 'ACTIVE', 
                'creation_time': datetime.utcnow(), 
                'fields': fields,
                'included_columns_query': included_columns_query,
                'included_tables_uris': included_tables_uris,
                'included_tables_uris_hash': included_tables_uris_hash,
                'excluded_tables_uris': excluded_tables_uris,
                'template_uuid': template_uuid,
                'template_id': template_id,
                'template_project': template_project,
                'template_region': template_region,
                'refresh_mode': refresh_mode, # ON_DEMAND refresh mode
                'refresh_frequency': 0,
                'tag_history': tag_history,
                'version': 1,
                'service_account': service_account
            })
        
        print('Created dynamic column config.')
        
        return config_uuid
    

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
    
    
    def write_entry_config(self, service_account, fields, included_assets_uris, excluded_assets_uris, template_uuid, \
                            template_id, template_project, template_region, refresh_mode, refresh_frequency, \
                            refresh_unit, tag_history):
        
        print('** enter write_entry_config **')
        
        included_assets_uris_hash = hashlib.md5(included_assets_uris.encode()).hexdigest()
        
        # check to see if this config already exists
        configs_ref = self.db.collection('entry_configs')
        query = configs_ref.where(filter=FieldFilter('template_uuid', '==', template_uuid))
        query = query.where(filter=FieldFilter('included_assets_uris_hash', '==', included_assets_uris_hash))
        query = query.where(filter=FieldFilter('config_type', '==', 'ENTRY_CREATE'))
        query = query.where(filter=FieldFilter('config_status', '!=', 'INACTIVE'))
       
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
                'config_type': 'ENTRY_CREATE',
                'config_status': 'ACTIVE', 
                'creation_time': datetime.utcnow(), 
                'fields': fields,
                'included_assets_uris': included_assets_uris,
                'included_assets_uris_hash': included_assets_uris_hash,
                'excluded_assets_uris': excluded_assets_uris,
                'template_uuid': template_uuid,
                'template_id': template_id,
                'template_project': template_project,
                'template_region': template_region,
                'refresh_mode': refresh_mode, # AUTO refresh mode
                'refresh_frequency': delta,
                'refresh_unit': refresh_unit,
                'tag_history': tag_history,
                'scheduling_status': 'ACTIVE',
                'next_run': next_run,
                'version': 1,
                'service_account': service_account
            })
            
        else:
            doc_ref.set({
                'config_uuid': config_uuid,
                'config_type': 'ENTRY_CREATE',
                'config_status': 'ACTIVE', 
                'creation_time': datetime.utcnow(), 
                'fields': fields,
                'included_assets_uris': included_assets_uris,
                'included_assets_uris_hash': included_assets_uris_hash,
                'excluded_assets_uris': excluded_assets_uris,
                'template_uuid': template_uuid,
                'template_id': template_id,
                'template_project': template_project,
                'template_region': template_region,
                'refresh_mode': refresh_mode, # ON_DEMAND refresh mode
                'refresh_frequency': 0,
                'tag_history': tag_history,
                'version': 1,
                'service_account': service_account
            })
        
        print('Created entry config.')
        
        return config_uuid

    
    def write_glossary_asset_config(self, service_account, fields, mapping_table, included_assets_uris, excluded_assets_uris, \
                                    template_uuid, refresh_mode, refresh_frequency, refresh_unit, tag_history, \
                                    overwrite=False):
        
        print('** enter write_glossary_asset_config **')
        
        included_assets_uris_hash = hashlib.md5(included_assets_uris.encode()).hexdigest()
        
        # check to see if this config already exists
        configs_ref = self.db.collection('glossary_asset_configs')
        query = configs_ref.where(filter=FieldFilter('template_uuid', '==', template_uuid))
        query = query.where(filter=FieldFilter('included_assets_uris_hash', '==', included_assets_uris_hash))
        query = query.where(filter=FieldFilter('config_type', '==', 'GLOSSARY_TAG_ASSET'))
        query = query.where(filter=FieldFilter('config_status', '!=', 'INACTIVE'))
       
        matches = query.get()
       
        for match in matches:
            if match.exists:
                config_uuid_match = match.id
                #print('config already exists. Found config_uuid: ' + str(config_uuid_match))
                
                # update status to INACTIVE 
                self.db.collection('glossary_asset_configs').document(config_uuid_match).update({
                    'config_status' : "INACTIVE"
                })
                print('Updated status to INACTIVE.')
       
        config_uuid = uuid.uuid1().hex
        config = self.db.collection('glossary_asset_configs')
        doc_ref = config.document(config_uuid)
        
        if refresh_mode == 'AUTO':
            
            delta, next_run = self.validate_auto_refresh(refresh_frequency, refresh_unit)
            
            doc_ref.set({
                'config_uuid': config_uuid,
                'config_type': 'GLOSSARY_TAG_ASSET',
                'config_status': 'ACTIVE', 
                'creation_time': datetime.utcnow(), 
                'fields': fields,
                'mapping_table': mapping_table,
                'included_assets_uris': included_assets_uris,
                'included_assets_uris_hash': included_assets_uris_hash,
                'excluded_assets_uris': excluded_assets_uris,
                'template_uuid': template_uuid,
                'template_id': template_id,
                'template_project': template_project,
                'template_region': template_region,
                'refresh_mode': refresh_mode, # AUTO refresh mode
                'refresh_frequency': delta,
                'refresh_unit': refresh_unit,
                'tag_history': tag_history,
                'scheduling_status': 'ACTIVE',
                'next_run': next_run,
                'version': 1,
                'overwrite': overwrite,
                'service_account': service_account
            })
            
        else:
            doc_ref.set({
                'config_uuid': config_uuid,
                'config_type': 'GLOSSARY_TAG_ASSET',
                'config_status': 'ACTIVE', 
                'creation_time': datetime.utcnow(), 
                'fields': fields,
                'mapping_table': mapping_table,
                'included_assets_uris': included_assets_uris,
                'included_assets_uris_hash': included_assets_uris_hash,
                'excluded_assets_uris': excluded_assets_uris,
                'template_uuid': template_uuid,
                'template_id': template_id,
                'template_project': template_project,
                'template_region': template_region,
                'refresh_mode': refresh_mode, # ON_DEMAND refresh mode
                'refresh_frequency': 0,
                'tag_history': tag_history,
                'version': 1,
                'overwrite': overwrite,
                'service_account': service_account
            })
        
        print('Created glossary asset config.')
        
        return config_uuid


    def write_sensitive_column_config(self, service_account, fields, dlp_dataset, infotype_selection_table, infotype_classification_table, \
                                        included_tables_uris, excluded_tables_uris, create_policy_tags, taxonomy_id, template_uuid, \
                                        template_id, template_project, template_region, refresh_mode, refresh_frequency, refresh_unit, \
                                        tag_history, overwrite=False):
        
        print('** enter write_sensitive_column_config **')
        
        included_tables_uris_hash = hashlib.md5(included_tables_uris.encode()).hexdigest()
        
        # check to see if this config already exists
        configs_ref = self.db.collection('sensitive_column_configs')
        query = configs_ref.where(filter=FieldFilter('template_uuid', '==', template_uuid))
        query = query.where(filter=FieldFilter('included_tables_uris_hash', '==', included_tables_uris_hash))
        query = query.where(filter=FieldFilter('config_type', '==', 'SENSITIVE_TAG_COLUMN'))
        query = query.where(filter=FieldFilter('config_status', '!=', 'INACTIVE'))
       
        matches = query.get()
       
        for match in matches:
            if match.exists:
                config_uuid_match = match.id
                #print('config already exists. Found config_uuid: ' + str(config_uuid_match))
                
                # update status to INACTIVE 
                self.db.collection('sensitive_column_configs').document(config_uuid_match).update({
                    'config_status' : "INACTIVE"
                })
                print('Updated status to INACTIVE.')
       
        config_uuid = uuid.uuid1().hex
        configs = self.db.collection('sensitive_column_configs')
        doc_ref = configs.document(config_uuid)
        
        if refresh_mode == 'AUTO':
            
            delta, next_run = self.validate_auto_refresh(refresh_frequency, refresh_unit)
            
            doc_ref.set({
                'config_uuid': config_uuid,
                'config_type': 'SENSITIVE_TAG_COLUMN',
                'config_status': 'ACTIVE', 
                'creation_time': datetime.utcnow(), 
                'fields': fields,
                'dlp_dataset': dlp_dataset,
                'infotype_selection_table': infotype_selection_table,
                'infotype_classification_table': infotype_classification_table,
                'included_tables_uris': included_tables_uris,
                'included_tables_uris_hash': included_tables_uris_hash,
                'excluded_tables_uris': excluded_tables_uris,
                'create_policy_tags': create_policy_tags, 
                'taxonomy_id': taxonomy_id,
                'template_uuid': template_uuid,
                'template_id': template_id,
                'template_project': template_project,
                'template_region': template_region,
                'refresh_mode': refresh_mode, # AUTO refresh mode
                'refresh_frequency': delta,
                'refresh_unit': refresh_unit,
                'tag_history': tag_history,
                'scheduling_status': 'ACTIVE',
                'next_run': next_run,
                'version': 1,
                'overwrite': overwrite,
                'service_account': service_account
            })
            
        else:
            doc_ref.set({
                'config_uuid': config_uuid,
                'config_type': 'SENSITIVE_TAG_COLUMN',
                'config_status': 'ACTIVE', 
                'creation_time': datetime.utcnow(), 
                'fields': fields,
                'dlp_dataset': dlp_dataset,
                'infotype_selection_table': infotype_selection_table,
                'infotype_classification_table': infotype_classification_table,
                'included_tables_uris': included_tables_uris,
                'included_tables_uris_hash': included_tables_uris_hash,
                'excluded_tables_uris': excluded_tables_uris,
                'create_policy_tags': create_policy_tags, 
                'taxonomy_id': taxonomy_id,
                'template_uuid': template_uuid,
                'template_id': template_id,
                'template_project': template_project,
                'template_region': template_region,
                'refresh_mode': refresh_mode, # ON_DEMAND refresh mode
                'refresh_frequency': 0,
                'tag_history': tag_history,
                'version': 1,
                'overwrite': overwrite,
                'service_account': service_account
            })
        
        print('Created sensitive column config.')
        
        return config_uuid

    
    def write_tag_restore_config(self, service_account, source_template_uuid, source_template_id, source_template_project, source_template_region, \
                                 target_template_uuid, target_template_id, target_template_project, target_template_region, \
                                 metadata_export_location, tag_history, overwrite=True):
                                    
        print('** write_tag_restore_config **')
        
        # check to see if this config already exists
        configs_ref = self.db.collection('restore_configs')
        query = configs_ref.where(filter=FieldFilter('source_template_uuid', '==', source_template_uuid))
        query = query.where(filter=FieldFilter('target_template_uuid', '==', target_template_uuid))
        query = query.where(filter=FieldFilter('config_status', '!=', 'INACTIVE'))
       
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
            'config_type': 'TAG_RESTORE',
            'config_status': 'ACTIVE', 
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
            'overwrite': overwrite,
            'service_account': service_account
        })
        
        return config_uuid
        

    def write_tag_import_config(self, service_account, template_uuid, template_id, template_project, template_region, \
                                metadata_import_location, tag_history, overwrite=True):
                                    
        print('** write_tag_import_csv_config **')
        
        # check if this config already exists
        coll_ref = self.db.collection('import_configs')
        query = coll_ref.where(filter=FieldFilter('template_uuid', '==', template_uuid))
        query = query.where(filter=FieldFilter('metadata_import_location', '==', metadata_import_location))
        query = query.where(filter=FieldFilter('config_status', '!=', 'INACTIVE'))
       
        matches = query.get()
       
        for match in matches:
            if match.exists:
                config_uuid_match = match.id
                print('config already exists. Found config_uuid: ' + str(config_uuid_match))
                
                # update status to INACTIVE 
                coll_ref.document(config_uuid_match).update({
                    'config_status' : "INACTIVE"
                })
                print('Updated status to INACTIVE.')
       
        config_uuid = uuid.uuid1().hex
        doc_ref = coll_ref.document(config_uuid)
        
        doc_ref.set({
            'config_uuid': config_uuid,
            'config_type': 'TAG_IMPORT',
            'config_status': 'ACTIVE', 
            'creation_time': datetime.utcnow(), 
            'template_uuid': template_uuid,
            'template_id': template_id, 
            'template_project': template_project,
            'template_region': template_region,
            'metadata_import_location': metadata_import_location,
            'tag_history': tag_history,
            'overwrite': overwrite,
            'service_account': service_account
        })
        
        return config_uuid

    
    def write_tag_export_config(self, service_account, source_projects, source_folder, source_region, \
                                target_project, target_dataset, target_region, write_option, \
                                refresh_mode, refresh_frequency, refresh_unit):
        
        print('** write_tag_export_config **')
        
        # check to see if this config already exists
        configs_ref = self.db.collection('export_configs')
        
        if source_projects != '':
            query = configs_ref.where(filter=FieldFilter('source_projects', '==', source_projects))
            query = query.where(filter=FieldFilter('source_region', '==', source_region))
            query = query.where(filter=FieldFilter('target_project', '==', target_project))
            query = query.where(filter=FieldFilter('target_dataset', '==', target_dataset))
            query = query.where(filter=FieldFilter('config_status', '!=', 'INACTIVE'))
        else:
            query = configs_ref.where(filter=FieldFilter('source_folder', '==', source_folder))
            query = query.where(filter=FieldFilter('source_region', '==', source_region))
            query = query.where(filter=FieldFilter('target_project', '==', target_project))
            query = query.where(filter=FieldFilter('target_dataset', '==', target_dataset))
            query = query.where(filter=FieldFilter('config_status', '!=', 'INACTIVE'))
       
        matches = query.get()
       
        for match in matches:
            if match.exists:
                config_uuid_match = match.id
                print('config already exists. Found config_uuid: ' + str(config_uuid_match))
                
                # update status to INACTIVE 
                self.db.collection('export_configs').document(config_uuid_match).update({
                    'config_status' : "INACTIVE"
                })
                print('Updated status to INACTIVE.')
       
        config_uuid = uuid.uuid1().hex
        configs = self.db.collection('export_configs')
        doc_ref = configs.document(config_uuid)
        
        if refresh_mode == 'AUTO':
            
            delta, next_run = self.validate_auto_refresh(refresh_frequency, refresh_unit)
        
            doc_ref.set({
                'config_uuid': config_uuid,
                'config_type': 'TAG_EXPORT',
                'config_status': 'ACTIVE', 
                'creation_time': datetime.utcnow(),
                'source_projects': source_projects, 
                'source_folder': source_folder,
                'source_region': source_region,
                'target_project': target_project,
                'target_dataset': target_dataset,
                'target_region': target_region,
                'write_option': write_option,
                'refresh_mode': refresh_mode,
                'refresh_frequency': delta,
                'refresh_unit': refresh_unit,
                'scheduling_status': 'READY',
                'next_run': next_run,
                'version': 1,
                'service_account': service_account
            })
        
        else:
                
            doc_ref.set({
                'config_uuid': config_uuid,
                'config_type': 'TAG_EXPORT',
                'config_status': 'ACTIVE', 
                'creation_time': datetime.utcnow(),
                'source_projects': source_projects, 
                'source_folder': source_folder,
                'source_region': source_region,
                'target_project': target_project,
                'target_dataset': target_dataset,
                'target_region': target_region,
                'write_option': write_option,
                'refresh_mode': refresh_mode, # ON_DEMAND
                'refresh_frequency': 0,
                'version': 1,
                'service_account': service_account
            })
        
        print('Created tag export config.')
        
        return config_uuid
                    
    def lookup_config_collection(self, requested_ct):
        
        coll = None
        
        for available_ct in (ct.ConfigType):
            
            if available_ct.name == requested_ct.strip():
                coll = available_ct.value
        
        return coll
    
    def get_config_collections(self):
        
        colls = []
        for coll in (ct.ConfigType):
            colls.append(coll.value)
        
        return colls
        
    def read_configs(self, service_account, config_type='ALL', template_id=None, template_project=None, template_region=None):
        
        print('* enter read_configs *')
        
        colls = []
        pending_running_configs = []
        active_configs = []
        combined_configs = []
        
        if template_id and template_project and template_region:
            template_exists, template_uuid = self.read_tag_template(template_id, template_project, template_region)
        else:
            template_exists = False
            template_uuid = None
        
        if config_type == 'ALL':
            colls = self.get_config_collections()
        else:
            colls.append(self.lookup_config_collection(config_type))
        
        #print('colls: ', colls)
        
        for coll_name in colls:
            
            # skip the export configs because they are tied to a project, not template
            if coll_name == 'export_configs':
                continue
            
            config_ref = self.db.collection(coll_name)
            
            if coll_name == 'restore_configs':
            
                if template_exists == True:
                    config_ref = config_ref.where(filter=FieldFilter('target_template_uuid', '==', template_uuid))
            else:
                
                if template_exists == True:
                    config_ref = config_ref.where(filter=FieldFilter('template_uuid', '==', template_uuid))
                
            docs = config_ref.where(filter=FieldFilter('config_status', '!=', 'INACTIVE'))
            docs = docs.where(filter=FieldFilter('service_account', '==', service_account)).stream()
                                    
            for doc in docs:
                config = doc.to_dict()
                
                if 'job_status' not in config or 'PENDING' in config['job_status'] or 'RUNNING' in config['job_status']:
                    pending_running_configs.append(config)
                else:
                    active_configs.append(config)
                        
        combined_configs = pending_running_configs + active_configs
        
        return combined_configs
        
    
    def read_config(self, service_account, config_uuid, config_type, reformat=False):
                
        config_result = {}
        coll_name = self.lookup_config_collection(config_type)
        
        config_ref = self.db.collection(coll_name).document(config_uuid)
        doc = config_ref.get()
        
        if doc.exists:
            config_result = doc.to_dict()
            
            if config_result['service_account'] != service_account:
                return {}  
            
            if reformat and config_type == 'TAG_EXPORT':
                config_result = self.format_source_projects(config_result)
            
        return config_result
        
    
    def read_jobs_by_config(self, config_uuid):
        
        job_results = []
        
        jobs_ref = self.db.collection('jobs')
        query = jobs_ref.where(filter=FieldFilter('config_uuid', '==', config_uuid))
        jobs_stream = query.order_by('completion_time', direction=firestore.Query.DESCENDING).stream()

        for job in jobs_stream:
            job_results.append(job.to_dict())
        
        return job_results
       
        
    def read_config_by_job(self, job_uuid):
        
        print('read_config_by_job, job_uuid:', job_uuid)
        
        config_uuid = None
        config_type = None
        
        job_ref = self.db.collection('jobs').document(job_uuid)
        doc = job_ref.get()
        
        if doc.exists:
            job = doc.to_dict()
            config_uuid = job['config_uuid']
            config_type = job['config_type']
   
        return config_uuid, config_type
        
    
    def read_service_account(self, config_type, config_uuid):
                
        service_account = None
        
        coll_name = self.lookup_config_collection(config_type)
        config_ref = self.db.collection(coll_name).document(config_uuid)
        
        doc = config_ref.get()
        
        if doc.exists:
            config = doc.to_dict()
            service_account = config['service_account']
            #print(str(config))
            
        return service_account
    
    
    def delete_config(self, service_account, config_uuid, config_type):
        
        coll_name = self.lookup_config_collection(config_type)
        
        config_ref = self.db.collection(coll_name).document(config_uuid)
        config = config_ref.get().to_dict()
        
        if config['service_account'] != service_account:
            return False
        
        try:
            self.db.collection(coll_name).document(config_uuid).delete()
        
        except Expection as e:
            print('Error occurred during delete_config: ', e)
            return False
        
        return True
    
    
    def purge_inactive_configs(self, service_account, config_type):
        
        config_uuids = []
        coll_names = []
        running_count = 0 
        
        if config_type == 'ALL':
            coll_names = self.get_config_collections()
        else:
            coll_names.append(self.lookup_config_collection(config_type))
        
        for coll_name in coll_names:
            config_ref = self.db.collection(coll_name)
            query = config_ref.where(filter=FieldFilter('config_status', '==', 'INACTIVE'))
            config_stream = query.where(filter=FieldFilter('service_account', '==', service_account)).stream()

            for inactive_config in config_stream:
                config_uuids.append(inactive_config.id)
        
            for config_uuid in config_uuids:
                self.db.collection(coll_name).document(config_uuid).delete()
                running_count += 1
            
            config_uuids.clear()
        
        return running_count
            
    
    def read_export_configs(self):
        
        configs = []
    
        query = self.db.collection('export_configs').where(filter=FieldFilter('config_status', '!=', 'INACTIVE'))
        docs = query.order_by('config_status', direction=firestore.Query.DESCENDING).stream()
                
        for doc in docs:
            config = doc.to_dict()
            config = self.format_source_projects(config)      
            configs.append(config)  

        return configs
      
      
    def format_source_projects(self, config):
        
        if config['source_projects'] != '':
            
            source_projects = config['source_projects']
            source_projects_str = ''
            for project in source_projects:
                source_projects_str += project + ','
            
            config['source_projects'] = source_projects_str[0:-1]
        
        return config
        
      
    def read_ready_configs(self):
        
        ready_configs = []

        for coll_name in self.get_config_collections():
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
        
        
    def lookup_config_by_uris(self, template_id, template_project, template_region, config_type, included_uris):
        
        success = False
        config = None
        config_uuid = ''
        
        template_exists, template_uuid = self.read_tag_template(template_id, template_project, template_region)
        
        if template_exists != True:
            print('Error: tag template', template_id, 'does not exist in', template_project, 'and', template_region)
            return success, config_uuid
        
        coll_name = self.lookup_config_collection(config_type)
           
        config_ref = self.db.collection(coll_name)
        
        if 'asset' in coll_name:
            config = config_ref.where(filter=FieldFilter('template_uuid', '==', template_uuid))
            config = config.where(filter=FieldFilter('config_status', '==', 'ACTIVE'))
            config = config.where(filter=FieldFilter('included_assets_uris', '==', included_uris)).get()
        else:
            config = config_ref.where(filter=FieldFilter('template_uuid', '==', template_uuid))
            config = config.where(filter=FieldFilter('config_status', '==', 'ACTIVE'))
            config = config.where(filter=FieldFilter('included_tables_uris', '==', included_uris)).get()
        
        if config == None:
            print('Error: config could not be found')
            return success, config_uuid
        else:
            if len(config) == 1:
                config_uuid = config[0].id
                success = True
                 
        return success, config_uuid


    def lookup_tag_template(self, config_type, config_uuid):
        
        template_id = None

        coll_name = self.lookup_config_collection(config_type)
        doc = self.db.collection(coll_name).document(config_uuid).get()
        
        if doc.exists:
            config = doc.to_dict()
            template_id = config['template_id']
        else:
            print('Error: could not locate the config')
                  
        return template_id


    def lookup_service_account(self, config_type, config_uuid):
        
        service_account = None

        coll_name = self.lookup_config_collection(config_type)
        doc = self.db.collection(coll_name).document(config_uuid).get()
        
        if doc.exists:
            config = doc.to_dict()
            service_account = config['service_account']
        else:
            print('Error: could not locate the config, returning an empty service account')
                  
        return service_account
        

    def update_config(self, old_config_uuid, config_type, config_status, fields, included_uris, excluded_uris, template_uuid, \
                      template_id, template_project, template_region, refresh_mode, refresh_frequency, \
                      refresh_unit, tag_history, overwrite=False, mapping_table=None):
        
        #print('enter update_config')
        #print('old_config_uuid: ', old_config_uuid)
        #print('config_type: ', config_type)
        
        coll_name = self.lookup_config_collection(config_type)
        print('coll_name: ', coll_name)
        
        self.db.collection(coll_name).document(old_config_uuid).update({
            'config_status' : "INACTIVE"
        })
        
        if config_type == 'STATIC_TAG_ASSET':
            new_config_uuid = self.write_static_asset_config(config_status, fields, included_uris, excluded_uris, template_uuid, \
                                                             template_id, template_project, template_region, \
                                                             refresh_mode, refresh_frequency, refresh_unit, \
                                                             tag_history, overwrite)
        
        if config_type == 'DYNAMIC_TAG_TABLE':
            new_config_uuid = self.write_dynamic_table_config(config_status, fields, included_uris, excluded_uris, \
                                                            template_uuid, template_id, template_project, template_region, \
                                                            refresh_mode, refresh_frequency, refresh_unit, tag_history)
                
        if config_type == 'ENTRY_CREATE':
            new_config_uuid = self.write_entry_config(config_status, fields, included_uris, excluded_uris, \
                                                      template_uuid, template_id, template_project, template_region, 
                                                      refresh_mode, refresh_frequency, refresh_unit, tag_history)
                                                                     
        if config_type == 'GLOSSARY_TAG_ASSET':
            new_config_uuid = self.write_glossary_asset_config(config_status, fields, mapping_table, included_uris, excluded_uris, \
                                                               template_uuid, template_id, template_project, template_region, \
                                                               refresh_mode, refresh_frequency, refresh_unit, tag_history, overwrite)
        # note: no need to return the included_uris_hash
            
        return new_config_uuid
    

    def update_dynamic_column_config(self, old_config_uuid, config_type, config_status, fields, included_columns_query, included_tables_uris,\
                                     excluded_tables_uris, template_uuid, template_id, template_project, template_region, \
                                     refresh_mode, refresh_frequency, refresh_unit, tag_history):
        
        self.db.collection('dynamic_column_configs').document(old_config_uuid).update({
            'config_status' : "INACTIVE"
        })
        
        
        new_config_uuid, included_tables_uris_hash = self.write_dynamic_column_config(config_status, fields, included_columns_query, \
                                                              included_tables_uris, excluded_tables_uris, \
                                                              template_uuid, template_id, template_project, template_region, \
                                                              refresh_mode, refresh_frequency, refresh_unit, tag_history)
        
        return new_config_uuid
        

    def update_sensitive_column_config(self, old_config_uuid, config_status, dlp_dataset, infotype_selection_table, \
                                       infotype_classification_table, included_tables_uris, excluded_tables_uris, \
                                       create_policy_tags, taxonomy_id, template_uuid, template_id, template_project, template_region, \
                                       refresh_mode, refresh_frequency, refresh_unit, tag_history, overwrite):
        
        self.db.collection('sensitive_column_configs').document(old_config_uuid).update({
            'config_status' : "INACTIVE"
        })
        
        config = self.read_config(old_config_uuid, 'SENSITIVE_TAG_COLUMN')
        
        new_config_uuid, included_tables_uris_hash = self.write_sensitive_column_config(config_status, config['fields'], dlp_dataset, \
                                                                          infotype_selection_table, infotype_classification_table, \
                                                                          included_tables_uris, excluded_tables_uris, \
                                                                          create_policy_tags, taxonomy_id, template_uuid, \
                                                                          template_id, template_project, template_region, \
                                                                          refresh_mode, refresh_frequency, refresh_unit, \
                                                                          tag_history, overwrite)
        
        
        return new_config_uuid
                     

    def update_tag_restore_config(self, old_config_uuid, config_status, source_template_uuid, source_template_id, source_template_project, 
                              source_template_region, target_template_uuid, target_template_id, target_template_project, \
                              target_template_region, metadata_export_location, tag_history, overwrite=False):
        
        self.db.collection('restore_configs').document(old_config_uuid).update({
            'config_status' : "INACTIVE"
        })
        
        new_config_uuid = self.write_tag_restore_config(config_status, source_template_uuid, source_template_id, source_template_project, \
                                                        source_template_region, target_template_uuid, target_template_id, \
                                                        target_template_project, target_template_region, \
                                                        metadata_export_location, tag_history, overwrite)
                    
        return new_config_uuid
    