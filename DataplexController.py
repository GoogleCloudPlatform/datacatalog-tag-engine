# Copyright 2024 Google, LLC.
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

import requests, configparser
from operator import itemgetter
import json
import os

from google.protobuf import struct_pb2
from google.protobuf import json_format

from google.api_core.gapic_v1.client_info import ClientInfo
from google.cloud import dataplex
from google.cloud.dataplex import CatalogServiceClient
from google.cloud import bigquery

import Resources as res
import BigQueryUtils as bq
import constants
from common import log_error, log_error_tag_dict, log_info, log_info_tag_dict

config = configparser.ConfigParser()
config.read("tagengine.ini")

BIGQUERY_REGION = config['DEFAULT']['BIGQUERY_REGION']
USER_AGENT = 'cloud-solutions/datacatalog-tag-engine-v3'

class DataplexController:
    
    def __init__(self, credentials, tag_creator_account=None, tag_invoker_account=None, \
                 aspect_type_id=None, aspect_type_project=None, aspect_type_region=None):
        self.credentials = credentials
        self.tag_creator_account = tag_creator_account
        self.tag_invoker_account = tag_invoker_account
        self.aspect_type_id = aspect_type_id
        self.aspect_type_project = aspect_type_project
        self.aspect_type_region = aspect_type_region
        self.client = CatalogServiceClient(credentials=self.credentials, client_info=ClientInfo(user_agent=USER_AGENT))
        
        if aspect_type_id != None and aspect_type_project != None and aspect_type_region != None:
            self.aspect_type_path = self.client.aspect_type_path(aspect_type_project, aspect_type_region, aspect_type_id)
        else:
            self.aspect_type_path = None
            
        self.bq_client = bigquery.Client(credentials=self.credentials, location=BIGQUERY_REGION, client_info=ClientInfo(user_agent=USER_AGENT))
        
        
    def get_aspect_type(self, included_fields=None):
        
        aspect_fields = []
        
        try:
            aspect_type = self.client.get_aspect_type(name=self.aspect_type_path)
            #print('aspect_type:', aspect_type)
        
        except Exception as e:
            msg = f'Error retrieving aspect type {self.aspect_type_path}'
            log_error(msg, e)
            return fields
        
        record_fields = aspect_type.metadata_template.record_fields

        for field in record_fields:

            if included_fields:
                match_found = False
                for included_field in included_fields:
                    if included_field['field_id'] == field.name:
                        match_found = True
                        
                        if 'field_value' in included_field:
                            assigned_value = included_field['field_value']
                        else:
                            assigned_value = None
                            
                        if 'query_expression' in included_field:
                            query_expression = included_field['query_expression']
                        else:
                            query_expression = None
                        
                        break
                
                if match_found == False:
                    continue
            
     
            enum_values = []

            if field.type_ == "enum":   
                for enum_value in field.enum_values:
                    enum_values.append(enum_value.name)
            
            # populate aspect_field dict
            aspect_field = {}
            aspect_field['field_id'] = field.name
            aspect_field['field_type'] = field.type_
            aspect_field['display_name'] = field.annotations.display_name
            aspect_field['is_required'] = field.constraints.required
            aspect_field['order'] = field.annotations.display_order
            
            if aspect_field['field_type'] == "enum":
                aspect_field['enum_values'] = enum_values
                
            if included_fields:
                if assigned_value:
                   aspect_field['field_value'] = assigned_value
                if query_expression:
                   aspect_field['query_expression'] = query_expression

            aspect_fields.append(aspect_field)
                            
        return sorted(aspect_fields, key=itemgetter('order'), reverse=True)
    
    
    def check_column_exists(self, aspects, target_column):
        
        print('enter check_column_exists')
        print('target_column:', target_column)
        #print('aspects:', aspects)
        
        column_exists = False
        
        for aspect_id, aspect_payload in aspects.items():
            if aspect_id.endswith('.global.schema'):
                break
        
        aspect_dict = json_format.MessageToDict(aspect_payload._pb)
        #print('aspect_dict:', aspect_dict)
        
        for field in aspect_dict['data']['fields']: 
            if field['name'] == target_column:
                column_exists = True
                print('found the target column', target_column)
                break
                  
        return column_exists
    
    
    def check_aspect_exists(self, aspect_type_path, aspects):
        
        print('enter check_aspect_exists')
        print('aspect_type_path:', aspect_type_path)
        
        aspect_type_path_short = '.'.join(aspect_type_path.split('.')[1:])
                
        aspect_exists = False
        aspect_id = ""
        
        for aspect_id, aspect_data in aspects.items():
            if aspect_id.endswith(aspect_type_path_short):
                aspect_exists = True
                break
           
        return aspect_exists, aspect_id
    
    
    def apply_import_config(self, job_uuid, config_uuid, data_asset_type, data_asset_region, tag_dict, tag_history, overwrite=False):
            
        op_status = constants.SUCCESS
        
        if 'project' in tag_dict:
            project = tag_dict['project']
        else:
            msg = "Error: project info missing from CSV"
            log_error_tag_dict(msg, None, job_uuid, tag_dict)
            op_status = constants.ERROR
            return op_status
        
        if data_asset_type == constants.BQ_ASSET:
            if 'dataset' not in tag_dict:
                msg = "Error: could not find the required dataset field in the CSV"
                log_error_tag_dict(msg, None, job_uuid, tag_dict)
                op_status = constants.ERROR
                return op_status
            else:
                entry_type = constants.DATASET
                dataset = tag_dict['dataset']
                
                if 'table' in tag_dict:
                    table = tag_dict['table']
                    entry_type = constants.BQ_TABLE
            
        if data_asset_type == constants.FILESET_ASSET:
            if 'entry_group' not in tag_dict or 'fileset' not in tag_dict:
                msg = "Error: could not find the required fields in the CSV. Missing entry_group or fileset or both"
                log_error_tag_dict(msg, None, job_uuid, tag_dict)
                op_status = constants.ERROR
                return op_status
            else:
                entry_type = constants.FILESET
                entry_group = tag_dict['entry_group']
                fileset = tag_dict['fileset']
        
        if data_asset_type == constants.SPAN_ASSET:
            if 'instance' not in tag_dict or 'database' not in tag_dict or 'table' not in tag_dict:
                msg = "Error: could not find the required fields in the CSV. The required fields for Spanner are instance, database, and table"
                log_error_tag_dict(msg, None, job_uuid, tag_dict)
                op_status = constants.ERROR
                return op_status
            else:
               entry_type = constants.SPAN_TABLE
               instance = tag_dict['instance']
               database = tag_dict['database']
               
               if 'schema' in tag_dict:
                   schema = tag_dict['schema']
                   table = tag_dict['table']
                   table = f"`{schema}.{table}`"
               else:
                   table = tag_dict['table']
                                
        if entry_type == constants.DATASET:
            entry_name = f'bigquery.googleapis.com/projects/{project}/datasets/{dataset}'
            entry_group = '@bigquery'
            
        if entry_type == constants.BQ_TABLE:
            entry_name = f'bigquery.googleapis.com/projects/{project}/datasets/{dataset}/tables/{table}'
            entry_group = '@bigquery'
            
        if entry_type == constants.FILESET:
            entry_name = f'datacatalog.googleapis.com/projects/{project}/locations/{data_asset_region}/entryGroups/{entry_group}/entries/{fileset}'
            entry_group = '@fileset'
            
        if entry_type == constants.SPAN_TABLE:
            entry_name = f'spanner:{project}.regional-{data_asset_region}.{instance}.{database}.{table}'
            entry_group = '@spanner'
        
        entry_path = f'projects/{project}/locations/{data_asset_region}/entryGroups/{entry_group}/entries/{entry_name}'
        
        entry_request = dataplex.GetEntryRequest(
            name=entry_path,
            view=dataplex.EntryView.ALL
        ) 
          
        try:
            entry = self.client.get_entry(request=entry_request)
            #print('entry:', entry)
        except Exception as e:
            msg = f"Error could not locate entry {entry_name}"
            log_error_tag_dict(msg, e, job_uuid, tag_dict)
            op_status = constants.ERROR
            return op_status

        # format uri for tag history table
        if data_asset_type == constants.BQ_ASSET:
            uri = entry.name.replace('bigquery.googleapis.com/projects/', '')
               
        target_column = None
        
        if 'column' in tag_dict:
            target_column = tag_dict['column'] 
            
            column_exists = self.check_column_exists(entry.aspects, target_column)
            print('column_exists:', column_exists)
            
            if column_exists == False:
                msg = f"Error could not find target column {target_column} in {entry.name}"
                log_error_tag_dict(msg, None, job_uuid, tag_dict)
                op_status = constants.ERROR
                return op_status
            
            uri = uri + '/column/' + target_column
            aspect_type_path = f'{self.aspect_type_project}.{self.aspect_type_region}.{self.aspect_type_id}@Schema.{target_column}'
        else:
            aspect_type_path = f'{self.aspect_type_project}.{self.aspect_type_region}.{self.aspect_type_id}'
        
        try:    
            aspect_exists, aspect_id = self.check_aspect_exists(aspect_type_path, entry.aspects)
            print('aspect_exists:', aspect_exists)
            
        except Exception as e:
            msg = f"Error during check_if_aspect_exists: {entry.name}"
            log_error_tag_dict(msg, e, job_uuid, tag_dict)
            op_status = constants.ERROR
            return op_status

        if aspect_exists and overwrite == False:
            msg = "Info: Aspect already exists and overwrite flag is False"
            log_info_tag_dict(msg, job_uuid, tag_dict)
            op_status = constants.SUCCESS
            return op_status
        
        aspect_fields = []
        aspect_type_fields = self.get_aspect_type()
        #print("aspect_type_fields:", aspect_type_fields)
        
        for field_name in tag_dict:
           
            if field_name == 'project' or field_name == 'dataset' or field_name == 'table' or \
                field_name == 'column' or field_name == 'entry_group' or field_name == 'fileset' or \
                field_name == 'instance' or field_name == 'database' or field_name == 'schema':
                continue
        
            found_field = False
            field_value = tag_dict[field_name]
            
            for aspect_type_field in aspect_type_fields:
                if field_name == aspect_type_field['field_id']:
                    field_type = aspect_type_field['field_type']
                    found_field = True
                    break
    
            if found_field != True:
                print('Error preparing the aspect. {field_name} was not found in {self.aspect_type_id}')
                op_status = constants.ERROR
                return op_status
    
            if field_type == 'bool':
                if field_value in ('True', 'TRUE', 'true'):
                    field_value = True
                else:
                    field_value = False
            
            elif field_type in ('datetime'):
                # timestamp needs to look like this: "2024-07-31T05:00:00.000Z"
                if len(field_value) == 10:
                    field_value = f"{field_value}T12:00:00.00Z"
                if len(field_value) == 19:
                    ts = field_value.replace(' ', 'T')
                    field_value = f"{ts}.00Z" 
                    
            elif field_type == 'double':
                    field_value = float(field_value)
            
            elif field_type == 'int':
                    field_value = int(field_value)
                    
            # this check allows for aspects with empty enums to get created, otherwise the empty enum gets flagged because DC thinks that you are storing an empty string as the enum value
            if field_type == 'enum' and field_value == '':
                continue

            aspect_fields.append({'field_id': field_name, 'field_type': field_type, 'field_value': field_value})
        

        op_status = self.create_update_delete_aspect(aspect_fields, aspect_type_path, entry_path, job_uuid, config_uuid, 'IMPORT_TAG', tag_history, uri, target_column)
                                
        return op_status
        
    
    def create_update_delete_aspect(self, aspect_fields, aspect_type_path, entry_path, job_uuid, config_uuid, config_type, tag_history, uri, target_column):
        
        print("enter create_update_delete_tag")
        print("aspect_fields:", aspect_fields)
        print("aspect_type_path:", aspect_type_path)
        print("entry_path:", entry_path)
        print("job_uuid:", job_uuid)
        print("config_uuid:", config_uuid)
        print("config_type:", config_type)
        #print("tag_history:", tag_history)
        #print("uri:", uri)
        
        op_status = constants.SUCCESS
        valid_field = False
        
        num_fields = len(aspect_fields)
        num_empty_values = 0
        
        aspect = dataplex.Aspect()
        aspect.path = "Schema.descriptor"
        aspect_data_dict = {}
        
        for field in aspect_fields:
            aspect_data_dict[field['field_id']] = field['field_value']
                
        aspect_data_struct = struct_pb2.Struct()
        json_format.ParseDict(aspect_data_dict, aspect_data_struct, ignore_unknown_fields=False)
        aspect.data = aspect_data_struct
        
        entry = dataplex.Entry()
        entry.name = entry_path
        entry.aspects = {aspect_type_path: aspect}
                
        #print("submitting entry for update:", entry)
        
        try:
            update_entry_request = dataplex.UpdateEntryRequest(
                entry=entry,
                update_mask="aspects",
            )

            resp = self.client.update_entry(request=update_entry_request)
            #print('update entry resp:', resp)
 
        except Exception as e:
            msg = f"Error while updating the entry"
            log_error(msg, error=str(e), job_uuid=job_uuid)
            op_status = constants.ERROR
            return op_status
        
        if tag_history and op_status == constants.SUCCESS:
            bqu = bq.BigQueryUtils(self.credentials, BIGQUERY_REGION)
            aspect_type_fields = self.get_aspect_type()
            success = bqu.copy_tag(self.tag_creator_account, self.tag_invoker_account, job_uuid, self.aspect_type_id, aspect_type_fields, uri, target_column, aspect_fields)
            
            if success == False:
                msg = 'Error occurred while writing to tag history table'
                log_error(msg, error='', job_uuid=job_uuid)
                op_status = constants.ERROR
       
        return op_status
    
                            
if __name__ == '__main__':
    
    import google.auth
    from google.auth import impersonated_credentials
    SCOPES = ['openid', 'https://www.googleapis.com/auth/cloud-platform', 'https://www.googleapis.com/auth/userinfo.email']
    
    source_credentials, _ = google.auth.default() 
    target_service_account = config['DEFAULT']['TAG_CREATOR_SA']
     
    credentials = impersonated_credentials.Credentials(source_credentials=source_credentials,
        target_principal=target_service_account,
        target_scopes=SCOPES,
        lifetime=1200)
        
    aspect_type_project = 'tag-engine-develop'
    aspect_type_region = 'us-central1'
    
    job_uuid = "1"
    config_uuid = "cd1983bc4d1811efbd33acde48001122"
    data_asset_type = "bigquery"
    data_asset_region = "us-central1"
    
    aspect_type_id = 'data-governance'
    tag_dict = {"project": "tag-engine-develop", "dataset": "sakila_dw", "table": "actor", "column": "actor_id", "data_domain": "LOGISTICS"} 
    tag_history = True
    overwrite = True
    
    dpc = DataplexController(credentials, target_service_account, 'scohen@gcp.solutions', aspect_type_id, aspect_type_project, aspect_type_region)
    #dpc.get_aspect_type()
    dpc.apply_import_config(job_uuid, config_uuid, data_asset_type, data_asset_region, tag_dict, tag_history, overwrite) 