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
        
    # note: included_fields can be populated or null
    # null = we want to return all the fields from the template
    def get_aspect_type(self, included_fields=None):
        
        print('enter get_aspect_type()')
        #print('included_fields:', included_fields)
              
        aspect_fields = []
        
        try:
            aspect_type = self.client.get_aspect_type(name=self.aspect_type_path)
            #print('aspect_type:', aspect_type)
        
        except Exception as e:
            msg = f'Error retrieving aspect type {self.aspect_type_path}'
            log_error(msg, e)
            return fields
        
        record_fields = aspect_type.metadata_template.record_fields
        #print('record_fields:', record_fields)

        for field in record_fields:
            
            match_found = False
            
            if included_fields != None:
                
                for included_field in included_fields:
                
                    if included_field['field_id'] == field.name:
                    
                        print('found_match:', field.name)
                    
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
            
            if included_fields != None and match_found == False:
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
        
        sorted_aspect_fields = sorted(aspect_fields, key=itemgetter('order'))
        #print('sorted_aspect_fields:', sorted_aspect_fields)
                            
        return sorted_aspect_fields
    
    
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
        
    
    def apply_dynamic_table_config(self, fields, uri, job_uuid, config_uuid, aspect_type_uuid, tag_history):
        
        print('*** apply_dynamic_table_config ***')
        #print('fields:', fields)
        #print('uri:', uri)
        #print('job_uuid:', job_uuid)
        #print('config_uuid:', config_uuid)
        #print('aspect_type_uuid:', aspect_type_uuid)
        #print('tag_history:', tag_history)
        
        op_status = constants.SUCCESS
        error_exists = False
        
        bigquery_project = uri.split('/')[0]

        # TO DO: allow user to overwrite default region with config value
        bigquery_region = BIGQUERY_REGION # default to the region from the ini file 
        
        entry_name = f'bigquery.googleapis.com/projects/{uri}'
        entry_path = f'projects/{bigquery_project}/locations/{bigquery_region}/entryGroups/@bigquery/entries/{entry_name}'
        
        entry_request = dataplex.GetEntryRequest(
            name=entry_path,
            view=dataplex.EntryView.ALL
        )

        try:
            entry = self.client.get_entry(request=entry_request)
            #print('entry:', entry)
        except Exception as e:
            msg = f"Error could not locate entry {entry_name}"
            log_error(msg, e, job_uuid)
            op_status = constants.ERROR
            return op_status

        # run query expressions
        verified_field_count = 0
        
        for field in fields:
            field_id = field['field_id']
            field_type = field['field_type']
            query_expression = field['query_expression']

            # parse the query expression
            query_str = self.parse_query_expression(uri, query_expression)
            print('returned query_str: ' + query_str)
            
            # run the SQL query
            # note: field_values is of type list
            field_values, error_exists = self.run_query(query_str, field_type, job_uuid)
    
            if error_exists or field_values == []:
                continue
                      
            verified_field_count = verified_field_count + 1
            
            if field_type == 'richtext':
                formatted_value = ', '.join(str(v) for v in field_values)
            else:
                formatted_value = field_values[0]
               
            field['field_value'] = formatted_value
       
        if verified_field_count == 0:
            # aspect is empty due to SQL errors, skip aspect creation
            op_status = constants.ERROR
            return op_status
                           
        aspect_type_path = f'{self.aspect_type_project}.{self.aspect_type_region}.{self.aspect_type_id}'
        
        op_status = self.create_update_delete_aspect(fields, aspect_type_path, entry_path, job_uuid, \
                                                     config_uuid, 'DYNAMIC_TAG_TABLE', tag_history, uri, None)
                         
        return op_status
    
    
    def parse_query_expression(self, uri, query_expression, column=None):
        
        query_str = None
        
        # analyze query expression
        from_index = query_expression.rfind(" from ", 0)
        where_index = query_expression.rfind(" where ", 0)
        project_index = query_expression.rfind("$project", 0)
        dataset_index = query_expression.rfind("$dataset", 0)
        table_index = query_expression.rfind("$table", 0)
        from_clause_table_index = query_expression.rfind(" from $table", 0)
        from_clause_backticks_table_index = query_expression.rfind(" from `$table`", 0)
        column_index = query_expression.rfind("$column", 0)
        
        if project_index != -1:
            project_end = uri.find('/') 
            project = uri[0:project_end]
            
        if dataset_index != -1:
            dataset_start = uri.find('/datasets/') + 10
            dataset_string = uri[dataset_start:]
            dataset_end = dataset_string.find('/') 
            
            if dataset_end == -1:
                dataset = dataset_string[0:]
            else:
                dataset = dataset_string[0:dataset_end]
        
        # $table referenced in from clause, use fully qualified table
        if from_clause_table_index > 0 or from_clause_backticks_table_index > 0:
             qualified_table = uri.replace('/project/', '.').replace('/datasets/', '.').replace('/tables/', '.')
             query_str = query_expression.replace('$table', qualified_table)
             
        # $table is referenced somewhere in the expression, replace $table with actual table name
        else:
        
            if table_index != -1:
                table_index = uri.rfind('/') + 1
                table_name = uri[table_index:]
                query_str = query_expression.replace('$table', table_name)
            
            # $project referenced in where clause too
            if project_index > -1:
                
                if query_str == None:
                    query_str = query_expression.replace('$project', project)
                else:
                    query_str = query_str.replace('$project', project)
                
                #print('query_str: ', query_str)
            
            # $dataset referenced in where clause too    
            if dataset_index > -1:

                if query_str == None:
                    query_str = query_expression.replace('$dataset', dataset)
                else:
                    query_str = query_str.replace('$dataset', dataset)
                    
                #print('query_str: ', query_str)
            
        # table not in query expression (e.g. select 'string')
        if table_index == -1 and query_str == None:
            query_str = query_expression
            
        if column_index != -1:
            
            if query_str == None:
                query_str = query_expression.replace('$column', column)
            else:
                query_str = query_str.replace('$column', column)
        
        #print('returning query_str:', query_str)            
        return query_str
    
    
    def run_query(self, query_str, field_type, job_uuid):
        
        field_values = []
        error_exists = False
            
        try:
            #print('query_str:', query_str)
            rows = self.bq_client.query_and_wait(query_str)
            
            # if query expression is well-formed, there should only be a single row returned with a single field_value
            # However, user may mistakenly run a query that returns a list of rows. In that case, grab only the top row.  
            row_count = 0

            for row in rows:
                row_count = row_count + 1
                field_values.append(row[0])
            
                if field_type != 'richtext' and row_count == 1:
                    return field_values, error_exists
        
            # check row_count
            if row_count == 0:
                #error_exists = True
                print('sql query returned nothing:', query_str)
        
        except Exception as e:
            error_exists = True
            msg = 'Error occurred during run_query {}'.format(query_str)
            log_error(msg, e, job_uuid)
            
        #print('field_values: ', field_values)
        
        return field_values, error_exists
    
    
    def apply_dynamic_column_config(self, fields, columns_query, uri, job_uuid, config_uuid, aspect_type_uuid, tag_history):
        
        print('*** apply_dynamic_column_config ***')
        #print('fields:', fields)
        #print('columns_query:', columns_query)
        #print('uri:', uri)
        #print('job_uuid:', job_uuid)
        #print('config_uuid:', config_uuid)
        #print('aspect_type_uuid:', aspect_type_uuid)
        #print('tag_history:', tag_history)
        
        op_status = constants.SUCCESS
        error_exists = False
        
        bigquery_project = uri.split('/')[0]

        # TO DO: allow user to overwrite default region with config value
        bigquery_region = BIGQUERY_REGION # default to the region from the ini file 
        
        entry_name = f'bigquery.googleapis.com/projects/{uri}'
        entry_path = f'projects/{bigquery_project}/locations/{bigquery_region}/entryGroups/@bigquery/entries/{entry_name}'
        
        entry_request = dataplex.GetEntryRequest(
            name=entry_path,
            view=dataplex.EntryView.ALL
        )

        try:
            entry = self.client.get_entry(request=entry_request)
            #print('entry:', entry)
        except Exception as e:
            msg = f"Error could not locate entry {entry_name}"
            log_error(msg, e, job_uuid)
            op_status = constants.ERROR
            return op_status
                         
        target_columns = [] # columns in the table which need to be tagged
        
        columns_query = self.parse_query_expression(uri, columns_query)
        #print('columns_query:', columns_query)
        
        rows = self.bq_client.query(columns_query).result()

        num_columns = 0
        for row in rows:    
            for column in row:
                print('column:', column)
                target_columns.append(column)
                num_columns += 1
                 
        if num_columns == 0:
            # no columns to tag
            msg = f"Error could not find columns to tag. Please check column_query parameter in your config. Current value: {columns_query}"
            log_error(msg, None, job_uuid)
            op_status = constants.ERROR
            return op_status
                                      
        column_fields_list = [] # list<dictionaries> where dict = {column, fields}

        for target_column in target_columns:
            
            #print('target_column:', target_column)
            
            # fail quickly if a column is not found in the entry's schema
            column_exists = self.check_column_exists(entry.aspects, target_column)
            
            if column_exists != True:
                msg = f"Error could not find column {target_column} in {entry.name}"
                log_error(msg, None, job_uuid)
                op_status = constants.ERROR
                return op_status

            verified_field_count = 0
            query_strings = []
            
            for field in fields:
                query_expression = field['query_expression']
                query_str = self.parse_query_expression(uri, query_expression, target_column)
                query_strings.append(query_str)
            
            print('query_strings:', query_strings)
                
            # combine query expressions 
            combined_query = self.combine_queries(query_strings)
            
            # run combined query, adding the results to the field_values for each field
            # Note: field_values is of type list
            fields, error_exists = self.run_combined_query(combined_query, target_column, fields, job_uuid)
            
            print('fields:', fields)

            if error_exists:
                op_status = constants.ERROR
                continue
                                                                     
            aspect_type_path = f'{self.aspect_type_project}.{self.aspect_type_region}.{self.aspect_type_id}@Schema.{target_column}'
            uri_column = f'{uri}/column/{target_column}' 
            
            op_status = self.create_update_delete_aspect(fields, aspect_type_path, entry_path, job_uuid, \
                                                         config_uuid, 'DYNAMIC_TAG_COLUMN', tag_history, uri_column, target_column)
                        
            # fail fast if aspect does not get created, updated or deleted 
            if op_status == constants.ERROR:
                return op_status
                                              
        return op_status
        
    
    def combine_queries(self, query_strings):
        
        large_query = "select "
        
        for query in query_strings:
             large_query += "({}), ".format(query)
        
        return large_query[0:-2]
        
     
    def run_combined_query(self, combined_query, column, fields, job_uuid):
        
        error_exists = False
            
        try:
            rows = self.bq_client.query_and_wait(combined_query)
            row_count = 0

            for row in rows:
                for i, field in enumerate(fields):
                    field['field_value'] = row[i]
            
                row_count += 1    
        
            if row_count == 0:
                error_exists = True
                print('sql query returned empty set:', combined_query)
        
        except Exception as e:
            error_exists = True
            msg = 'Error occurred during run_combined_query {}'.format(combined_query)
            log_error(msg, e, job_uuid)
            
        return fields, error_exists
        
           
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
        
    aspect_type_id = 'data-governance'
    aspect_type_project = 'tag-engine-develop'
    aspect_type_region = 'us-central1'
    aspect_type_uuid = 'Bofcfg9kkkFz4d0Dk2SM'
    
    #fields = [{'field_type': 'enum', 'field_id': 'data_domain', 'enum_values': ['LOGISTICS', 'FINANCE', 'HR', 'LEGAL', 'MARKETING', 'SALES'], 'is_required': True, 'display_name': 'Data Domain', 'order': 1, 'query_expression': "select 'LOGISTICS'"}]
    columns_query = "select 'c_id'"
    uri = 'tag-engine-develop/datasets/crm/tables/UpdAcct'
    job_uuid = 'b0a8e1de89cf11ef833d42004e494300'
    config_uuid = '9aaaed5089cf11ef825b42004e494300'
    tag_history = True
        
    included_fields = [{'field_id': 'data_domain', 'query_expression': "select 'LOGISTICS'"}, {'field_id': 'broad_data_category', 'query_expression': "select 'CONTENT'"}]
    dpc = DataplexController(credentials, target_service_account, 'scohen@gcp.solutions', aspect_type_id, aspect_type_project, aspect_type_region)
    #dpc.get_aspect_type(included_fields)
    dpc.get_aspect_type()
    
    #dpc.apply_dynamic_column_config(fields, columns_query, uri, job_uuid, config_uuid, aspect_type_uuid, tag_history)
   