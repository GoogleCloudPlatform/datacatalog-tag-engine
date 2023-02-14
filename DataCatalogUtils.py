# Copyright 2020-2021 Google, LLC.
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

import requests, configparser, time
from datetime import datetime, date
from datetime import time as dtime
import pytz
from operator import itemgetter
import pandas as pd
from pyarrow import parquet
import json
import os

from google.protobuf.timestamp_pb2 import Timestamp
from google.cloud import datacatalog
from google.cloud.datacatalog_v1 import types
from google.cloud.datacatalog import DataCatalogClient
from google.cloud import bigquery
from google.cloud import storage

import Resources as res
import TagEngineUtils as te
import BigQueryUtils as bq
import PubSubUtils as ps
import constants

config = configparser.ConfigParser()
config.read("tagengine.ini")
BIGQUERY_REGION = config['DEFAULT']['BIGQUERY_REGION']

class DataCatalogUtils:
    
    def __init__(self, template_id=None, template_project=None, template_region=None):
        self.template_id = template_id
        self.template_project = template_project
        self.template_region = template_region
        
        self.client = DataCatalogClient()
        
        if template_id is not None and template_project is not None and template_region is not None:
            self.template_path = DataCatalogClient.tag_template_path(template_project, template_region, template_id)
    
    def get_template(self, included_fields=None):
        
        fields = []
        
        tag_template = self.client.get_tag_template(name=self.template_path)
        
        for field_id, field_value in tag_template.fields.items():
            
            field_id = str(field_id)
            
            if included_fields:
                match_found = False
                for included_field in included_fields:
                    if included_field['field_id'] == field_id:
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
            
            display_name = field_value.display_name
            is_required = field_value.is_required
            order = field_value.order
     
            #print("field_id: " + str(field_id))
            #print("field_value: " + str(field_value))
            #print("display_name: " + display_name)
            #print("primitive_type: " + str(FieldType.primitive_type))
            #print("is_required: " + str(is_required))
            
            enum_values = []
            
            field_type = None
            
            if field_value.type_.primitive_type == datacatalog.FieldType.PrimitiveType.DOUBLE:
                field_type = "double"
            if field_value.type_.primitive_type == datacatalog.FieldType.PrimitiveType.STRING:
                field_type = "string"
            if field_value.type_.primitive_type == datacatalog.FieldType.PrimitiveType.BOOL:
                field_type = "bool"
            if field_value.type_.primitive_type == datacatalog.FieldType.PrimitiveType.TIMESTAMP:
                field_type = "datetime"
            if field_value.type_.primitive_type == datacatalog.FieldType.PrimitiveType.RICHTEXT:
                field_type = "richtext"
            if field_value.type_.primitive_type == datacatalog.FieldType.PrimitiveType.PRIMITIVE_TYPE_UNSPECIFIED:
                field_type = "enum"
                       
                index = 0
                enum_values_long = str(field_value.type_).split(":") 
                for long_value in enum_values_long:
                    if index > 0:
                        enum_value = long_value.split('"')[1]
                        #print("enum value: " + enum_value)
                        enum_values.append(enum_value)
                    index = index + 1
            
            # populate dict
            field = {}
            field['field_id'] = field_id
            field['display_name'] = display_name
            field['field_type'] = field_type
            field['is_required'] = is_required
            field['order'] = order
            
            if field_type == "enum":
                field['enum_values'] = enum_values
                
            if included_fields:
                if assigned_value:
                   field['field_value'] = assigned_value
                if query_expression:
                   field['query_expression'] = query_expression

            fields.append(field)
                          
        return sorted(fields, key=itemgetter('order'), reverse=True)
    
        
    def check_if_exists(self, parent, column=None):
        
        #print('enter check_if_exists')
        #print('input parent: ' + parent)
        #print('input column: ' + column)
        
        tag_exists = False
        tag_id = ""
        
        if column != None and column != "" and len(column) > 1:
            column = column.lower() # column is stored in lower case in the tag object
        
        tag_list = self.client.list_tags(parent=parent, timeout=120)
        
        for tag_instance in tag_list:
            
            #print('tag_instance: ' + str(tag_instance))
            #print('tag name: ' + str(tag_instance.name))
            tagged_column = tag_instance.column
            
            #print('found tagged column: ' + tagged_column)            
            tagged_template_project = tag_instance.template.split('/')[1]
            tagged_template_location = tag_instance.template.split('/')[3]
            tagged_template_id = tag_instance.template.split('/')[5]
            
            if column == '' or column == None:
                # looking for a table-level tag
                if tagged_template_id == self.template_id and tagged_template_project == self.template_project and \
                    tagged_template_location == self.template_region and tagged_column == "":
                    #print('DEBUG: table-level tag exists.')
                    tag_exists = True
                    tag_id = tag_instance.name
                    #print('DEBUG: tag_id: ' + tag_id)
                    break
            else:
                if column == tagged_column and tagged_template_id == self.template_id and tagged_template_project == self.template_project and \
                    tagged_template_location == self.template_region:
                    #print('DEBUG: column-level tag exists.')
                    tag_exists = True
                    tag_id = tag_instance.name
                    #print('DEBUG: tag_id: ' + tag_id)
                    break
        
        #print('DEBUG: tag_exists: ' + str(tag_exists))
        #print('DEBUG: tag_id: ' + str(tag_id))
           
        return tag_exists, tag_id
    
    
    def apply_static_asset_config(self, fields, uri, config_uuid, template_uuid, tag_history, tag_stream, overwrite=False):
        
        print('*** apply_static_asset_config ***')
        print('fields: ', fields)
        print('uri: ', uri)
        print('config_uuid: ', config_uuid)
        print('template_uuid: ', template_uuid)
        print('tag_history: ', tag_history)
        
        # uri is either a BQ table/view path or GCS file path
        store = te.TagEngineUtils()        
        creation_status = constants.SUCCESS
        column = ''
        
        is_gcs = False
        is_bq = False
        
        # look up the entry based on the resource type
        if isinstance(uri, list):
            is_gcs = True
            bucket = uri[0].replace('-', '_')
            filename = uri[1].split('.')[0].replace('/', '_') # extract the filename without extension, replace '/' with '_'
            gcs_resource = '//datacatalog.googleapis.com/projects/' + self.template_project + '/locations/' + self.template_region + '/entryGroups/' + bucket + '/entries/' + filename
            print('gcs_resource: ', gcs_resource)
            request = datacatalog.LookupEntryRequest()
            request.linked_resource=gcs_resource
            uri = '/'.join(uri)
            #print('uri:', uri)
            
            try:
                entry = self.client.lookup_entry(request)
                print('GCS entry:', entry.name)
            except Exception as e:
                print('Unable to find the entry in the catalog. Entry ' + gcs_resource + ' does not exist.')
                creation_status = constants.ERROR
                return creation_status
                
        elif isinstance(uri, str):
            is_bq = True
            bigquery_resource = '//bigquery.googleapis.com/projects/' + uri
            print("bigquery_resource: " + bigquery_resource)
        
            request = datacatalog.LookupEntryRequest()
            request.linked_resource=bigquery_resource
            entry = self.client.lookup_entry(request)
            print('entry: ', entry.name)
        
        try:    
            tag_exists, tag_id = self.check_if_exists(parent=entry.name)
            print('tag exists: ', tag_exists)
        
        except Exception as e:
            print('Error during check_if_exists: ', e)
            creation_status = constants.ERROR
            return creation_status

        if tag_exists and overwrite == False:
            #print('Tag already exists and overwrite set to False')
            creation_status = constants.SUCCESS
            return creation_status
        
        creation_status = self.create_update_tag(fields, tag_exists, tag_id, config_uuid, 'STATIC_ASSET_TAG', tag_history, tag_stream, entry, uri)    
           
        return creation_status


    def apply_dynamic_table_config(self, fields, uri, config_uuid, template_uuid, tag_history, tag_stream, batch_mode=False):
        
        print('*** apply_dynamic_table_config ***')
        print('fields:', fields) 
        print('uri:', uri)
        print('config_uuid:', config_uuid)
        print('template_uuid:', template_uuid)
        print('tag_history:', tag_history)
        print('tag_stream:', tag_stream)
        
        store = te.TagEngineUtils()
        bq_client = bigquery.Client(location=BIGQUERY_REGION)
        
        creation_status = constants.SUCCESS
        error_exists = False
        
        bigquery_resource = '//bigquery.googleapis.com/projects/' + uri
        
        #print('bigquery_resource: ', bigquery_resource)
        
        request = datacatalog.LookupEntryRequest()
        request.linked_resource=bigquery_resource
        entry = self.client.lookup_entry(request)

        tag_exists, tag_id = self.check_if_exists(parent=entry.name)
        #print("tag_exists: " + str(tag_exists))
        
        # create new tag
        tag = datacatalog.Tag()
        tag.template = self.template_path
        verified_field_count = 0
        
        for field in fields:
            field_id = field['field_id']
            field_type = field['field_type']
            query_expression = field['query_expression']

            # parse and run query in BQ
            query_str = self.parse_query_expression(uri, query_expression)
            #print('returned query_str: ' + query_str)
            
            # note: field_values is of type list
            field_values, error_exists = self.run_query(bq_client, query_str, field_type, batch_mode, store)
            #print('field_values: ', field_values)
            #print('error_exists: ', error_exists)
    
            if error_exists or field_values == []:
                continue
            
            tag, error_exists = self.populate_tag_field(tag, field_id, field_type, field_values, store)
    
            if error_exists:
                continue
                                    
            verified_field_count = verified_field_count + 1
            #print('verified_field_count: ' + str(verified_field_count))    
            
            # store the value back in the dict, so that it can be accessed by the exporter
            #print('field_value: ' + str(field_value))
            if field_type == 'richtext':
                formatted_value = ', '.join(str(v) for v in field_values)
            else:
                formatted_value = field_values[0]
                
            field['field_value'] = formatted_value
            
        # for loop ends here
                
        if error_exists:
            # error was encountered while running SQL expression
            # proceed with tag creation / update, but return error to user
            creation_status = constants.ERROR
            
        if verified_field_count == 0:
            # tag is empty due to errors, skip tag creation
            return constants.ERROR
                        
        if tag_exists == True:
            #print('updating tag')
            #print('tag request: ' + str(tag))
            tag.name = tag_id
            
            try:
                print('tag update request: ', tag)
                response = self.client.update_tag(tag=tag)
                #print('response: ', response)
            except Exception as e:
                print('Error occurred during tag update: ' + str(e))
                msg = 'Error occurred during tag update: ' + str(e)
                store.write_tag_op_error(constants.TAG_UPDATED, config_uuid, 'DYNAMIC_TABLE_TAG', msg)
                creation_status = constants.ERROR
        else:

            try:
                print('tag create request: ', tag)
                response = self.client.create_tag(parent=entry.name, tag=tag)
                #print('response: ', response)
                
            except Exception as e:
                print('Error occurred during tag create: ', e)
                msg = 'Error occurred during tag create: ' + str(e)
                store.write_tag_op_error(constants.TAG_CREATED, config_uuid, 'DYNAMIC_TABLE_TAG', msg)
                creation_status = constants.ERROR
            
        if creation_status == constants.SUCCESS and tag_history:
            bqu = bq.BigQueryUtils()
            template_fields = self.get_template()
            bqu.copy_tag(self.template_id, template_fields, uri, None, fields)
            
        if creation_status == constants.SUCCESS and tag_stream:
            psu = ps.PubSubUtils()
            psu.copy_tag(self.template_id, uri, None, fields)
                
                                 
        return creation_status


    def apply_dynamic_column_config(self, fields, columns_query, uri, config_uuid, template_uuid, tag_history, tag_stream, batch_mode=False):
        
        print('*** apply_dynamic_column_config ***')
        #print('fields:', fields) 
        #print('columns_query', columns_query)
        #print('uri:', uri)
        #print('config_uuid:', config_uuid)
        #print('template_uuid:', template_uuid)
        #print('tag_history:', tag_history)
        #print('tag_stream:', tag_stream)
                
        store = te.TagEngineUtils()
        bq_client = bigquery.Client(location=BIGQUERY_REGION)
        
        creation_status = constants.SUCCESS
        error_exists = False
        
        columns = [] # columns to be tagged
        columns_query = self.parse_query_expression(uri, columns_query)
        #print('columns_query:', columns_query)
        rows = bq_client.query(columns_query).result()
        
        num_rows = 0
        for row in rows:
            num_rows += 1
            columns.append(row[0]) # DC stores all schema columns in lower case
        
        if num_rows == 0:
            # no columns to tag
            return constants.SUCCESS
                
        #print('columns to be tagged:', columns)
                    
        bigquery_resource = '//bigquery.googleapis.com/projects/' + uri
        #print('bigquery_resource: ', bigquery_resource)
        
        request = datacatalog.LookupEntryRequest()
        request.linked_resource=bigquery_resource
        entry = self.client.lookup_entry(request)
        
        schema_columns = [] # columns in the entry's schema
                
        for column_schema in entry.schema.columns:
            schema_columns.append(column_schema.column)
            
        #print('schema_columns:', schema_columns)

        for column in columns:
            
            # skip columns not found in the entry's schema
            if column not in schema_columns:
                continue
            
            # check to see if a tag has already been created on this column
            tag_exists, tag_id = self.check_if_exists(entry.name, column)
            print("tag_exists: ", tag_exists)
        
            # initialize the new column-level tag
            tag = datacatalog.Tag()
            tag.template = self.template_path
            tag.column = column
            
            verified_field_count = 0
            
            for field in fields:
                field_id = field['field_id']
                field_type = field['field_type']
                query_expression = field['query_expression']

                query_str = self.parse_query_expression(uri, query_expression, column)
                #print('returned query_str: ' + query_str)
            
                # note: field_values is of type list
                field_values, error_exists = self.run_query(bq_client, query_str, field_type, batch_mode, store)
                #print('field_values: ', field_values)
                #print('error_exists: ', error_exists)
    
                if error_exists or field_values == []:
                    continue
            
                tag, error_exists = self.populate_tag_field(tag, field_id, field_type, field_values, store)
    
                if error_exists:
                    continue
                                    
                verified_field_count = verified_field_count + 1
                #print('verified_field_count: ' + str(verified_field_count))    
            
                # store the value back in the dict, so that it can be accessed by the exporter
                #print('field_value: ' + str(field_value))
                if field_type == 'richtext':
                    formatted_value = ', '.join(str(v) for v in field_values)
                else:
                    formatted_value = field_values[0]
                
                field['field_value'] = formatted_value
            
            # inner loop ends here

            if error_exists:
                # error was encountered while running SQL expression
                # proceed with tag creation / update, but return error to user
                creation_status = constants.ERROR
            
            if verified_field_count == 0:
                # tag is empty due to errors, skip tag creation
                return constants.ERROR
            
            # we are ready to create or update the tag 
                       
            if tag_exists == True:
                #print('updating tag')
                #print('tag request: ' + str(tag))
                tag.name = tag_id
            
                try:
                    print('tag update request: ', tag)
                    response = self.client.update_tag(tag=tag)
                    #print('response: ', response)
                except Exception as e:
                    print('Error occurred during tag update: ' + str(e))
                    msg = 'Error occurred during tag update: ' + str(e)
                    store.write_tag_op_error(constants.TAG_UPDATED, config_uuid, 'DYNAMIC_COLUMN_TAG', msg)
                    creation_status = constants.ERROR
            else:

                try:
                    print('tag create request: ', tag)
                    response = self.client.create_tag(parent=entry.name, tag=tag)
                    #print('response: ', response)
                
                except Exception as e:
                    print('Error occurred during tag create: ', e)
                    msg = 'Error occurred during tag create: ' + str(e)
                    store.write_tag_op_error(constants.TAG_CREATED, config_uuid, 'DYNAMIC_COLUMN_TAG', msg)
                    creation_status = constants.ERROR
            
            if creation_status == constants.SUCCESS and tag_history:
                bqu = bq.BigQueryUtils()
                template_fields = self.get_template()
                bqu.copy_tag(self.template_id, template_fields, uri, column, fields)
            
            if creation_status == constants.SUCCESS and tag_stream:
                psu = ps.PubSubUtils()
                psu.copy_tag(self.template_id, uri, column, fields)

        # outer loop ends here                
                                 
        return creation_status

        

    def apply_entry_config(self, fields, uri, config_uuid, template_uuid, tag_history, tag_stream):
        
        print('** apply_entry_config **')
        
        creation_status = constants.SUCCESS
        store = te.TagEngineUtils()
        gcs_client = storage.Client()
        
        bucket_name, filename = uri
        bucket = gcs_client.get_bucket(bucket_name)
        blob = bucket.get_blob(filename)
        
        entry_group_short_name = bucket_name.replace('-', '_')
        entry_group_full_name = 'projects/' + self.template_project + '/locations/' + self.template_region + '/entryGroups/' + bucket_name.replace('-', '_')
        
        # create the entry group    
        is_entry_group = self.entry_group_exists(entry_group_full_name)
        print('is_entry_group: ', is_entry_group)
        
        if is_entry_group != True:
            self.create_entry_group(entry_group_short_name)
        
        # generate the entry id, replace '/' with '_' and remove the file extension from the name
        entry_id = filename.split('.')[0].replace('/', '_')
         
        try:
            entry_name = entry_group_full_name + '/entries/' + entry_id
            print('Info: entry_name: ', entry_name)
            
            entry = self.client.get_entry(name=entry_name)
            print('Info: entry already exists: ', entry.name)
            
        except Exception as e: 
            print('Info: entry does not exist')   
         
            # populate the entry
            entry = datacatalog.Entry()
            entry.name = filename
            
            entry.display_name = entry_id 
            entry.type_ = 'FILESET'
            entry.gcs_fileset_spec.file_patterns = ['gs://' + bucket_name + '/' + filename]
            entry.fully_qualified_name = 'gs://' + bucket_name + '/' + filename
            entry.source_system_timestamps.create_time = datetime.utcnow() 
            entry.source_system_timestamps.update_time = datetime.utcnow() 
            
            # get the file's schema
            # download the file to App Engine's tmp directory 
            tmp_file = '/tmp/' + entry_id
            blob.download_to_filename(filename=tmp_file)
        
            # validate that it's a parquet file
            try:
                parquet.ParquetFile(tmp_file)
            except Exception as e:
                # not a parquet file, ignore it
                print('The file ' + filename + ' is not a parquet file, ignoring it.')
                creation_status = constants.ERROR
                return creation_status   
        
            schema = parquet.read_schema(tmp_file, memory_map=True)
            df = pd.DataFrame(({"column": name, "datatype": str(pa_dtype)} for name, pa_dtype in zip(schema.names, schema.types)))
            df = df.reindex(columns=["column", "datatype"], fill_value=pd.NA)  
            #print('df: ', df)

            for index, row in df.iterrows():                            
                entry.schema.columns.append(
                   types.ColumnSchema(
                       column=row['column'],
                       type_=row['datatype'],
                       description=None,
                       mode=None
                   )
                ) 
                                         
            # create the entry
            #print('entry request: ', entry)            
            created_entry = self.client.create_entry(parent=entry_group_full_name, entry_id=entry_id, entry=entry)
            print('Info: created entry: ', created_entry.name)
            
            # get the number of rows in the file
            num_rows = parquet.ParquetFile(tmp_file).metadata.num_rows
            #print('num_rows: ', num_rows)
            
            # delete the tmp file ASAP to free up memory
            os.remove(tmp_file)
            
            # create the file metadata tag
            template_path = self.client.tag_template_path(self.template_project, self.template_region, self.template_id)
            tag = datacatalog.Tag()
            tag.template = template_path
    
            for field in fields:
                
                if field['field_id'] == 'name':
                    string_field = datacatalog.TagField()
                    string_field.string_value = filename
                    tag.fields['name'] = string_field
                    field['field_value'] = filename # field_value is used by the BQ exporter
                    
                if field['field_id'] == 'bucket':
                    string_field = datacatalog.TagField()
                    string_field.string_value = bucket_name
                    tag.fields['bucket'] = string_field
                    field['field_value'] = bucket_name # field_value is used by the BQ exporter
                    
                if field['field_id'] == 'path':
                    string_field = datacatalog.TagField()
                    string_field.string_value = 'gs://' + bucket_name + '/' + filename
                    tag.fields['path'] = string_field
                    field['field_value'] = 'gs://' + bucket_name + '/' + filename # field_value is used by the BQ exporter
    
                if field['field_id'] == 'type':
                    enum_field = datacatalog.TagField()
                    enum_field.enum_value.display_name = 'PARQUET' # hardcode file extension for now
                    tag.fields['type'] = enum_field
                    field['field_value'] = 'PARQUET' # field_value is used by the BQ exporter
    
                if field['field_id'] == 'size':
                    double_field = datacatalog.TagField()
                    double_field.double_value = blob.size
                    tag.fields['size'] = double_field
                    field['field_value'] = blob.size # field_value is used by the BQ exporter

                if field['field_id'] == 'num_rows':
                    double_field = datacatalog.TagField()
                    double_field.double_value = num_rows
                    tag.fields['num_rows'] = double_field
                    field['field_value'] = num_rows # field_value is used by the BQ exporter

                if field['field_id'] == 'created_time':
                     datetime_field = datacatalog.TagField()
                     datetime_field.timestamp_value = blob.time_created
                     tag.fields['created_time'] = datetime_field
                     field['field_value'] = blob.time_created # field_value is used by the BQ exporter

                if field['field_id'] == 'updated_time':    
                     datetime_field = datacatalog.TagField()
                     datetime_field.timestamp_value = blob.time_created
                     tag.fields['updated_time'] = datetime_field
                     field['field_value'] = blob.time_created # field_value is used by the BQ exporter
 
                if field['field_id'] == 'storage_class':              
                      string_field = datacatalog.TagField()
                      string_field.string_value = blob.storage_class
                      tag.fields['storage_class'] = string_field
                      field['field_value'] = blob.storage_class # field_value is used by the BQ exporter
            
                if field['field_id'] == 'content_encoding':   
                    if blob.content_encoding:
                        string_field = datacatalog.TagField()
                        string_field.string_value = blob.content_encoding
                        tag.fields['content_encoding'] = string_field
                        field['field_value'] = blob.content_encoding # field_value is used by the BQ exporter
            
                if field['field_id'] == 'content_language':
                    if blob.content_language:
                        string_field = datacatalog.TagField()
                        string_field.string_value = blob.content_language
                        tag.fields['content_language'] = string_field
                        field['field_value'] = blob.content_language # field_value is used by the BQ exporter

                if field['field_id'] == 'media_link':            
                    string_field = datacatalog.TagField()
                    string_field.string_value = blob.media_link
                    tag.fields['media_link'] = string_field
                    field['field_value'] = blob.media_link # field_value is used by the BQ exporter

            #print('tag request: ', tag)
            created_tag = self.client.create_tag(parent=entry_name, tag=tag)
            #print('created_tag: ', created_tag)
            
            if tag_history:
                bqu = bq.BigQueryUtils()
                template_fields = self.get_template()
                bqu.copy_tag(self.template_id, template_fields, '/'.join(uri), None, fields)
            
            if tag_stream:
                psu = ps.PubSubUtils()
                psu.copy_tag(self.template_id, uri, column, fields)
                                    
        return creation_status


    def entry_group_exists(self, entry_group_full_name):
    
        request = datacatalog.GetEntryGroupRequest(name=entry_group_full_name)
        
        try:
            response = self.client.get_entry_group(request=request)
            return True
        except Exception as e:
            return False
    
    
    def create_entry_group(self, entry_group_short_name):
    
        eg = datacatalog.EntryGroup()
        eg.display_name = entry_group_short_name
        
        entry_group = self.client.create_entry_group(
                    parent='projects/' + self.template_project + '/locations/' + self.template_region,
                    entry_group_id=entry_group_short_name,
                    entry_group=eg)
        
        print('created entry_group: ', entry_group.name)
        return entry_group.name
           

    def apply_glossary_asset_config(self, fields, mapping_table, uri, config_uuid, template_uuid, tag_history, tag_stream, overwrite=False):
        
        print('** enter apply_glossary_asset_config **')
        #print('fields: ', fields)
        print('mapping_table: ', mapping_table)
        print('uri: ', uri)
        #print('config_uuid: ', config_uuid)
        #print('template_uuid: ', template_uuid)
        #print('tag_history: ', tag_history)
        #print('tag_stream: ', tag_stream)
 
        # uri is either a BQ table/view path or GCS file path
        store = te.TagEngineUtils()        
        creation_status = constants.SUCCESS
        
        is_gcs = False
        is_bq = False
        
        # look up the entry based on the resource type
        if isinstance(uri, list):
            is_gcs = True
            bucket = uri[0].replace('-', '_')
            filename = uri[1].split('.')[0].replace('/', '_') # extract the filename without the extension, replace '/' with '_'
            gcs_resource = '//datacatalog.googleapis.com/projects/' + self.template_project + '/locations/' + self.template_region + '/entryGroups/' + bucket + '/entries/' + filename
            #print('gcs_resource: ', gcs_resource)
            request = datacatalog.LookupEntryRequest()
            request.linked_resource=gcs_resource
            
            try:
                entry = self.client.lookup_entry(request)
                print('entry: ', entry.name)
            except Exception as e:
                print('Unable to find entry in the catalog. Entry ' + gcs_resource + ' does not exist.')
                creation_status = constants.ERROR
                return creation_status
                #print('entry found: ', entry)
        
        elif isinstance(uri, str):
            is_bq = True        
            bigquery_resource = '//bigquery.googleapis.com/projects/' + uri
            print("bigquery_resource: " + bigquery_resource)
        
            request = datacatalog.LookupEntryRequest()
            request.linked_resource=bigquery_resource
            entry = self.client.lookup_entry(request)
            print('entry: ', entry.name)
        
        try:    
            tag_exists, tag_id = self.check_if_exists(parent=entry.name)
            print('tag_exists: ', tag_exists)
        
        except Exception as e:
            print('Error during check_if_exists: ', e)
            creation_status = constants.ERROR
            return creation_status

        if tag_exists and overwrite == False:
            print('Info: tag already exists and overwrite set to False')
            creation_status = constants.SUCCESS
            return creation_status
         
        if entry.schema == None:
            print('Error: entry ' + entry.name + ' does not have a schema in the catalog.')
            creation_status = constants.ERROR
            return creation_status
        
        # retrieve the schema columns from the entry
        column_schema_str = ''
        for column_schema in entry.schema.columns: 
            column_schema_str += "'" + column_schema.column + "',"
        
        #print('column_schema_str: ', column_schema_str)
             
        mapping_table_formatted = mapping_table.replace('bigquery/project/', '').replace('/dataset/', '.').replace('/', '.')
                
        query_str = 'select canonical_name from `' + mapping_table_formatted + '` where source_name in (' + column_schema_str[0:-1] + ')'
        #print('query_str: ', query_str)

        # run query against mapping table
        bq_client = bigquery.Client(location=BIGQUERY_REGION)
        rows = bq_client.query(query_str).result()
        
        tag = datacatalog.Tag()
        tag.template = self.template_path
        
        tag_is_empty = True
        
        for row in rows:
            canonical_name = row['canonical_name']
            #print('canonical_name: ', canonical_name)
        
            for field in fields:
                if field['field_id'] == canonical_name:
                    #print('found match')
                    bool_field = datacatalog.TagField()
                    bool_field.bool_value = True
                    tag.fields[canonical_name] = bool_field
                    field['field_value'] = True
                    tag_is_empty = False
                    break
                    
        if tag_is_empty:
            print("Error: can't create the tag because it's empty")
            creation_status = constants.ERROR
            return creation_status
                            
        if tag_exists:
            # tag already exists and overwrite is True
            tag.name = tag_id
            
            try:
                print('tag update: ', tag)
                response = self.client.update_tag(tag=tag)
            except Exception as e:
                msg = 'Error occurred during tag update: ' + str(e)
                store.write_tag_op_error(constants.TAG_UPDATED, config_uuid, 'GLOSSARY_ASSET_TAG', msg)
                
                # sleep and retry the tag update
                if 'Quota exceeded for quota metric' or '503 The service is currently unavailable' in str(e):
                    print('sleep for 3 minutes due to ' + str(e))
                    time.sleep(180)
                    
                    try:
                        response = self.client.update_tag(tag=tag)
                    except Exception as e:
                        msg = 'Error occurred during tag update after sleep: ' + str(e)
                        store.write_tag_op_error(constants.TAG_UPDATED, config_uuid, 'GLOSSARY_ASSET_TAG', msg)
        else:
            try:
                print('tag create: ', tag)
                response = self.client.create_tag(parent=entry.name, tag=tag)
            except Exception as e:
                msg = 'Error occurred during tag create: ' + str(e) + '. Failed tag request = ' + str(tag)
                store.write_tag_op_error(constants.TAG_CREATED, config_uuid, 'GLOSSARY_ASSET_TAG', msg)
                
                # sleep and retry write
                if 'Quota exceeded for quota metric' or '503 The service is currently unavailable' in str(e):
                    print('sleep for 3 minutes due to ' + str(e))
                    time.sleep(180)
                    
                    try:
                        response = self.client.create_tag(parent=entry.name, tag=tag)
                    except Exception as e:
                        msg = 'Error occurred during tag create after sleep: ' + str(e)
                        store.write_tag_op_error(constants.TAG_CREATED, config_uuid, 'GLOSSARY_ASSET_TAG', msg)
                    
        if tag_history:
            bqu = bq.BigQueryUtils()
            template_fields = self.get_template()
            if is_gcs:
                bqu.copy_tag(self.template_id, template_fields, '/'.join(uri), None, fields)
            if is_bq:
                bqu.copy_tag(self.template_id, template_fields, uri, None, fields)
        
        if tag_stream:
            psu = ps.PubSubUtils()
            if is_gcs:
                bqu.copy_tag(self.template_id, '/'.join(uri), None, fields)
            if is_bq:
                psu.copy_tag(self.template_id, uri, None, fields)
           
        return creation_status
      
                 
    def apply_sensitive_column_config(self, fields, dlp_dataset, infotype_selection_table, infotype_classification_table, \
                                      uri, create_policy_tags, taxonomy_id, config_uuid, template_uuid, \
                                      tag_history, tag_stream, overwrite=False):
        
        print('** enter apply_sensitive_column_config **')
        print('fields: ', fields)
        print('dlp_dataset: ', dlp_dataset)
        print('infotype_selection_table: ', infotype_selection_table)
        print('infotype_classification_table: ', infotype_classification_table)
        print('uri: ', uri)
        print('create_policy_tags: ', create_policy_tags)
        print('taxonomy_id: ', taxonomy_id)
        print('config_uuid: ', config_uuid)
        print('template_uuid: ', template_uuid)
        
        if create_policy_tags:
            ptm_client = datacatalog.PolicyTagManagerClient()
            
            request = datacatalog.ListPolicyTagsRequest(
                parent=taxonomy_id
            )

            try:
                page_result = ptm_client.list_policy_tags(request=request)
            except Exception as e:
                print('Unable to retrieve the policy tag taxonomy for taxonomy_id ' + taxonomy_id + '. Error message: ', e)
                creation_status = constants.ERROR
                return creation_status    

            policy_tag_names = [] # list of fully qualified policy tag names and sensitive categories

            for response in page_result:
                policy_tag_names.append((response.name, response.display_name))
        
            #print('policy_tag_names: ', policy_tag_names)
            
            policy_tag_requests = [] # stores the list of fully qualified policy tag names and table column names, 
                                     # so that we can create the policy tags on the various sensitive fields
 
        # uri is a BQ table path 
        store = te.TagEngineUtils()        
        creation_status = constants.SUCCESS
        column = ''
        
        if isinstance(uri, str) == False:
            print('Error: url ' + str(url) + ' is not of type string.')
            creation_status = constants.ERROR
            return creation_status
            
        bigquery_resource = '//bigquery.googleapis.com/projects/' + uri
        #print("bigquery_resource: ", bigquery_resource)
        
        request = datacatalog.LookupEntryRequest()
        request.linked_resource=bigquery_resource
        
        try:
            entry = self.client.lookup_entry(request)
        except Exception as e:
            print('Error looking up entry ' + bigquery_resource + ' in the catalog. Error message: ', e)
            creation_status = constants.ERROR
            return creation_status
             
        dlp_dataset = dlp_dataset.replace('bigquery/project/', '').replace('/dataset/', '.').replace('/', '.')        
        infotype_selection_table = infotype_selection_table.replace('bigquery/project/', '').replace('/dataset/', '.').replace('/', '.')
        infotype_classification_table = infotype_classification_table.replace('bigquery/project/', '').replace('/dataset/', '.').replace('/', '.')
        dlp_table = dlp_dataset + '.' + uri.split('/')[4]
        
        #print('dlp_dataset: ', dlp_dataset)
        #print('dlp_table: ', dlp_table)
                
        infotype_fields = []
        notable_infotypes = []
    
        # get an array of infotypes associated with each field in the DLP findings table
        dlp_sql = 'select field, array_agg(infotype) infotypes '
        dlp_sql += 'from (select distinct cl.record_location.field_id.name as field, info_type.name as infotype '
        dlp_sql += 'from ' + dlp_table + ', unnest(location.content_locations) as cl '
        dlp_sql += 'order by cl.record_location.field_id.name) '
        dlp_sql += 'group by field'
        
        #print('dlp_sql: ', dlp_sql)
        
        bq_client = bigquery.Client(location=BIGQUERY_REGION)
        
        try:
            dlp_rows = bq_client.query(dlp_sql).result()
        
        except Exception as e:
            print('Error querying DLP findings table. Error message: ', e)
            creation_status = constants.ERROR
            return creation_status

        dlp_row_count = 0
    
        for dlp_row in dlp_rows:
        
            dlp_row_count += 1
        
            field = dlp_row['field']
            infotype_fields.append(field)
            infotypes = dlp_row['infotypes']
        
            print('field ', field, ', infotypes [', infotypes, ']')
        
            is_sql = 'select notable_infotype '
            is_sql += 'from ' + infotype_selection_table + ' i, '
        
            infotype_count = len(infotypes)
        
            for i in range(0, infotype_count):
            
                is_sql += 'unnest(i.field_infotypes) as i' + str(i) + ', '
        
            is_sql = is_sql[:-2] + ' '
            
            for i, infotype in enumerate(infotypes):
            
                if i == 0:
                    is_sql += 'where i' + str(i) + ' = "' + infotype + '" ' 
                else:
                    is_sql += 'and i' + str(i) + ' = "' + infotype + '" ' 
        
            is_sql += 'order by array_length(i.field_infotypes) '
            is_sql += 'limit 1'
        
            #print('is_sql: ', is_sql)
            
            try:
                ni_rows = bq_client.query(is_sql).result()
            except Exception as e:
                print('Error querying infotype selection table. Error message: ', e)
                creation_status = constants.ERROR
                return creation_status
        
            for ni_row in ni_rows:
                notable_infotypes.append(ni_row['notable_infotype']) # there should be just one notable infotype per field
    
        # there are no DLP findings
        if dlp_row_count == 0:
            creation_status = constants.SUCCESS
            return creation_status
    
        # remove duplicate infotypes from notable list
        final_set = list(set(notable_infotypes))
        print('final_set: ', final_set)
        
        # lookup classification using set of notable infotypes   
        c_sql = 'select classification_result '
        c_sql += 'from ' + infotype_classification_table + ' c, '
    
        for i in range(0, len(final_set)):
            c_sql += 'unnest(c.notable_infotypes) as c' + str(i) + ', '
    
        c_sql = c_sql[:-2] + ' '
    
        for i, notable_infotype in enumerate(final_set):
        
            if i == 0:
                c_sql += 'where c' + str(i) + ' = "' + notable_infotype + '" '
            else:
                c_sql += 'and c' + str(i) + ' = "' + notable_infotype + '" '

        c_sql += 'order by array_length(c.notable_infotypes) '
        c_sql += 'limit 1'  

        #print('c_sql: ', c_sql)
    
        try:
            c_rows = bq_client.query(c_sql).result()
        except Exception as e:
            print('Error querying infotype classification table. Error message: ', e)
            creation_status = constants.ERROR
            return creation_status
        
        classification_result = None
    
        for c_row in c_rows:
            classification_result = c_row['classification_result'] # we should end up with one classification result per table
    
        print('classification_result: ', classification_result)
        
        tag = datacatalog.Tag()
        tag.template = self.template_path
        
        # each element represents a field which needs to be tagged
        for infotype_field in infotype_fields:
            
            for field in fields:
                if 'sensitive_field' in field['field_id']:
                    bool_field = datacatalog.TagField()
                    
                    if classification_result == 'Public_Information':
                        bool_field.bool_value = False
                        field['field_value'] = False
                    else:
                        bool_field.bool_value = True
                        field['field_value'] = True
                    
                    tag.fields['sensitive_field'] = bool_field
                    
                if 'sensitive_type' in field['field_id']:
                    enum_field = datacatalog.TagField()
                    enum_field.enum_value.display_name = classification_result
                    tag.fields['sensitive_type'] = enum_field
                    field['field_value'] = classification_result
           
            tag.column = infotype_field # DLP has a bug and sometimes the infotype field does not equal to the column name in the table
            print('tag.column: ', infotype_field)
            
            # check if a tag already exists on this column
            try:    
                tag_exists, tag_id = self.check_if_exists(parent=entry.name, column=infotype_field)
        
            except Exception as e:
                print('Error during check_if_exists: ', e)
                creation_status = constants.ERROR
                return creation_status   
            
            # tag already exists    
            if tag_exists:
                
                if overwrite == False:
                    # skip this sensitive column because it is already tagged
                    continue
                
                tag.name = tag_id
            
                try:
                    print('tag update request: ', tag)
                    response = self.client.update_tag(tag=tag)
                
                    #print('response: ', response)
                    
                except Exception as e:
                    msg = 'Error occurred during tag update: ' + str(e) + '. Failed tag request = ' + str(tag)
                    store.write_tag_op_error(constants.TAG_UPDATED, config_uuid, 'SENSITIVE_TAG', msg)
                
                    # sleep and retry the tag update
                    if 'Quota exceeded for quota metric' or '503 The service is currently unavailable' in str(e):
                        print('sleep for 3 minutes due to ' + str(e))
                        time.sleep(180)
                    
                        try:
                            response = self.client.update_tag(tag=tag)
                        except Exception as e:
                            msg = 'Error occurred during tag update after sleep: ' + str(e)
                            print(msg)
                            store.write_tag_op_error(constants.TAG_UPDATED, config_uuid, 'SENSITIVE_TAG', msg)
            else:
                try:
                    print('tag create request: ', tag)
                    response = self.client.create_tag(parent=entry.name, tag=tag)

                except Exception as e:
                    msg = 'Error occurred during tag create: ' + str(e) + '. Failed tag request = ' + str(tag)
                    print(msg)
                    store.write_tag_op_error(constants.TAG_CREATED, config_uuid, 'SENSITIVE_TAG', msg)
                
                    # sleep and retry write
                    if 'Quota exceeded for quota metric' or '503 The service is currently unavailable' in str(e):
                        print('sleep for 3 minutes due to ' + str(e))
                        time.sleep(180)
                    
                        try:
                            response = self.client.create_tag(parent=entry.name, tag=tag)
                        except Exception as e:
                            msg = 'Error occurred during tag create after sleep: ' + str(e)
                            print(msg)
                            store.write_tag_op_error(constants.TAG_CREATED, config_uuid, 'SENSITIVE_TAG', msg)
                    else:
                        # could not create the tag, could be due to a column mismatch
                        creation_status = constants.ERROR
                    
            if creation_status == constants.SUCCESS and create_policy_tags and classification_result != 'Public_Information':
                # add the column name and policy tag name to a list
                for policy_tag_name, policy_tag_category in policy_tag_names:
                    if policy_tag_category == classification_result:
                        policy_tag_requests.append((infotype_field, policy_tag_name))
                    
                            
            if creation_status == constants.SUCCESS and tag_history:
                bqu = bq.BigQueryUtils()
                template_fields = self.get_template()
                bqu.copy_tag(self.template_id, template_fields, uri, infotype_field, fields)
        
            if creation_status == constants.SUCCESS and tag_stream:
                psu = ps.PubSubUtils()
                psu.copy_tag(self.template_id, uri, infotype_field, fields)
        
                
        # Once we have created the regular tags, we can create/update the policy tags
        if create_policy_tags and len(policy_tag_requests) > 0:
            table_id = uri.replace('/datasets/', '.').replace('/tables/', '.')
            self.apply_policy_tags(table_id, policy_tag_requests)
            
           
        return creation_status

    
    def apply_policy_tags(self, table_id, policy_tag_requests):
        
        success = True
        
        bq_client = bigquery.Client(location=BIGQUERY_REGION)
        table = bq_client.get_table(table_id) 
        schema = table.schema

        new_schema = []
        
        for field in schema:
            
            field_match = False
            
            for column, policy_tag_name in policy_tag_requests:
                
                if field.name == column:
                    print('applying policy tag on', field.name)
                    policy = bigquery.schema.PolicyTagList(names=[policy_tag_name,])
                    new_schema.append(bigquery.schema.SchemaField(field.name, field.field_type, field.mode, policy_tags=policy)) 
                    field_match = True
                    break
        
            if field_match == False:    
                new_schema.append(field)
                
        table.schema = new_schema
        
        try:
            table = bq_client.update_table(table, ["schema"])  
        
        except Exception as e:
            print('Error occurred while updating the schema of', table_id, 'Error message:', e)
            success = False
        
        return success
        
            
    def apply_export_config(self, config_uuid, target_project, target_dataset, target_region, uri):
        
        print('** enter apply_export_config **')
        print('config_uuid:', config_uuid)
        print('target_project:', target_project)
        print('target_dataset:', target_dataset)
        print('target_region:', target_region)
        print('uri:', uri)
        
        column_tag_records = []
        table_tag_records = []
        dataset_tag_records = []
        
        export_status = constants.SUCCESS
        bqu = bq.BigQueryUtils(target_region)
        
        if isinstance(uri, str) == False:
            print('Error: url ' + str(url) + ' is not of type string.')
            export_status = constants.ERROR
            return export_status
        
        tagged_project = uri.split('/')[0]
        tagged_dataset = uri.split('/')[2]
        
        if '/tables/' in uri:
            target_table_id = 'catalog_report_table_tags'
            tagged_table = uri.split('/')[4]
        else:
            target_table_id = 'catalog_report_dataset_tags'
            tagged_table = None
            
        bigquery_resource = '//bigquery.googleapis.com/projects/' + uri
        #print("bigquery_resource: ", bigquery_resource)
        
        request = datacatalog.LookupEntryRequest()
        request.linked_resource=bigquery_resource
        
        try:
            entry = self.client.lookup_entry(request)
        except Exception as e:
            print('Error looking up entry ' + bigquery_resource + ' in the catalog. Error message: ', e)
            export_status = constants.ERROR
            return export_status

        tag_list = self.client.list_tags(parent=entry.name, timeout=120)
    
        for tag in tag_list:
            print('tag.template:', tag.template)
            print('tag.column:', tag.column)
            
            # get tag template fields
            self.template_id = tag.template.split('/')[5]
            self.template_project = tag.template.split('/')[1]
            self.template_region = tag.template.split('/')[3]
            self.template_path = tag.template
            template_fields = self.get_template()
            
            if tag.column and len(tag.column) > 1:
                tagged_column = tag.column
                target_table_id = 'catalog_report_column_tags'
            else:
                tagged_column = None
                target_table_id = 'catalog_report_table_tags'
            
            for template_field in template_fields:
    
                #print('template_field:', template_field)
                field_id = template_field['field_id']
                
                if field_id not in tag.fields:
                    continue
                    
                tagged_field = tag.fields[field_id]
                tagged_field_str = str(tagged_field)
                tagged_field_split = tagged_field_str.split('\n')
                #print('tagged_field_split:', tagged_field_split)
                
                split_index = 0
                
                for split in tagged_field_split:
                    if '_value:' in split:
                        start_index = split.index(':', 0) + 1
                        #print('start_index:', start_index)
                        field_value = split[start_index:].strip().replace('"', '').replace('<br>', ',')
                        print('extracted field_value:', field_value)
                        break
                    elif 'enum_value' in split:
                        field_value = tagged_field_split[split_index+1].replace('display_name:', '').replace('"', '').strip()
                        print('extracted field_value:', field_value)
                        break
                    
                    split_index += 1                    
                    
                # format record to be written
                current_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S") + " UTC"
                
                if target_table_id in 'catalog_report_column_tags':
                    column_tag_records.append({"project": tagged_project, "dataset": tagged_dataset, "table": tagged_table, "column": tagged_column, "tag_template": self.template_id, "tag_field": field_id, "tag_value": field_value, "export_time": current_ts})
                
                elif target_table_id in 'catalog_report_table_tags':
                    table_tag_records.append({"project": tagged_project, "dataset": tagged_dataset, "table": tagged_table, "tag_template": self.template_id, "tag_field": field_id, "tag_value": field_value, "export_time": current_ts})
                
                elif target_table_id in 'catalog_report_dataset_tags':
                    dataset_tag_records.append({"project": tagged_project, "dataset": tagged_dataset, "tag_template": self.template_id, "tag_field": field_id, "tag_value": field_value, "export_time": current_ts})
                      
        # write exported records to BQ
        if len(dataset_tag_records) > 0:
            target_table_id = target_project + '.' + target_dataset + '.catalog_report_dataset_tags'
            success = bqu.insert_exported_records(target_table_id, dataset_tag_records)
        
        if len(table_tag_records) > 0:
            target_table_id = target_project + '.' + target_dataset + '.catalog_report_table_tags'
            success = bqu.insert_exported_records(target_table_id, table_tag_records)
                    
        if len(column_tag_records) > 0:
            target_table_id = target_project + '.' + target_dataset + '.catalog_report_column_tags'
            success = bqu.insert_exported_records(target_table_id, column_tag_records)
                     
        return export_status
        
            
    def apply_import_config(self, config_uuid, tag_dict, tag_history, tag_stream, overwrite=False):
        
        print('** enter apply_import_config **')
        #print('tag_dict: ', tag_dict)
             
        creation_status = constants.SUCCESS
        
        project = tag_dict['project']
        dataset = tag_dict['dataset']
        table = tag_dict['table']

        bigquery_resource = '//bigquery.googleapis.com/projects/' + project + '/datasets/' + dataset + '/tables/' + table

        request = datacatalog.LookupEntryRequest()
        request.linked_resource=bigquery_resource
        
        try:
            entry = self.client.lookup_entry(request)
        except Exception as e:
            print("Error: could not find the entry for ", bigquery_resource, ". Error: ", e)
            creation_status = constants.ERROR
            return creation_status

        if 'column' in tag_dict:
            column_name = tag_dict['column']
            uri = entry.linked_resource.replace('//bigquery.googleapis.com/projects/', '') + '/column/' + column_name
        else:
            column_name = None
            uri = entry.linked_resource.replace('//bigquery.googleapis.com/projects/', '')
            
        try:    
            tag_exists, tag_id = self.check_if_exists(parent=entry.name, column=column_name)

        except Exception as e:
            print('Error during check_if_exists: ', e)
            creation_status = constants.ERROR
            return creation_status

        if tag_exists and overwrite == False:
            print('Tag already exists and overwrite flag is False')
            creation_status = constants.SUCCESS
            return creation_status
        
        tag_fields = []
        template_fields = self.get_template()
        
        for field_name in tag_dict:
           
            if field_name == 'project' or field_name == 'dataset' or field_name == 'table' or field_name == 'column':
                continue
        
            field_type = None
            field_value = tag_dict[field_name].strip()
            
            for template_field in template_fields:
                if template_field['field_id'] == field_name:
                    field_type = template_field['field_type']
                    break
    
            if field_type == None:
                print('Error while preparing the tag. The field ', field_name, ' was not found in the tag template ', self.template_id)
                creation_status = constants.ERROR
                return creation_status
    
            field = {'field_id': field_name, 'field_type': field_type, 'field_value': field_value}    
            tag_fields.append(field)
            
        
        creation_status = self.create_update_tag(tag_fields, tag_exists, tag_id, config_uuid, 'IMPORT_TAG', tag_history, tag_stream, \
                                                 entry, uri, column_name)
                                
        return creation_status
    

    def apply_restore_config(self, config_uuid, tag_extract, tag_history, tag_stream, overwrite=False):
        
        print('** enter apply_restore_config **')
        print('config_uuid:', config_uuid)
        print('tag_extract: ', tag_extract)
              
        creation_status = constants.SUCCESS
        
        for json_obj in tag_extract:
            #print('json_obj: ', json_obj)
        
            entry_group = json_obj['entryGroupId']
            entry_id = json_obj['id']
            location_id = json_obj['locationId']
            project_id = json_obj['projectId']
    
            #print('entry_group: ', entry_group)
            #print('entry_id: ', entry_id)
        
            entry_name = 'projects/' + project_id + '/locations/' + location_id + '/entryGroups/' + entry_group + '/entries/' + entry_id
            print('entry_name: ', entry_name)
    
            try:
                entry = self.client.get_entry(name=entry_name)
                
            except Exception as e:
                print("Error: couldn't find the entry: ", e)
                creation_status = constants.ERROR
                return creation_status
            
            if 'columns' in json_obj:
                # column-level tag
                json_columns = json_obj['columns']
                #print('json_columns: ', json_columns)
                
                for column_obj in json_columns:
                
                    column_name = column_obj['name'].split(':')[1]
                    column_tags = column_obj['tags']
                    fields = column_tags[0]['fields']
                                     
                    try:    
                        tag_exists, tag_id = self.check_if_exists(parent=entry.name, column=column_name)
        
                    except Exception as e:
                        print('Error during check_if_exists: ', e)
                        creation_status = constants.ERROR
                        return creation_status

                    if tag_exists and overwrite == False:
                        print('Tag already exists and overwrite flag is False')
                        creation_status = constants.SUCCESS
                        return creation_status
            
                    # create or update column-level tag
                    uri = entry.linked_resource.replace('//bigquery.googleapis.com/projects/', '') + '/column/' + column_name
                    creation_status = self.create_update_tag(fields, tag_exists, tag_id, config_uuid, 'RESTORE_TAG', tag_history, tag_stream, \
                                                             entry, uri, column_name)
            
            if 'tags' in json_obj:
                # table-level tag
                json_tags = json_obj['tags'] 
                fields = json_tags[0]['fields']
                #print('fields: ', fields)  
                
                try:    
                    tag_exists, tag_id = self.check_if_exists(parent=entry.name, column='')
    
                except Exception as e:
                    print('Error during check_if_exists: ', e)
                    creation_status = constants.ERROR
                    return creation_status

                if tag_exists and overwrite == False:
                    print('Tag already exists and overwrite flag is False')
                    creation_status = constants.SUCCESS
                    return creation_status
                
                # create or update table-level tag
                uri = entry.linked_resource.replace('//bigquery.googleapis.com/projects/', '')
                creation_status = self.create_update_tag(fields, tag_exists, tag_id, config_uuid, 'RESTORE_TAG', tag_history, \
                                                         tag_stream, entry, uri)                     
                    
        return creation_status
        
    # used by apply_static_assets_config, apply_import_config, and apply_restore_config
    def create_update_tag(self, fields, tag_exists, tag_id, config_uuid, config_type, tag_history, tag_stream, entry, uri, column_name=''):
        
        print('create_update_tag')
        print('tag_history:', tag_history)
        
        creation_status = constants.SUCCESS
        valid_field = False
        store = te.TagEngineUtils() 
        tag = datacatalog.Tag()
        tag.template = self.template_path

        for field in fields:
            
            if 'name' in field:
                valid_field = True
                field_id = field['name']
                field_type = field['type']
                field_value = field['value']
                
                # rename the keys, which will be used by tag history
                if tag_history:
                    field['field_id'] = field['name']
                    field['field_type'] = field['type']
                    field['field_value'] = field['value']
                    del field['name']
                    del field['type']
                    del field['value']
                
            elif 'field_id' in field:
                valid_field = True
                field_id = field['field_id']
                field_type = field['field_type'].upper()
                field_value = field['field_value']
                
            else:
                # export file contains invalid tags (e.g. a tagged field without a name)
                continue
            
            if field_type == 'BOOL':
                bool_field = datacatalog.TagField()

                if isinstance(field_value, str):
                    if field_value == 'TRUE':
                        bool_field.bool_value = True
                    else:
                        bool_field.bool_value = False
                else:
                    bool_field.bool_value = field_value

                tag.fields[field_id] = bool_field

            if field_type == 'STRING':
                string_field = datacatalog.TagField()
                string_field.string_value = str(field_value)
                tag.fields[field_id] = string_field
            if field_type == 'DOUBLE':
                float_field = datacatalog.TagField()
                float_field.double_value = float(field_value)
                tag.fields[field_id] = float_field
            if field_type == 'RICHTEXT':
                richtext_field = datacatalog.TagField()
                richtext_field.richtext_value = field_value.replace(',', '<br>')
                tag.fields[field_id] = richtext_field
                
                # For richtext values, replace '<br>' with ',' when exporting to BQ
                field['field_value'] = field_value.replace('<br>', ', ')
                
            if field_type == 'ENUM':
                enum_field = datacatalog.TagField()
                enum_field.enum_value.display_name = field_value
                tag.fields[field_id] = enum_field
            if field_type == 'DATETIME' or field_type == 'TIMESTAMP': 
                # field_value could be a date value e.g. "2022-05-08" or a datetime value e.g. "2022-05-08 15:00:00"
                utc = pytz.timezone('UTC')

                if len(field_value) == 10:
                    d = date(int(field_value[0:4]), int(field_value[5:7]), int(field_value[8:10]))
                    dt = datetime.combine(d, dtime(00, 00)) # when no time is supplied, default to 12:00:00 AM UTC  
                else:
                    # raw timestamp format: 2022-05-11 21:18:20
                    d = date(int(field_value[0:4]), int(field_value[5:7]), int(field_value[8:10]))
                    t = dtime(int(field_value[11:13]), int(field_value[14:16]))
                    dt = datetime.combine(d, t)
    
                timestamp = utc.localize(dt)
                #print('timestamp: ', timestamp)    
                datetime_field = datacatalog.TagField()
                datetime_field.timestamp_value = timestamp
                tag.fields[field_id] = datetime_field
                field['field_value'] = timestamp  # store this value back in the field, so it can be exported
    
        # export file can have invalid tags, skip tag creation if that's the case 
        if valid_field == False:
            creation_status = constants.ERROR
            return creation_status
        
        if column_name != '':
            tag.column = column_name
            #print('tag.column == ' + column)   
    
        if tag_exists == True:
            tag.name = tag_id

            try:
                print('tag update: ', tag)
                response = self.client.update_tag(tag=tag)
            except Exception as e:
                msg = 'Error occurred during tag update: ' + str(e)
                store.write_tag_op_error(constants.TAG_UPDATED, config_uuid, config_type, msg)
                
                # sleep and retry the tag update
                if 'Quota exceeded for quota metric' or '503 The service is currently unavailable' in str(e):
                    print('sleep for 3 minutes due to ' + str(e))
                    time.sleep(180)
        
                    try:
                        response = self.client.update_tag(tag=tag)
                    except Exception as e:
                        msg = 'Error occurred during tag update after sleep: ' + str(e)
                        store.write_tag_op_error(constants.TAG_UPDATED, config_uuid, config_type, msg)
                        creation_status = constants.ERROR
                        return creation_status
        else:
            try:
                print('tag create: ', tag)
                response = self.client.create_tag(parent=entry.name, tag=tag)
            except Exception as e:
                msg = 'Error occurred during tag create: ' + str(e) + '. Failed tag request = ' + str(tag)
                store.write_tag_op_error(constants.TAG_CREATED, config_uuid, config_type, msg)
    
                # sleep and retry write
                if 'Quota exceeded for quota metric' or '503 The service is currently unavailable' in str(e):
                    print('sleep for 3 minutes due to ' + str(e))
                    time.sleep(180)
        
                    try:
                        response = self.client.create_tag(parent=entry.name, tag=tag)
                    except Exception as e:
                        msg = 'Error occurred during tag create after sleep: ' + str(e)
                        store.write_tag_op_error(constants.TAG_CREATED, config_uuid, config_type, msg)
                        creation_status = constants.ERROR
                        return creation_status
        
        if tag_history:
            bqu = bq.BigQueryUtils()
            template_fields = self.get_template()
            success = bqu.copy_tag(self.template_id, template_fields, uri, column_name, fields)
            
            if success == False:
                print('Error occurred while writing to tag history table.')
                creation_status = constants.ERROR

        if tag_stream:
            psu = ps.PubSubUtils()
            psu.copy_tag(self.template_id, uri, column_name, fields)
       
        return creation_status
        
       
    def search_catalog(self, bigquery_project, bigquery_dataset):
        
        linked_resources = {}
        
        scope = datacatalog.SearchCatalogRequest.Scope()
        scope.include_project_ids.append(bigquery_project)
        
        request = datacatalog.SearchCatalogRequest()
        request.scope = scope
    
        query = 'parent:' + bigquery_project + '.' + bigquery_dataset
        print('query string: ' + query)
    
        request.query = query
        request.page_size = 1
    
        for result in self.client.search_catalog(request):
            print('result: ' + str(result))
            
            resp = self.client.list_tags(parent=result.relative_resource_name)
            tags = list(resp.tags)
            tag_count = len(tags)
            
            index = result.linked_resource.rfind('/')
            table_name = result.linked_resource[index+1:]
            linked_resources[table_name] = tag_count
            
        return linked_resources

  
    def parse_query_expression(self, uri, query_expression, column=None):
        
        print("*** enter parse_query_expression ***")
        print("uri: " + uri)
        print("query_expression: " + query_expression)
        
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
        
        #print('table_index: ', table_index)
        #print('column_index: ', column_index)
        
        if project_index != -1:
            project_end = uri.find('/') 
            project = uri[0:project_end]
            #print('project: ' + project)
            #print('project_index: ', project_index)
            
        if dataset_index != -1:
            dataset_start = uri.find('/datasets/') + 10
            dataset_string = uri[dataset_start:]
            dataset_end = dataset_string.find('/') 
            dataset = dataset_string[0:dataset_end]
            #print('dataset: ' + dataset)
            #print('dataset_index: ', dataset_index)
        
        # $table referenced in from clause, use fully qualified table
        if from_clause_table_index > 0 or from_clause_backticks_table_index > 0:
             print('$table referenced in from clause')
             qualified_table = uri.replace('/project/', '.').replace('/datasets/', '.').replace('/tables/', '.')
             print('qualified_table:', qualified_table)
             print('query_expression:', query_expression)
             query_str = query_expression.replace('$table', qualified_table)
             print('query_str:', query_str)
             
        # $table is referenced somewhere in the expression, replace $table with actual table name
        else:
        
            if table_index != -1:
                #print('$table referenced somewhere, but not in the from clause')
                table_index = uri.rfind('/') + 1
                table_name = uri[table_index:]
                #print('table_name: ' + table_name)
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
        
        print('returning query_str:', query_str)            
        return query_str
    
    
    def run_query(self, bq_client, query_str, field_type, batch_mode, store):
        
        #print('*** enter run_query ***')
        
        field_values = []
        error_exists = False
            
        try:
            
            if batch_mode:
                
                batch_config = bigquery.QueryJobConfig(
                    # run at batch priority which won't count toward concurrent rate limit
                    priority=bigquery.QueryPriority.BATCH
                )
                
                query_job = bq_client.query(query_str, job_config=batch_config)
                job = bq_client.get_job(query_job.job_id, location=query_job.location)
            
                while job.state == 'RUNNING':
                    time.sleep(2)
            
                rows = job.result()
            
            else:
                #print('query_str:', query_str)
                rows = bq_client.query(query_str).result()
                #print('rows:', rows)
            
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
                # SQL query returned nothing, log error in Firestore
                #error_exists = True
                print('query_str returned nothing, writing error entry')
                store.write_tag_value_error('sql returned nothing: ' + query_str)
        
        except Exception as e:
            error_exists = True
            print('Error occurred during run_query ' + query_str + ', Error: ' + str(e))
            store.write_tag_value_error('invalid query parameter(s): ' + query_str + ' produced error ' + str(e))
        
        #print('field_values: ', field_values)
        
        return field_values, error_exists
        

    def populate_tag_field(self, tag, field_id, field_type, field_values, store=None):
        
        print('enter populate_tag_field; field_id:', field_id)
        
        error_exists = False
        
        try:             
            if field_type == "bool":
                bool_field = datacatalog.TagField()
                bool_field.bool_value = bool(field_values[0])
                tag.fields[field_id] = bool_field
            if field_type == "string":
                string_field = datacatalog.TagField()
                string_field.string_value = str(field_values[0])
                tag.fields[field_id] = string_field
            if field_type == "richtext":
                richtext_field = datacatalog.TagField()
                formatted_value = '<br>'.join(str(v) for v in field_values)
                richtext_field.richtext_value = str(formatted_value)
                tag.fields[field_id] = richtext_field
            if field_type == "double":
                float_field = datacatalog.TagField()
                float_field.double_value = float(field_values[0])
                tag.fields[field_id] = float_field
            if field_type == "enum":
                enum_field = datacatalog.TagField()
                enum_field.enum_value.display_name = field_values[0]
                tag.fields[field_id] = enum_field
            if field_type == "datetime" or field_type == "timestamp":
                # expected format for datetime values in DC: 2020-12-02T16:34:14Z
                # however, field_value can be a date value e.g. "2022-05-08", a datetime value e.g. "2022-05-08 15:00:00"
                # or timestamp value e.g. datetime.datetime(2022, 9, 14, 18, 24, 31, 615000, tzinfo=datetime.timezone.utc)
                field_value = field_values[0]
                #print('field_value:', field_value)
                #print('field_value type:', type(field_value))
                
                # we have a datetime or timestamp 
                if type(field_value) == datetime:
                    timestamp = pytz.utc.localize(field_value)
                # we have a date
                elif type(field_value) == date:
                    dt = datetime.combine(field_value, datetime.min.time())
                    timestamp = pytz.utc.localize(dt)
                # we have a date cast as a string
                elif len(str(field_value)) == 10:
                    utc = pytz.timezone('UTC')
                    d = date(int(field_value[0:4]), int(field_value[5:7]), int(field_value[8:10]))
                    dt = datetime.combine(d, dtime(00, 00)) # when no time is supplied, default to 12:00:00 AM UTC
                    timestamp = utc.localize(dt)
                elif len(str(field_value)) == 19:
                    # input format: '2022-12-05 15:05:26'
                    year = int(field_value[0:4])
                    month = int(field_value[5:7])
                    day = int(field_value[8:10])
                    hour = int(field_value[11:13])
                    minute = int(field_value[14:16])
                    second = int(field_value[17:19])
                    dt = datetime(year, month, day, hour, minute, second)
                    timestamp = pytz.utc.localize(dt) 
                # we have a timestamp cast as a string
                else:
                    timestamp_value = field_value.isoformat()
                    field_value = timestamp_value[0:19] + timestamp_value[26:32] + "Z"
                    timestamp = Timestamp()
                    timestamp.FromJsonString(field_value[0])
                
                print('timestamp:', timestamp)
                datetime_field = datacatalog.TagField()
                datetime_field.timestamp_value = timestamp
                tag.fields[field_id] = datetime_field
                
        except Exception as e:
            error_exists = True
            print("Error storing values ", field_values, " into field ", field_id)
            
            if store != None:
                store.write_tag_value_error("Error storing value(s) " + str(field_values) + " into field " + field_id)
        
        return tag, error_exists
    
    
    def copy_tags(self, source_project, source_dataset, source_table, target_project, target_dataset, target_table, include_policy_tags=False):
        
        success = True
        
        # lookup the source entry
        linked_resource = '//bigquery.googleapis.com/projects/{0}/datasets/{1}/tables/{2}'.format(source_project, source_dataset, source_table)
        
        request = datacatalog.LookupEntryRequest()
        request.linked_resource = linked_resource
        source_entry = self.client.lookup_entry(request)
        
        if source_entry.bigquery_table_spec.table_source_type != types.TableSourceType.BIGQUERY_TABLE:
            success = False
            print('Error:', source_table, 'is not a BQ table.')
            return success
        
        # lookup the target entry
        linked_resource = '//bigquery.googleapis.com/projects/{0}/datasets/{1}/tables/{2}'.format(target_project, target_dataset, target_table)
        
        request = datacatalog.LookupEntryRequest()
        request.linked_resource = linked_resource
        target_entry = self.client.lookup_entry(request)
        
        if target_entry.bigquery_table_spec.table_source_type != types.TableSourceType.BIGQUERY_TABLE:
            success = False
            print('Error:', target_table, 'is not a BQ table.')
            return success
        
        
        # look to see if the source table is tagged
        tag_list = self.client.list_tags(parent=source_entry.name, timeout=120)
    
        for source_tag in tag_list:
            print('source_tag.template:', source_tag.template)
            print('source_tag.column:', source_tag.column)
            
            # get tag template fields
            self.template_id = source_tag.template.split('/')[5]
            self.template_project = source_tag.template.split('/')[1]
            self.template_region = source_tag.template.split('/')[3]
            self.template_path = source_tag.template
            template_fields = self.get_template()
            
            # start a new target tag
            target_tag = datacatalog.Tag()
            target_tag.template = source_tag.template
            
            if source_tag.column:
                target_tag.column = source_tag.column
            
            for template_field in template_fields:
    
                #print('template_field:', template_field)
                
                if template_field['field_id'] in source_tag.fields:
                    field_id = template_field['field_id']
                    tagged_field = source_tag.fields[field_id]
                    
                    print('field_id:', field_id)
                    
                    if tagged_field.bool_value:
                        field_type = 'bool'
                        field_value = tagged_field.bool_value
                    if tagged_field.double_value:
                        field_type = 'double'
                        field_value = tagged_field.double_value
                    if tagged_field.string_value:
                        field_type = 'string'
                        field_value = tagged_field.string_value
                    if tagged_field.enum_value:
                        field_type = 'enum'
                        field_value = tagged_field.enum_value.display_name
                    if tagged_field.timestamp_value:
                        field_type = 'timestamp'
                        field_value = tagged_field.timestamp_value
                    if tagged_field.richtext_value:
                        field_type = 'richtext'
                        field_value = tagged_field.richtext_value
                        
                    target_tag, error_exists = self.populate_tag_field(target_tag, field_id, field_type, [field_value])
            
            # create the target tag            
            tag_exists, tag_id = self.check_if_exists(parent=target_entry.name, column=source_tag.column)
		
            if tag_exists == True:
                target_tag.name = tag_id
            
                try:
                    print('tag update request: ', target_tag)
                    response = self.client.update_tag(tag=target_tag)
                except Exception as e:
                    success = False
                    print('Error occurred during tag update: ', e)
            
            else:
                try:
                    print('tag create request: ', target_tag)
                    response = self.client.create_tag(parent=target_entry.name, tag=target_tag)
                except Exception as e:
                    success = False
                    print('Error occurred during tag create: ', e)
                        
        # copy policy tags            
        success = self.copy_policy_tags(source_project, source_dataset, source_table, target_project, target_dataset, target_table)    
        
        return success

    
    def copy_policy_tags(self, source_project, source_dataset, source_table, target_project, target_dataset, target_table):
    
        success = True
    
        bq_client = bigquery.Client(location=BIGQUERY_REGION)

        source_table_id = source_project + '.' + source_dataset + '.' + source_table
        target_table_id = target_project + '.' + target_dataset + '.' + target_table
    
        try:
            source_schema = bq_client.get_table(source_table_id).schema
        except Exception as e:
            success = False
            print('Error occurred while retrieving the schema of ', source_table_id, '. Error message:', e)
            return success 
    
        policy_tag_list = []
    
        for field in source_schema:
            if field.policy_tags != None:
                policy_tag = field.policy_tags.names[0]
                pt_tuple = (field.name, policy_tag)
                policy_tag_list.append(pt_tuple)
	
        if len(policy_tag_list) == 0:
            return success
    
        print('policy_tag_list:', policy_tag_list)
        success = self.apply_policy_tags(target_table_id, policy_tag_list)
    
        return success
    
    # used to update the status of a data product tag as part of the product_registration_pipeline
    # https://github.com/GoogleCloudPlatform/datacatalog-tag-engine/tree/main/examples/product_registration_pipeline    
    def update_tag_subset(self, template_id, template_project, template_region, entry_name, changed_fields):
        
        success = True
        
        tag_list = self.client.list_tags(parent=entry_name, timeout=120)
    
        for tag in tag_list:
            print('tag.template:', tag.template)
            
            # get tag template fields
            tagged_template_id = tag.template.split('/')[5]
            tagged_template_project = tag.template.split('/')[1]
            tagged_template_region = tag.template.split('/')[3]
            
            if tagged_template_id != template_id:
                continue
            
            if tagged_template_project != template_project:
                continue
                
            if tagged_template_region != template_region:
                continue
                
            # start a new target tag to overwrite the existing one
            target_tag = datacatalog.Tag()
            target_tag.template = tag.template
            target_tag.name = tag.name
            
            self.template_path = tag.template
            template_fields = self.get_template()
            
            for template_field in template_fields:
    
                #print('template_field:', template_field)
                field_id = template_field['field_id']
                
                # skip this field if it's not in the tag
                if field_id not in tag.fields:
                    continue
                    
                tagged_field = tag.fields[field_id]
                    
                if tagged_field.bool_value:
                    field_type = 'bool'
                    field_value = str(tagged_field.bool_value)
                if tagged_field.double_value:
                    field_type = 'double'
                    field_value = str(tagged_field.double_value)
                if tagged_field.string_value:
                    field_type = 'string'
                    field_value = tagged_field.string_value
                if tagged_field.enum_value:
                    field_type = 'enum'
                    field_value = str(tagged_field.enum_value.display_name)
                if tagged_field.timestamp_value:
                    field_type = 'timestamp'
                    field_value = str(tagged_field.timestamp_value)
                    print('orig timestamp:', field_value)
                if tagged_field.richtext_value:
                    field_type = 'richtext'
                    field_value = str(tagged_field.richtext_value)
        		
                # overwrite logic
                for changed_field in changed_fields: 
                    if changed_field['field_id'] == field_id:
                        field_value = changed_field['field_value']
                        break
                
                target_tag, error_exists = self.populate_tag_field(target_tag, field_id, field_type, [field_value])
                
                if error_exists:
                    print('Error while populating the tag field. Aborting tag update.')
                    success = False
                    return success

            # update the tag
            try:
                print('tag update request: ', target_tag)
                response = self.client.update_tag(tag=target_tag)
            except Exception as e:
                success = False
                print('Error occurred during tag update: ', e)
 
        return success 
                        
        
if __name__ == '__main__':
    
    #template_id='data_quality'
    #template_project='tag-engine-develop'
    #template_region='us-central1'
    
    config_uuid = '7619fcaaa66f11ed83adc50169b5642d'
    target_project = 'sdw-data-gov-b1927e-dd69'
    target_dataset = 'tag_exports'
    target_region = 'us-central1'
    uri = 'sdw-conf-b1927e-bcc1/datasets/crm/tables/NewCust'
    
    dcu = DataCatalogUtils()
    dcu.apply_export_config(config_uuid, target_project, target_dataset, target_region, uri)