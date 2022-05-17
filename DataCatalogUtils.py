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
from datetime import datetime, date, time
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

class DataCatalogUtils:
    
    def __init__(self, template_id=None, project_id=None, region=None):
        self.template_id = template_id
        self.project_id = project_id
        self.region = region
        
        self.client = DataCatalogClient()
        
        if template_id is not None and project_id is not None and region is not None:
            self.template_path = DataCatalogClient.tag_template_path(project_id, region, template_id)
    
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
    
        
    def check_if_exists(self, parent, column):
        
        #print('enter check_if_exists')
        #print('input parent: ' + parent)
        #print('input column: ' + column)
        
        tag_exists = False
        tag_id = ""
        
        tag_list = self.client.list_tags(parent=parent, timeout=120)
        
        for tag_instance in tag_list:
            #print('tag_instance: ' + str(tag_instance))
            #print('tag name: ' + str(tag_instance.name))
            tagged_column = tag_instance.column
            
            #print('found tagged column: ' + tagged_column)
            
            tagged_template = tag_instance.template.split('/')[5]
             
            if column == "":
                # looking for table tags
                if tagged_template == self.template_id and tagged_column == "":
                    #print('Table tag exists.')
                    tag_exists = True
                    tag_id = tag_instance.name
                    #print('tag_id: ' + tag_id)
                    break
            else:
                # looking for column tags
                if column == tagged_column and tagged_template == self.template_id:
                    #print('Column tag exists.')
                    tag_exists = True
                    tag_id = tag_instance.name
                    #print('tag_id: ' + tag_id)
                    break
        
        #print('tag_exists: ' + str(tag_exists))
        #print('tag_id: ' + str(tag_id))
           
        return tag_exists, tag_id
    
    
    def create_update_static_config(self, fields, uri, tag_uuid, template_uuid, tag_history, tag_stream, overwrite=False):
        
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
            gcs_resource = '//datacatalog.googleapis.com/projects/' + self.project_id + '/locations/' + self.region + '/entryGroups/' + bucket + '/entries/' + filename
            #print('gcs_resource: ', gcs_resource)
            request = datacatalog.LookupEntryRequest()
            request.linked_resource=gcs_resource
            
            try:
                entry = self.client.lookup_entry(request)
            except Exception as e:
                print('Unable to find entry in the catalog. Entry ' + gcs_resource + ' does not exist.')
                creation_status = constants.ERROR
                return creation_status
                #print('entry found: ', entry)
        
        if isinstance(uri, str):
            is_bq = True
            if "/column/" in uri:
                # we have a column tag
                split_resource = uri.split("/column/")
                uri = split_resource[0]
                column = split_resource[1]
        
            bigquery_resource = '//bigquery.googleapis.com/projects/' + uri
            #print("bigquery_resource: " + bigquery_resource)
        
            request = datacatalog.LookupEntryRequest()
            request.linked_resource=bigquery_resource
            entry = self.client.lookup_entry(request)
        
        try:    
            tag_exists, tag_id = self.check_if_exists(parent=entry.name, column=column)
        
        except Exception as e:
            print('Error during check_if_exists: ', e)
            creation_status = constants.ERROR
            return creation_status

        if tag_exists and overwrite == False:
            #print('Tag already exists and overwrite set to False')
            creation_status = constants.SUCCESS
            return creation_status
            
        tag = datacatalog.Tag()
        tag.template = self.template_path
        
        for field in fields:
            field_id = field['field_id']
            field_type = field['field_type']
            field_value = field['field_value']

            if field_type == "bool":
                bool_field = datacatalog.TagField()
                
                if isinstance(field_value, str):
                    if field_value.lower() == 'true':
                        bool_field.bool_value = True
                    else:
                        bool_field.bool_value = False
                else:
                    bool_field.bool_value = field_value
                
                tag.fields[field_id] = bool_field

            if field_type == "string":
                string_field = datacatalog.TagField()
                string_field.string_value = str(field_value)
                tag.fields[field_id] = string_field
            if field_type == "double":
                float_field = datacatalog.TagField()
                float_field.double_value = float(field_value)
                tag.fields[field_id] = float_field
            if field_type == "enum":
                enum_field = datacatalog.TagField()
                enum_field.enum_value.display_name = field_value
                tag.fields[field_id] = enum_field
            if field_type == "datetime": 
                # field_value could be a date value e.g. "2022-05-08" or a datetime value e.g. "2022-05-08 15:00:00"
                utc = pytz.timezone('UTC')
                
                if len(field_value) == 10:
                    d = date(int(field_value[0:4]), int(field_value[5:7]), int(field_value[8:10]))
                    dt = datetime.combine(d, time(00, 00)) # when no time is supplied, default to 12:00:00 AM UTC  
                else:
                    # raw timestamp format: 2022-05-11 21:18:20
                    d = date(int(field_value[0:4]), int(field_value[5:7]), int(field_value[8:10]))
                    t = time(int(field_value[11:13]), int(field_value[14:16]))
                    dt = datetime.combine(d, t)
                    
                timestamp = utc.localize(dt)
                print('timestamp: ', timestamp)    
                datetime_field = datacatalog.TagField()
                datetime_field.timestamp_value = timestamp
                tag.fields[field_id] = datetime_field
                field['field_value'] = timestamp # store this value back in the field, so it can be exported
                
        if column != "":
            tag.column = column
            #print('tag.column == ' + column)   
                
        if tag_exists == True:
            tag.name = tag_id
            
            try:
                response = self.client.update_tag(tag=tag)
            except Exception as e:
                msg = 'Error occurred during tag update: ' + str(e)
                store.write_tag_op_error(constants.TAG_UPDATED, uri, column, tag_uuid, template_uuid, msg)
                
                # sleep and retry the tag update
                if 'Quota exceeded for quota metric' or '503 The service is currently unavailable' in str(e):
                    print('sleep for 3 minutes due to ' + str(e))
                    time.sleep(180)
                    
                    try:
                        response = self.client.update_tag(tag=tag)
                    except Exception as e:
                        msg = 'Error occurred during tag update after sleep: ' + str(e)
                        store.write_tag_op_error(constants.TAG_UPDATED, uri, column, tag_uuid, template_uuid, msg)
        else:
            try:
                response = self.client.create_tag(parent=entry.name, tag=tag)
            except Exception as e:
                msg = 'Error occurred during tag create: ' + str(e) + '. Failed tag request = ' + tag
                store.write_tag_op_error(constants.TAG_CREATED, uri, column, tag_uuid, template_uuid, msg)
                
                # sleep and retry write
                if 'Quota exceeded for quota metric' or '503 The service is currently unavailable' in str(e):
                    print('sleep for 3 minutes due to ' + str(e))
                    time.sleep(180)
                    
                    try:
                        response = self.client.create_tag(parent=entry.name, tag=tag)
                    except Exception as e:
                        msg = 'Error occurred during tag create after sleep: ' + str(e)
                        store.write_tag_op_error(constants.TAG_UPDATED, uri, column, tag_uuid, template_uuid, msg)
                    
        if tag_history:
            bqu = bq.BigQueryUtils()
            template_fields = self.get_template()
            if is_gcs:
                bqu.copy_tag(self.template_id, template_fields, '/'.join(uri), None, fields)
            if is_bq:
                bqu.copy_tag(self.template_id, template_fields, uri, column, fields)
        
        if tag_stream:
            psu = ps.PubSubUtils()
            if is_gcs:
                bqu.copy_tag(self.template_id, '/'.join(uri), None, fields)
            if is_bq:
                psu.copy_tag(self.template_id, uri, column, fields)
           
        return creation_status


    def create_update_dynamic_config(self, fields, uri, tag_uuid, template_uuid, tag_history, tag_stream, batch_mode=False):
        
        store = te.TagEngineUtils()
        bq_client = bigquery.Client()
        
        creation_status = constants.SUCCESS
  
        #print('uri: ' + uri)
        
        error_exists = False
        
        column = ""
        if "/column/" in uri:
            # we have a column tag
            split_resource = uri.split("/column/")
            uri = split_resource[0]
            column = split_resource[1]
            
        bigquery_resource = '//bigquery.googleapis.com/projects/' + uri
        request = datacatalog.LookupEntryRequest()
        request.linked_resource=bigquery_resource
        entry = self.client.lookup_entry(request)

        tag_exists, tag_id = self.check_if_exists(parent=entry.name, column=column)
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
            #print('query_str: ' + query_str)
            
            field_value, error_exists = self.run_query(bq_client, query_str, batch_mode, store)
    
            if error_exists:
                continue
    
            tag, error_exists = self.populate_tag_field(tag, field_id, field_type, field_value, store)
    
            if error_exists:
                continue
                                    
            verified_field_count = verified_field_count + 1
            #print('verified_field_count: ' + str(verified_field_count))    
            
            # store the value back in the dict, so that it can be accessed by the exporter
            #print('field_value: ' + str(field_value))
            field['field_value'] = field_value
            
        # for loop ends here
                
        if error_exists:
            # error was encountered while running SQL expression
            # proceed with tag creation / update, but return error to user
            creation_status = constants.ERROR
            
        if verified_field_count == 0:
            # tag is empty due to errors, skip tag creation
            return constants.ERROR
                        
        if column != "":
            tag.column = column
            #print('tag.column: ' + column) 
        
        if tag_exists == True:
            #print('updating tag')
            #print('tag request: ' + str(tag))
            tag.name = tag_id
            
            try:
                #print('tag request: ', tag)
                response = self.client.update_tag(tag=tag)
            except Exception as e:
                print('Error occurred during tag update: ', e)
                store.write_tag_op_error(constants.TAG_UPDATED, uri, column, tag_uuid, template_uuid, str(e))
            
        else:
            print('creating tag')
            
            try:
                response = self.client.create_tag(parent=entry.name, tag=tag)
                #print('response: ', response)
                
            except Exception as e:
                print('Error occurred during tag create: ', e)
                store.write_tag_op_error(constants.TAG_CREATED, uri, column, tag_uuid, template_uuid, str(e))
            
        if tag_history:
            bqu = bq.BigQueryUtils()
            template_fields = self.get_template()
            bqu.copy_tag(self.template_id, template_fields, uri, column, fields)
            
        if tag_stream:
            psu = ps.PubSubUtils()
            psu.copy_tag(self.template_id, uri, column, fields)
                
                                 
        return creation_status
        

    def create_update_entry_config(self, fields, uri, tag_uuid, template_uuid, tag_history, tag_stream):
        
        print('** create_update_entry_config **')
        
        creation_status = constants.SUCCESS
        store = te.TagEngineUtils()
        gcs_client = storage.Client()
        
        bucket_name, filename = uri
        bucket = gcs_client.get_bucket(bucket_name)
        blob = bucket.get_blob(filename)
        
        entry_group_short_name = bucket_name.replace('-', '_')
        entry_group_full_name = 'projects/' + self.project_id + '/locations/' + self.region + '/entryGroups/' + bucket_name.replace('-', '_')
        
        # create the entry group    
        is_entry_group = self.entry_group_exists(entry_group_full_name)
        print('is_entry_group: ', is_entry_group)
        
        if is_entry_group != True:
            self.create_entry_group(entry_group_short_name)
        
        # generate the entry id, replace '/' with '_' and remove the file extension from the name
        entry_id = filename.split('.')[0].replace('/', '_')
         
        try:
            entry_name = entry_group_full_name + '/entries/' + entry_id
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
            template_path = self.client.tag_template_path(self.project_id, self.region, self.template_id)
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
                    parent='projects/' + self.project_id + '/locations/' + self.region,
                    entry_group_id=entry_group_short_name,
                    entry_group=eg)
        
        print('created entry_group: ', entry_group.name)
        return entry_group.name
           

    def create_update_mapping_config(self, fields, mapping_table, uri, tag_uuid, template_uuid, tag_history, tag_stream, overwrite=False):
        
        print('** enter create_update_mapping_config **')
 
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
            filename = uri[1].split('.')[0].replace('/', '_') # extract the filename without the extension, replace '/' with '_'
            gcs_resource = '//datacatalog.googleapis.com/projects/' + self.project_id + '/locations/' + self.region + '/entryGroups/' + bucket + '/entries/' + filename
            #print('gcs_resource: ', gcs_resource)
            request = datacatalog.LookupEntryRequest()
            request.linked_resource=gcs_resource
            
            try:
                entry = self.client.lookup_entry(request)
            except Exception as e:
                print('Unable to find entry in the catalog. Entry ' + gcs_resource + ' does not exist.')
                creation_status = constants.ERROR
                return creation_status
                #print('entry found: ', entry)
        
        if isinstance(uri, str):
            is_bq = True
            if "/column/" in uri:
                # we have a column tag
                split_resource = uri.split("/column/")
                uri = split_resource[0]
                column = split_resource[1]
        
            bigquery_resource = '//bigquery.googleapis.com/projects/' + uri
            #print("bigquery_resource: " + bigquery_resource)
        
            request = datacatalog.LookupEntryRequest()
            request.linked_resource=bigquery_resource
            entry = self.client.lookup_entry(request)
        
        try:    
            tag_exists, tag_id = self.check_if_exists(parent=entry.name, column=column)
        
        except Exception as e:
            print('Error during check_if_exists: ', e)
            creation_status = constants.ERROR
            return creation_status

        if tag_exists and overwrite == False:
            #print('Info: tag already exists and overwrite set to False')
            creation_status = constants.SUCCESS
            return creation_status
         
        # entry exists while mapping tag does not
        if entry.schema == None:
            #print('Error: entry ' + entry.name + ' does not have a schema in the catalog.')
            creation_status = constants.ERROR
            return creation_status
        
        column_schema_str = ''
        for column_schema in entry.schema.columns: 
            column_schema_str += str(column_schema).split(':')[3].strip() + ','
                
        mapping_table_formatted = mapping_table.replace('bigquery/project/', '').replace('/dataset/', '.').replace('/', '.')
                
        query_str = 'select cannonical_name from `' + mapping_table_formatted + '` where source_name in (' + column_schema_str[0:-1] + ')'
        #print('query_str: ', query_str)
        
        # run query against mapping table
        bq_client = bigquery.Client()
        rows = bq_client.query(query_str).result()
        
        tag = datacatalog.Tag()
        tag.template = self.template_path
        
        for row in rows:
            cannonical_name = row['cannonical_name']
            #print('cannonical_name: ', cannonical_name)
        
            for field in fields:
                if field['field_id'] == cannonical_name:
                    bool_field = datacatalog.TagField()
                    bool_field.bool_value = True
                    tag.fields[cannonical_name] = bool_field
                    field['field_value'] = True
                    break
                        
        if column != "":
            tag.column = column
            #print('tag.column == ' + column)   
                
        if tag_exists:
            # tag already exists and overwrite is True
            tag.name = tag_id
            
            try:
                print('tag update: ', tag)
                response = self.client.update_tag(tag=tag)
            except Exception as e:
                msg = 'Error occurred during tag update: ' + str(e)
                store.write_tag_op_error(constants.TAG_UPDATED, uri, column, tag_uuid, template_uuid, msg)
                
                # sleep and retry the tag update
                if 'Quota exceeded for quota metric' or '503 The service is currently unavailable' in str(e):
                    print('sleep for 3 minutes due to ' + str(e))
                    time.sleep(180)
                    
                    try:
                        response = self.client.update_tag(tag=tag)
                    except Exception as e:
                        msg = 'Error occurred during tag update after sleep: ' + str(e)
                        store.write_tag_op_error(constants.TAG_UPDATED, uri, column, tag_uuid, template_uuid, msg)
        else:
            try:
                response = self.client.create_tag(parent=entry.name, tag=tag)
            except Exception as e:
                msg = 'Error occurred during tag create: ' + str(e) + '. Failed tag request = ' + tag
                store.write_tag_op_error(constants.TAG_CREATED, uri, column, tag_uuid, template_uuid, msg)
                
                # sleep and retry write
                if 'Quota exceeded for quota metric' or '503 The service is currently unavailable' in str(e):
                    print('sleep for 3 minutes due to ' + str(e))
                    time.sleep(180)
                    
                    try:
                        response = self.client.create_tag(parent=entry.name, tag=tag)
                    except Exception as e:
                        msg = 'Error occurred during tag create after sleep: ' + str(e)
                        store.write_tag_op_error(constants.TAG_UPDATED, uri, column, tag_uuid, template_uuid, msg)
                    
        if tag_history:
            bqu = bq.BigQueryUtils()
            template_fields = self.get_template()
            if is_gcs:
                bqu.copy_tag(self.template_id, template_fields, '/'.join(uri), None, fields)
            if is_bq:
                bqu.copy_tag(self.template_id, template_fields, uri, column, fields)
        
        if tag_stream:
            psu = ps.PubSubUtils()
            if is_gcs:
                bqu.copy_tag(self.template_id, '/'.join(uri), None, fields)
            if is_bq:
                psu.copy_tag(self.template_id, uri, column, fields)
           
        return creation_status
                 
        
    def search_catalog(self, bq_project, bq_dataset):
        
        linked_resources = {}
        
        scope = datacatalog.SearchCatalogRequest.Scope()
        scope.include_project_ids.append(bq_project)
        
        request = datacatalog.SearchCatalogRequest()
        request.scope = scope
    
        query = 'parent:' + bq_project + '.' + bq_dataset
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


################### Propagation Methods ##########################

    def create_update_static_propagated_tag(self, config_status, source_res, view_res, columns, fields, source_tag_uuid, view_tag_uuid, template_uuid):
        
        store = te.TagEngineUtils()        
        bigquery_resource = '//bigquery.googleapis.com/projects/' + view_res
        print("bigquery_resource: " + bigquery_resource)
        
        request = datacatalog.LookupEntryRequest()
        request.linked_resource=bigquery_resource
        entry = self.client.lookup_entry(request)
       
        creation_status = constants.SUCCESS
            
        try:    
            
            if len(columns) == 0:
                columns.append("")
            
            for column in columns:
            
                tag_exists, tag_id = self.check_if_exists(parent=entry.name, column=column)
                #print('tag_exists: ' + str(tag_exists))
            
                tag = datacatalog.Tag()
                tag.template = self.template_path
            
                for field in fields:
                    field_id = field['field_id']
                    field_type = field['field_type']
                    field_value = field['field_value']
            
                    if field_type == "bool":
                        bool_field = datacatalog.TagField()
                        bool_field.bool_value = bool(field_value)
                        tag.fields[field_id] = bool_field    
                    if field_type == "string":
                        string_field = datacatalog.TagField()
                        string_field.string_value = str(field_value)
                        tag.fields[field_id] = string_field
                    if field_type == "double":
                        float_field = datacatalog.TagField()
                        float_field.double_value = float(field_value)
                        tag.fields[field_id] = float_field
                    if field_type == "enum":
                        enum_field = datacatalog.TagField()
                        enum_field.enum_value.display_name = field_value
                        tag.fields[field_id] = enum_field
                    if field_type == "datetime":
                        datetime_field = datacatalog.TagField()
                        split_datetime = field_value.split(" ")
                        datetime_value = split_datetime[0] + "T" + split_datetime[1] + "Z"
                        print("datetime_value: " + datetime_value)
                        tag.fields[field_id].timestamp_value.FromJsonString(datetime_value)
                            
                if column != "":
                    tag.column = column
                    print('tag.column == ' + column)   
            
                if tag_exists == True:
                    tag.name = tag_id
                    response = self.client.update_tag(tag=tag)
                    store.write_propagated_log_entry(config_status, constants.TAG_UPDATED, constants.BQ_RES, source_res, view_res, column, "STATIC", source_tag_uuid, view_tag_uuid, tag_id, template_uuid)
                else:
                    response = self.client.create_tag(parent=entry.name, tag=tag)
                    tag_id = response.name
                    store.write_propagated_log_entry(config_status, constants.TAG_CREATED, constants.BQ_RES, source_res, view_res, column, "STATIC", source_tag_uuid, view_tag_uuid, tag_id, template_uuid)
            
                #print("response: " + str(response))
        
        except ValueError:
            print("ValueError: create_static_tags failed due to invalid parameters.")
            creation_status = constants.ERROR
            
        return creation_status
     
         
    def parse_query_expression(self, uri, query_expression):
        
        #print("*** enter parse_query_expression ***")
        #print("uri: " + uri)
        #print("query_expression: " + query_expression)
        
        query_str = None
        
        # analyze query expression
        from_index = query_expression.rfind(" from ", 0)
        where_index = query_expression.rfind(" where ", 0)
        project_index = query_expression.rfind("$project", 0)
        dataset_index = query_expression.rfind("$dataset", 0)
        table_index = query_expression.rfind("$table", 0)
        from_clause_table_index = query_expression.rfind(" from $table", 0)
        column_index = query_expression.rfind("$column", 0)
        
        if project_index != -1:
            project_end = uri.find('/') 
            project = uri[0:project_end]
            print('project: ' + project)
            
        if dataset_index != -1:
            dataset_start = uri.find('/datasets/') + 10
            dataset_string = uri[dataset_start:]
            dataset_end = dataset_string.find('/') 
            dataset = dataset_string[0:dataset_end]
            print('dataset: ' + dataset)
        
        # $table referenced in from clause, use fully qualified table
        if from_clause_table_index != -1:
             #print('$table referenced in from clause')
             qualified_table = uri.replace('/project/', '.').replace('/datasets/', '.').replace('/tables/', '.')
             #print('qualified_table: ' + qualified_table)
             query_str = query_expression.replace('$table', qualified_table)
             
        # $table is referenced somewhere in the expression, replace $table with actual table name
        if from_clause_table_index == -1 and table_index != -1:
            #print('$table referenced somewhere, but not in the from clause')
            table_index = uri.rfind('/') + 1
            table_name = uri[table_index:]
            #print('table_name: ' + table_name)
            query_str = query_expression.replace('$table', table_name)
            
            # $project referenced in where clause too
            if project_index > -1:
                query_str = query_str.replace('$project', project)
            
            # $dataset referenced in where clause too    
            if dataset_index > -1:
                query_str = query_str.replace('$dataset', dataset)
            
        # table not in query expression (e.g. select 'string')
        if table_index == -1:
            query_str = query_expression
            
        if column_index != -1:
            query_str = query_str.replace('$column', column)
            
        return query_str
    
    def run_query(self, bq_client, query_str, batch_mode, store):
        
        field_value = None
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
                rows = bq_client.query(query_str).result()
            
            # if query expression is well-formed, there should only be a single row returned with a single field_value
            # However, user may mistakenly run a query that returns a list of rows. In that case, grab only the top row.  
            row_count = 0
            for row in rows:
                row_count = row_count + 1
                field_value = row[0]
            
                if row_count > 1:
                    break
        
            # check row_count
            if row_count == 0:
                # SQL query returned nothing, log error in Firestore
                error_exists = True
                print('query_str returned nothing, writing error entry')
                store.write_tag_value_error('sql returned nothing: ' + query_str)
        
        except Exception as e:
            error_exists = True
            store.write_tag_value_error('invalid query parameter(s): ' + query_str + ' produced error ' + str(e))
        
        #print('field_value: ' + str(field_value))
        
        return field_value, error_exists
        

    def populate_tag_field(self, tag, field_id, field_type, field_value, store):
        
        error_exists = False
        
        try:             
            if field_type == "bool":
                bool_field = datacatalog.TagField()
                bool_field.bool_value = bool(field_value)
                tag.fields[field_id] = bool_field
            if field_type == "string":
                string_field = datacatalog.TagField()
                string_field.string_value = str(field_value)
                tag.fields[field_id] = string_field
            if field_type == "double":
                float_field = datacatalog.TagField()
                float_field.double_value = float(field_value)
                tag.fields[field_id] = float_field
            if field_type == "enum":
                enum_field = datacatalog.TagField()
                enum_field.enum_value.display_name = field_value
                tag.fields[field_id] = enum_field
            if field_type == "datetime":
                # timestamp value gets stored in DC, expected format: 2020-12-02T16:34:14Z
                # however, field_value can be a date value e.g. "2022-05-08" or a datetime value e.g. "2022-05-08 15:00:00"
                if len(str(field_value)) == 10:
                    utc = pytz.timezone('UTC')
                    d = date(int(field_value[0:4]), int(field_value[5:7]), int(field_value[8:10]))
                    dt = datetime.combine(d, time(00, 00)) # when no time is supplied, default to 12:00:00 AM UTC
                    timestamp = utc.localize(dt)
                else:
                    timestamp_value = field_value.isoformat()
                    field_value = timestamp_value[0:19] + timestamp_value[26:32] + "Z"
                    timestamp = Timestamp()
                    timestamp.FromJsonString(field_value)
                
                datetime_field = datacatalog.TagField()
                datetime_field.timestamp_value = timestamp
                tag.fields[field_id] = datetime_field
                
        except ValueError:
            error_exists = True
            print("cast error, writing error entry")
            store.write_tag_value_error('cast error in sql query: ' + query_str)
        
        return tag, error_exists
    
    def create_update_dynamic_propagated_tag(self, config_status, source_res, view_res, columns, fields, source_tag_uuid, view_tag_uuid,\
                                             template_uuid, batch_mode=False):
        
        #print('*** enter create_update_dynamic_propagated_tag ***')
        
        store = te.TagEngineUtils()
        bq_client = bigquery.Client() 
        view_res = view_res.replace('/views/', '/tables/')       
        bigquery_resource = '//bigquery.googleapis.com/projects/' + view_res
        
        request = datacatalog.LookupEntryRequest()
        request.linked_resource=bigquery_resource
        entry = self.client.lookup_entry(request)
         
        creation_status = constants.SUCCESS
        
        try:    
                
            if len(columns) == 0:
                columns.append("")
            
            for column in columns:
            
                tag_exists, tag_id = self.check_if_exists(parent=entry.name, column=column)
                print('tag_exists == ' + str(tag_exists))
    
                tag = datacatalog.Tag()
                tag.template = self.template_path
    
                for field in fields:
                    field_id = field['field_id']
                    field_type = field['field_type']
                    query_expression = field['query_expression']
    
                    # parse and run query in BQ
                    query_str = self.parse_query_expression(view_res, query_expression)
                    field_value, error_exists = self.run_query(bq_client, query_str, batch_mode, store)
                    
                    if error_exists:
                        continue
                    
                    tag, error_exists = self.populate_tag_field(tag, field_id, field_type, field_value, store)
                    
                    if error_exists:
                        continue
    
                if column != "":
                    tag.column = column
                    print('tag.column == ' + column)             
    
                if tag_exists == True:
                    print('tag exists')
                    tag.name = tag_id
                    response = self.client.update_tag(tag=tag)
                    store.write_propagated_log_entry(config_status, constants.TAG_UPDATED, constants.BQ_RES, source_res, view_res, column, "DYNAMIC",\
                                                    source_tag_uuid, view_tag_uuid, tag_id, template_uuid)
                else:
                    print('tag doesn''t exists')
                    response = self.client.create_tag(parent=entry.name, tag=tag)
                    tag_id = response.name
                    store.write_propagated_log_entry(config_status, constants.TAG_CREATED, constants.BQ_RES, source_res, view_res, column, "DYNAMIC",\
                                                    source_tag_uuid, view_tag_uuid, tag_id, template_uuid)
        
            #print("response: " + str(response))

        except ValueError:
            print("ValueError: create_update_propagated_dynamic_tags failed due to invalid parameters.")
            creation_status = constants.ERROR

        return creation_status
    

if __name__ == '__main__':
    
    config = configparser.ConfigParser()
    config.read("tagengine.ini")
    project_id=config['DEFAULT']['TAG_ENGINE_PROJECT']
    region=config['DEFAULT']['QUEUE_REGION']

    dcu = DataCatalogUtils(template_id='enterprise_dictionary_template', project_id=project_id, region=region);
    fields = dcu.get_template()
    mapping_table = 'bigquery/project/tag-engine-develop/dataset/dictionary/mapping'
    #print(str(fields))
    
    #uri = ['discovery-area', 'cities_311/san_francisco_311_service_requests/000000000008']
    #uri = ['discovery-area', 'austin_311_service_requests.parquet']
    #dcu.create_update_entry_config(fields, uri, tag_uuid=None, template_uuid=None, tag_history=False, tag_stream=False, batch_mode=False)
    
    uri = ['discovery-area', 'sample_data/farm.parquet']
    #uri = ['discovery-area', 'sample_data/usa.parquet']
    dcu.create_update_mapping_config(fields, mapping_table, uri, tag_uuid=None, template_uuid=None, tag_history=False, tag_stream=False, overwrite=False)