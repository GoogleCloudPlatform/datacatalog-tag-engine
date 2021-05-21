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

import requests
from google.cloud import datacatalog
from google.cloud.datacatalog import DataCatalogClient
#from google.cloud.datacatalog_v1.types import Tag
from google.cloud import bigquery
import Resources as res
import TagEngineUtils as te
import BigQueryUtils as bq
import constants

class DataCatalogUtils:
    
    def __init__(self, template_id, project_id, region):
        self.template_id = template_id
        self.project_id = project_id
        self.region = region
        
        self.client = DataCatalogClient()
        self.template_path = DataCatalogClient.tag_template_path(project_id, region, template_id)
    
    def get_template(self):
        
        fields = []
        
        tag_template = self.client.get_tag_template(name=self.template_path)
        
        for field_id, field_value in tag_template.fields.items():
            
            field_id = str(field_id)
            display_name = field_value.display_name
            is_required = field_value.is_required
            
            #print("field_id: " + str(field_id))
            #print("field_value: " + str(field_value))
            #print("display_name: " + display_name)
            #print("primitive_type: " + str(FieldType.primitive_type))
            #print("is_required: " + str(is_required))
            
            enum_values = []
            
            field_type = None
            
            if field_value.type.primitive_type == datacatalog.FieldType.PrimitiveType.DOUBLE:
                field_type = "double"
            if field_value.type.primitive_type == datacatalog.FieldType.PrimitiveType.STRING:
                field_type = "string"
            if field_value.type.primitive_type == datacatalog.FieldType.PrimitiveType.BOOL:
                field_type = "bool"
            if field_value.type.primitive_type == datacatalog.FieldType.PrimitiveType.TIMESTAMP:
                field_type = "datetime"
            if field_value.type.primitive_type == datacatalog.FieldType.PrimitiveType.PRIMITIVE_TYPE_UNSPECIFIED:
                field_type = "enum"   
                     
                index = 0
                enum_values_long = str(field_value.type).split(":") 
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
            
            if field_type == "enum":
                field['enum_values'] = enum_values

            fields.append(field)
                          
        return fields
    
        
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
                print('tag_exists: ' + str(tag_exists))
            
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
            
                print("response: " + str(response))
        
        except ValueError:
            print("ValueError: create_static_tags failed due to invalid parameters.")
            creation_status = constants.ERROR
            
        return creation_status
         
    
    def create_update_dynamic_propagated_tag(self, config_status, source_res, view_res, columns, fields, source_tag_uuid, view_tag_uuid, template_uuid):
        
        store = te.TagEngineUtils()
        bq_client = bigquery.Client()        
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
    
                    # run query in BQ
                    qualified_table = view_res.replace('/project/', '.').replace('/datasets/', '.').replace('/tables/', '.')
                    query_str = query_expression.replace('$$', qualified_table)
                    #print('query_str: ' + query_str)
                    rows = bq_client.query(query_str).result()
    
                    for row in rows: 
                        #print("query result: " + str(row[0]))
                        field_value = row[0]
                    
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
                    print('tag exists')
                    tag.name = tag_id
                    response = self.client.update_tag(tag=tag)
                    store.write_propagated_log_entry(config_status, constants.TAG_UPDATED, constants.BQ_RES, source_res, view_res, column, "DYNAMIC", source_tag_uuid, view_tag_uuid, tag_id, template_uuid)
                else:
                    print('tag doesn''t exists')
                    response = self.client.create_tag(parent=entry.name, tag=tag)
                    tag_id = response.name
                    store.write_propagated_log_entry(config_status, constants.TAG_CREATED, constants.BQ_RES, source_res, view_res, column, "DYNAMIC", source_tag_uuid, view_tag_uuid, tag_id, template_uuid)
        
            #print("response: " + str(response))

        except ValueError:
            print("ValueError: create_dynamic_tags failed due to invalid parameters.")
            creation_status = constants.ERROR

        return creation_status
    
    
    def check_if_exists(self, parent, column):
        
        print('enter check_if_exists')
        print('input parent: ' + parent)
        print('input column: ' + column)
        
        tag_exists = False
        tag_id = ""
        
        tag_list = self.client.list_tags(parent=parent, timeout=10)
        
        for tag_instance in tag_list:
            print('tag_instance: ' + str(tag_instance))
            print('tag name: ' + str(tag_instance.name))
            tagged_column = tag_instance.column
            
            print('found tagged column: ' + tagged_column)
            
            tagged_template = tag_instance.template.split('/')[5]
             
            if column == "":
                # looking for table tags
                if tagged_template == self.template_id and tagged_column == "":
                    print('Table tag exists.')
                    tag_exists = True
                    tag_id = tag_instance.name
                    print('tag_id: ' + tag_id)
                    break
            else:
                # looking for column tags
                if column == tagged_column and tagged_template == self.template_id:
                    print('Column tag exists.')
                    tag_exists = True
                    tag_id = tag_instance.name
                    print('tag_id: ' + tag_id)
                    break
        
        print('tag_exists: ' + str(tag_exists))
        print('tag_id: ' + str(tag_id))
           
        return tag_exists, tag_id
    
    
    def create_update_static_tags(self, fields, included_uris, excluded_uris, tag_uuid, template_uuid, tag_export):
        
        store = te.TagEngineUtils()        
        rs = res.Resources(self.project_id)
        resources = rs.get_resources(included_uris, excluded_uris)
        print("resources: " + str(resources))
        
        creation_status = constants.SUCCESS
        
        for resource in resources:
            
            column = ""
            
            if "/column/" in resource:
                # we have a column tag
                split_resource = resource.split("/column/")
                resource = split_resource[0]
                column = split_resource[1]
            
            bigquery_resource = '//bigquery.googleapis.com/projects/' + resource
            print("bigquery_resource: " + bigquery_resource)
            
            request = datacatalog.LookupEntryRequest()
            request.linked_resource=bigquery_resource
            entry = self.client.lookup_entry(request)
            
            try:    
                
                tag_exists, tag_id = self.check_if_exists(parent=entry.name, column=column)
                
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
                        tag.fields[field_id].timestamp_value.FromJsonString(datetime_value)
                        
                if column != "":
                    tag.column = column
                    print('tag.column == ' + column)   
                
                if tag_exists == True:
                    tag.name = tag_id
                    response = self.client.update_tag(tag=tag)
                    store.write_log_entry(constants.TAG_UPDATED, constants.BQ_RES, resource, column, "STATIC", tag_uuid, tag_id, template_uuid)
                else:
                    response = self.client.create_tag(parent=entry.name, tag=tag)
                    tag_id = response.name
                    store.write_log_entry(constants.TAG_CREATED, constants.BQ_RES, resource, column, "STATIC", tag_uuid, tag_id, template_uuid)
                
                if tag_export == True:
                    bqu = bq.BigQueryUtils()
                    template_fields = self.get_template()
                    bqu.copy_tag(self.template_id, template_fields, resource, column, fields)
                
                print("response: " + str(response))
            
            except ValueError:
                print("ValueError: create_static_tags failed due to invalid parameters.")
                creation_status = constants.ERROR
            
        return creation_status

    def create_update_dynamic_tags(self, fields, included_uris, excluded_uris, tag_uuid, template_uuid, tag_export):
        
        store = te.TagEngineUtils()
        bq_client = bigquery.Client()
                
        rs = res.Resources(self.project_id)
        resources = rs.get_resources(included_uris, excluded_uris)
        
        creation_status = constants.SUCCESS

        for resource in resources:
            
            print('resource: ' + resource)
            
            column = ""
            if "/column/" in resource:
                # we have a column tag
                split_resource = resource.split("/column/")
                resource = split_resource[0]
                column = split_resource[1]
                
            bigquery_resource = '//bigquery.googleapis.com/projects/' + resource
            request = datacatalog.LookupEntryRequest()
            request.linked_resource=bigquery_resource
            entry = self.client.lookup_entry(request)

            try:    
                
                tag_exists, tag_id = self.check_if_exists(parent=entry.name, column=column)
                print("tag_exists: " + str(tag_exists))
                
                # create new tag
                tag = datacatalog.Tag()
                tag.template = self.template_path
                
                for field in fields:
                    field_id = field['field_id']
                    field_type = field['field_type']
                    query_expression = field['query_expression']
                    
                    print('field_id: ' + field_id + ', field_type: ' + field_type + ', query_exp: ' + query_expression)
                
                    # analyze query expression
                    from_index = query_expression.rfind(" from ", 0)
                    where_index = query_expression.rfind(" where ", 0)
                    table_index = query_expression.rfind("$table", 0)
                    column_index = query_expression.rfind("$column", 0)
                    
                    #print('from_index: ' + str(from_index))
                    #print('where_index: ' + str(where_index))
                    #print('table_index: ' + str(table_index))
                    #print('column_index: ' + str(column_index))
                    
                    # $table referenced in from clause, use fully qualified table
                    if table_index > from_index and (table_index < where_index or where_index == -1):
                         print('$table referenced in from clause')
                         qualified_table = resource.replace('/project/', '.').replace('/datasets/', '.').replace('/tables/', '.')
                         query_str = query_expression.replace('$table', qualified_table)
                         
                         if column_index != -1:
                             query_str = query_str.replace('$column', column)
                    
                    # table referenced in where clause, only use table name
                    if table_index > where_index and where_index != -1:
                        print('$table referenced in where clause')
                        table_index = resource.rfind('/') + 1
                        #print('table_index: ' + str(table_index))
                        table_name = resource[table_index:]
                        print('table_name: ' + table_name)
                        query_str = query_expression.replace('$table', table_name)
                        
                    # table not in query expression (e.g. select 'string')
                    if table_index == -1:
                        query_str = query_expression
                        
                    # run resulting query in BQ
                    print('query_str: ' + query_str)
                    rows = bq_client.query(query_str).result()
                    
                    # Note: if query expression is well-formed, there should only be a single row with a single field_value
                    # However, the user may also run a query that returns a list of rows. In that case, grab the top row 
                    row_count = 0
                    for row in rows:
                        
                        row_count = row_count + 1
                        field_value = row[0]
                        
                        if row_count > 1:
                            break
                    
                    print('field_value: ' + str(field_value))           
                                
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
                    
                        # timestamp value must be in this format: 2020-12-02T16:34:14Z
                        timestamp_value = field_value.isoformat()
                    
                        if len(timestamp_value) == 10:
                            field_value = timestamp_value + 'T12:00:00Z'
                        else:
                            field_value = timestamp_value[0:19] + timestamp_value[26:32] + "Z"
                    
                        tag.fields[field_id].timestamp_value.FromJsonString(field_value)
                        
                    # store the value back in the dict, so that it can be accessed by the exporter
                    field['field_value'] = field_value
                
                if column != "":
                    tag.column = column
                    print('tag.column: ' + column) 
                
                if tag_exists == True:
                    print('updating tag')
                    tag.name = tag_id
                    response = self.client.update_tag(tag=tag)
                    store.write_log_entry(constants.TAG_UPDATED, constants.BQ_RES, resource, column, "DYNAMIC", tag_uuid, tag_id, template_uuid)
                else:
                    print('creating tag')
                    response = self.client.create_tag(parent=entry.name, tag=tag)
                    tag_id = response.name
                    store.write_log_entry(constants.TAG_CREATED, constants.BQ_RES, resource, column, "DYNAMIC", tag_uuid, tag_id, template_uuid)
                
                if tag_export == True:
                    bqu = bq.BigQueryUtils()
                    template_fields = self.get_template()
                    bqu.copy_tag(self.template_id, template_fields, resource, column, fields)
                    
                print("response: " + str(response))
            
            except ValueError:
                print("ValueError: create_dynamic_tags failed due to invalid parameters.")
                creation_status = constants.ERROR
            
        return creation_status

if __name__ == '__main__':
    dcu = DataCatalogUtils(template_id='dg_template', project_id='tag-engine-283315', region='us');
    fields = dcu.get_template()
    print(str(fields))