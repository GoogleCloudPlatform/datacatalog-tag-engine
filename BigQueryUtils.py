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

import json, datetime, time
import decimal

from google.cloud import bigquery
from google.cloud.exceptions import NotFound

import TagEngineUtils as te

class BigQueryUtils:
    
    def __init__(self):
        
        self.client = bigquery.Client()

    def create_dataset(self, project_id, region, dataset):

        dataset_id = bigquery.Dataset(project_id + '.' + dataset)
        dataset_id.location = region
        dataset_status = self.client.create_dataset(dataset_id, exists_ok=True)  
        print("Created dataset {}".format(dataset_status.dataset_id))
        
    # used by tag history feature
    def table_exists(self, table_name):
        
        store = te.TagEngineUtils()
        enabled, settings = store.read_tag_history_settings()
        
        if enabled == False:
            return enabled, settings
        
        project_id = settings['project_id']
        region = settings['region']
        dataset = settings['dataset']
        
        dataset_id = self.client.dataset(dataset, project=project_id)
        table_id = dataset_id.table(table_name)
        
        try:
            self.client.get_table(table_id) 
            exists = True 
            #print("Tag history table {} already exists.".format(table_name))
        except NotFound:
            exists = False
            print("Tag history table {} not found.".format(table_name))
        
        return exists, table_id, settings
    
    # used by tag history feature
    def create_table(self, dataset_id, table_name, fields):
        
        schema = [bigquery.SchemaField('event_time', 'DATETIME', mode='REQUIRED'), \
                  bigquery.SchemaField('asset_name', 'STRING', mode='REQUIRED')]

        for field in fields:
            
            col_name = field['field_id']
            
            if field['field_type'] == 'string':
                col_type = 'STRING'
            
            if field['field_type'] == 'enum':
                col_type = 'STRING'
                
            if field['field_type'] == 'double':
                col_type = 'NUMERIC'
                
            if field['field_type'] == 'bool':
                col_type = 'BOOLEAN'
                
            if field['field_type'] == 'timestamp':
                col_type = 'TIMESTAMP'
                
            if field['field_type'] == 'datetime':
                col_type = 'TIMESTAMP' # datetime fields should be mapped to timestamps in BQ because they actually contain a timezone
            
            if field['is_required'] == True:
                mode = "REQUIRED"
            else:
                mode = "NULLABLE"
                
            schema.append(bigquery.SchemaField(col_name, col_type, mode=mode))
        
        table_id = dataset_id.table(table_name)
        table = bigquery.Table(table_id, schema=schema)
        table.time_partitioning = bigquery.TimePartitioning(type_=bigquery.TimePartitioningType.DAY, field="event_time")  
        table = self.client.create_table(table, exists_ok=True)  
        
        print("Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id))        
        table_id = ("{}.{}.{}".format(table.project, table.dataset_id, table.table_id))
        
        return table_id
    
    # used by tag history feature
    def insert_row(self, table_id, asset_name, tagged_values):
        
        row = {'event_time': datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%f'), 'asset_name': asset_name}
        
        for tagged_value in tagged_values:
            
            #print('tagged_value: ' + str(tagged_value))
            
            if 'field_value' not in tagged_value:
                continue
            
            if isinstance(tagged_value['field_value'], decimal.Decimal):
                row[tagged_value['field_id']] = float(tagged_value['field_value'])
            elif isinstance(tagged_value['field_value'], datetime.datetime) or isinstance(tagged_value['field_value'], datetime.date):
                row[tagged_value['field_id']] = tagged_value['field_value'].isoformat()
            else:
                row[tagged_value['field_id']]= json.dumps(tagged_value['field_value'], default=str)
                row[tagged_value['field_id']]= tagged_value['field_value']
    
        #print('insert row: ' + str(row))
        row_to_insert = [row,]

        try:
            self.client.insert_rows_json(table_id, row_to_insert)  
        except Exception as e:
            if 'NotFound: 404' in str(e):
                # table isn't quite ready to be written to
                time.sleep(3)
                errors = self.client.insert_rows_json(table_id, row_to_insert)  
            else:    
                print("Error while inserting row into BQ history table: {}", e)
        
    # used by tag history feature
    def copy_tag(self, table_name, table_fields, tagged_table, tagged_column, tagged_values):
        
        #print("*** inside BigQueryUtils.copy_tag() ***")
        #print("table_name: " + table_name)
        #print("table_fields: " + str(table_fields))
        #print("tagged_table: " + tagged_table)
        #print("tagged_column: " + tagged_column)
        #print("tagged_values: " + str(tagged_values))
        
        exists, table_id, settings = self.table_exists(table_name)
        
        if exists != True:
            dataset_id = self.client.dataset(settings['dataset'], project=settings['project_id'])
            table_id = self.create_table(dataset_id, table_name, table_fields)

        if tagged_column and tagged_column not in "":
            asset_name = ("{}/column/{}".format(tagged_table, tagged_column))
        else:
            asset_name = tagged_table
            
        asset_name = asset_name.replace("datasets", "dataset").replace("tables", "table")
        #print('asset_name: ', asset_name)
                
        self.insert_row(table_id, asset_name, tagged_values)  
        
        
        