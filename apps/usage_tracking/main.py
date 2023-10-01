# Copyright 2023 Google, LLC.
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

import json

from google.cloud import bigquery
from google.cloud import datacatalog

bq_client = bigquery.Client()
dc_client = datacatalog.DataCatalogClient()

reporting_table = 'entry_click_stream' # name of the output table

def event_handler(request):
    request_json = request.get_json()

    log_sync_project = request_json['calls'][0][0].strip()
    log_sync_dataset = request_json['calls'][0][1].strip()
    reporting_project = request_json['calls'][0][2].strip()
    reporting_dataset = request_json['calls'][0][3].strip() 
    start_date = request_json['calls'][0][4].strip() 
    
    try:
        report_table_create(reporting_project, reporting_dataset, reporting_table)
        status = main(log_sync_project, log_sync_dataset, reporting_project, reporting_dataset, start_date)
        return json.dumps({"replies": [status]})
    
    except Exception as e:
        print("Exception caught: " + str(e))
        return json.dumps({"errorMessage": str(e)}), 400 

    
def main(log_sync_project, log_sync_dataset, reporting_project, reporting_dataset, start_date:None):
    
    success = True
    rows_to_insert = []
    
    sql = "select distinct timestamp_trunc(timestamp, SECOND) as event_time, "
    sql += "resource.labels.project_id as project, " 
    sql += "protopayload_auditlog.authenticationInfo.principalEmail as user_email, "
    sql += "protopayload_auditlog.resourceName as dc_entry "
    sql += "from " + log_sync_project + "." + log_sync_dataset + "." + "cloudaudit_googleapis_com_activity "
    sql += "where protopayload_auditlog.methodName = 'google.cloud.datacatalog.v1.DataCatalog.TestUpdateTagPermission' "
    
    if start_date != None:
        sql += "and timestamp_trunc(timestamp, DAY) >= timestamp('" + start_date + "')"  
     
    print(sql)
    
    query_job = bq_client.query(sql)  
    results = query_job.result()
      
    for result in results:
        event_time = result.event_time
        project = result.project
        user_email = result.user_email
        dc_entry = result.dc_entry
        print('event_time:', event_time)
        
        # lookup BQ resource
        entry_request = datacatalog.GetEntryRequest(name=dc_entry)
        entry_response = dc_client.get_entry(request=entry_request)
        bq_resource = entry_response.linked_resource.replace('//bigquery.googleapis.com/', '')
        print('bq_resource:', bq_resource)
        
        event_time = event_time.strftime("%Y-%m-%d %H:%M:%S") + " UTC"
        print('event_time_formatted:', event_time)
        
        rows_to_insert.append({"event_time": event_time, "project": project, "user_email": user_email, "dc_entry": dc_entry, "bq_resource": bq_resource})
    
    if len(rows_to_insert) > 0:
        success = insert_records(reporting_project, reporting_dataset, reporting_table, rows_to_insert)    
      
    return success


def insert_records(reporting_project, reporting_dataset, reporting_table, rows_to_insert):    

    print('insert_records')
    
    success = True
    
    table_id = reporting_project + '.' + reporting_dataset + '.' + reporting_table  
    job_config = bigquery.LoadJobConfig(schema=report_table_schema(), source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON)  
    table_ref = bigquery.table.TableReference.from_string(table_id)

    try:
        job = bq_client.load_table_from_json(rows_to_insert, table_ref, job_config=job_config)
        print('Inserted record into reporting table')
        print('job errors:', job.errors)
    
    except Exception as e:
        
        print('Error while writing record into reporting table ', e)
        
        if '404' in str(e):
            print('Reporting table not ready to be written to. Sleeping for 5 seconds.')
            time.sleep(5)
            try:
                errors = bq_client.insert_rows_json(table_id, rows_to_insert)
            except Exception as e:
                 print("Error occurred during insert_records: {}".format(e))
                 success = False
    
    return success
    

def report_table_create(project, dataset, table):
    
    created = True
    
    table_id = project + '.' + dataset + '.' + table
    table_ref = bigquery.Table.from_string(table_id)

    try:
        table = bq_client.get_table(table_ref)
        created = False
        return created
          
    except NotFound:

        table = bigquery.Table(table_id, schema=report_table_schema())
        table.time_partitioning = bigquery.TimePartitioning(type_=bigquery.TimePartitioningType.DAY, field="event_time") 
        table = bq_client.create_table(table)
        print("Created table {}".format(table.table_id))  
    
    return created
    

def report_table_schema():
    
    schema = [
        bigquery.SchemaField("event_time", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("project", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("user_email", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("dc_entry", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("bq_resource", "STRING", mode="REQUIRED"),
    ]
    
    return schema


if __name__ == "__main__":
                         # log_sync_project, log_sync_dataset, reporting_project, reporting_dataset, start_date
    success = main('tag-engine-develop', 'audit_logs', 'tag-engine-develop', 'reporting', '2023-10-01') 
    print('success:', success)       