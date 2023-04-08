import json
import time
import argparse
from datetime import datetime
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from google.cloud import tasks_v2
    
    
def load_tables(project, queue_region, queue_name, url, src_uri, dest_project, dest_dataset, table_prefix, num_copies, step):    
    
    for i in range(0, num_copies, step):
        
        start = i
        stop = start + step
        
        print('start: ' + str(start) + ', stop: ' + str(stop))
        create_task(project, queue_region, queue_name, url, src_uri, dest_project, dest_dataset, table_prefix, start, stop)
        
        #time.sleep(60)
        
def create_task(project, queue_region, queue_name, url, src_table, dest_project, dest_dataset, table_prefix, start, stop):
    
    print('*** enter create_task ***')

    client = tasks_v2.CloudTasksClient()
    parent = client.queue_path(project, queue_region, queue_name)
    
    task = {
        "http_request": { 
            "http_method": tasks_v2.HttpMethod.POST,
            "url": url, 
        }
    }
    
    task['http_request']['headers'] = {'Content-type': 'application/json'}
    payload = {'src_uri': src_uri, 'dest_project': dest_project, 'dest_dataset': dest_dataset, 'table_prefix': table_prefix, 'start_index': start, 'stop_index': stop}
    print('payload: ' + str(payload))
    
    payload_utf8 = json.dumps(payload).encode()
    task['http_request']['body'] = payload_utf8

    try:
        task = client.create_task(parent=parent, task=task)
        
        print('task: ' + task.name)
    
    except Exception as e:
        print('Error: could not create task ', e)
    

def get_table_count(dest_project, dest_dataset):
    
    bq = bigquery.Client()
    sql = 'select count(*) as count from ' + dest_project + '.' + dest_dataset + '.INFORMATION_SCHEMA.TABLES'
    query_job = bq.query(sql)  
    rows = query_job.result()
    
    for row in rows:
        count = row.count
    
    return count


def get_table_list(dest_project, dest_dataset):
    
    tables = set()
    
    bq = bigquery.Client()
    sql = 'select table_name from ' + dest_project + '.' + dest_dataset + '.INFORMATION_SCHEMA.TABLES'
    query_job = bq.query(sql)  
    rows = query_job.result()
    
    for row in rows:
        tables.add(row.table_name)
    
    return tables
    
        
def find_missing(dest_project, dest_dataset, table_prefix, num_copies):
    
    with open('logs/missing_' + dest_dataset + '.out', 'w') as log:
    
        log.write('started find in ' + dest_dataset + ' at ' + datetime.now().strftime('%m/%d/%Y, %H:%M:%S') + '\n')
        
        count = get_table_count(dest_project, dest_dataset)
        
        log.write(dest_dataset + ' has ' + str(count) + ' tables\n')
        
        if count == num_copies:
            log.write(dest_dataset + ' has the expected number of tables \n')
            return
        
        tables = get_table_list(dest_project, dest_dataset)
        
        for i in range(0, num_copies):
        
            dest_table = table_prefix + '_' + str(i)
        
            if dest_table not in tables:
                log.write(dest_table + ' is missing\n')

        log.write('finished find at ' + datetime.now().strftime('%m/%d/%Y, %H:%M:%S') + '\n')
    
    
if __name__ == '__main__':
    
    parser = argparse.ArgumentParser(description='runs load_tables_distributed.py')
    parser.add_argument('option', help='Choose load or find')
    args = parser.parse_args()
    
    project = 'warehouse-337221'
    queue_region = 'us-central1'
    queue_name = 'default'
    url = 'https://us-central1-warehouse-337221.cloudfunctions.net/load_tables_function' 
    src_uri = 'gs://austin_311/austin_311_service_requests_sample.avro'
    table_prefix = 'austin_311_service_requests'
    dest_project = 'warehouse-337221'
    dest_dataset = 'austin_311_500k'   # change this each run
    num_copies = 500000                # change this each run
    step = 200                       # keep this small because a function times-out after 9 minutes
    
    if args.option == 'load':
        load_tables(project, queue_region, queue_name, url, src_uri, dest_project, dest_dataset, table_prefix, num_copies, step) 
    
    if args.option == 'find':
        find_missing(dest_project, dest_dataset, table_prefix, num_copies)   


