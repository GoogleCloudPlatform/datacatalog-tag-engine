import json
import time
from google.cloud import bigquery
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
        

if __name__ == '__main__':
    
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
    load_tables(project, queue_region, queue_name, url, src_uri, dest_project, dest_dataset, table_prefix, num_copies, step)    


