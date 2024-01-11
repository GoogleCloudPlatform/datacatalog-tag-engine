from google.cloud import bigquery
from google.cloud.exceptions import NotFound
    
def load_tables(request):    
    
    request_data = request.get_json()
    print('src_uri: {}'.format(request_data['src_uri']))
    print('dest_project: {}'.format(request_data['dest_project']))
    print('dest_dataset: {}'.format(request_data['dest_dataset']))
    print('table_prefix: {}'.format(request_data['table_prefix']))
    print('start_index: ' + str(request_data['start_index']))
    print('stop_index: ' + str(request_data['stop_index']))
    
    src_uri = request_data['src_uri']
    dest_project = request_data['dest_project']
    dest_dataset = request_data['dest_dataset']
    table_prefix = request_data['table_prefix']
    start_index = request_data['start_index']
    stop_index = request_data['stop_index']
    
    client = bigquery.client.Client()
    #bq.create_dataset(dest_project + '.' + dest_dataset, exists_ok=True)
        
    dest_dataset_ref = client.dataset(dest_dataset, project=dest_project)
    
    config = bigquery.LoadJobConfig(source_format=bigquery.SourceFormat.AVRO, write_disposition='WRITE_APPEND')
    
    for i in range(start_index, stop_index):
        
        dest_table = table_prefix + '_' + str(i)    
        
        try:
            client.get_table(dest_project + '.' + dest_dataset + '.' + dest_table) 
            print('table exists {}.'.format(dest_table))
        
        except NotFound:
            dest_table_ref = dest_dataset_ref.table(dest_table)
            
            load_job = client.load_table_from_uri(src_uri, dest_table_ref, job_config=config)
            load_job.result()  
        
            print('finished loading {}'.format(dest_table_ref.table_id))

    return "OK"
 


