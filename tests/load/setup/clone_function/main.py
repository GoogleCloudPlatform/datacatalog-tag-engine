from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import time
    
def make_copies(request):    
    
    request_data = request.get_json()
    print('src_table: {}'.format(request_data['src_table']))
    print('dest_project: {}'.format(request_data['dest_project']))
    print('dest_dataset: {}'.format(request_data['dest_dataset']))
    print('start_index: ' + str(request_data['start_index']))
    print('stop_index: ' + str(request_data['stop_index']))
    
    src_table = request_data['src_table']
    dest_project = request_data['dest_project']
    dest_dataset = request_data['dest_dataset']
    start_index = request_data['start_index']
    stop_index = request_data['stop_index']
    
    bq = bigquery.client.Client()
    #bq.create_dataset(dest_project + '.' + dest_dataset, exists_ok=True)
    
    src_table = bq.get_table(src_table)
    src_table_id = src_table.table_id
    
    dest_dataset_ref = bq.dataset(dest_dataset, project=dest_project)
    
    config = bigquery.job.CopyJobConfig()
    config.write_disposition = "WRITE_TRUNCATE"
    
    for i in range(start_index, stop_index):
        
        dest_table = src_table_id + '_' + str(i)
        
        #try:
            #bq.get_table(dest_project + '.' + dest_dataset + '.' + dest_table) 
            #print("Table {} already exists.".format(dest_table))
        
        #except NotFound:
        dest_table_ref = dest_dataset_ref.table(dest_table)

        job = bq.copy_table(src_table, dest_table_ref, location='us-central1', job_config=config)  
        job.result()
            
        print('created ' + dest_table_ref.table_id) 

    return "OK"
 


