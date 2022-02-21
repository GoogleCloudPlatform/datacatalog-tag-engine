from google.cloud import bigquery
from google.cloud.exceptions import NotFound
    
def make_copies(src_table, dest_project, dest_dataset, num_copies):    
    
    bq = bigquery.client.Client()
    bq.create_dataset(dest_project + '.' + dest_dataset, exists_ok=True)
    
    src_table = bq.get_table(src_table)
    src_table_id = src_table.table_id
    
    dest_dataset_ref = bq.dataset(dest_dataset, project=dest_project)
    
    config = bigquery.job.CopyJobConfig()
    config.write_disposition = "WRITE_TRUNCATE"
    
    for i in range(0, num_copies):
        
        dest_table = src_table_id + '_' + str(i)
        
        #try:
            #bq.get_table(dest_project + '.' + dest_dataset + '.' + dest_table) 
            #print("Table {} already exists.".format(dest_table))
        
        #except NotFound:
        dest_table_ref = dest_dataset_ref.table(dest_table)
        print('attempting to create ' + dest_table_ref.table_id)
    
        job = bq.copy_table(src_table, dest_table_ref, location='us-central1', job_config=config)  
        job.result()
            
        print('finished creating ' + dest_table_ref.table_id)
            

if __name__ == '__main__':
    
    src_table = 'warehouse-337221.austin_311_source.austin_311_service_requests'
    dest_project = 'warehouse-337221'
    dest_dataset = 'austin_311_100k' 
    num_copies = 100000
    make_copies(src_table, dest_project, dest_dataset, num_copies)    


