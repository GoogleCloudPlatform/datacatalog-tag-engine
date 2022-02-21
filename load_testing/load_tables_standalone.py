from google.cloud import bigquery
    
def load_tables(src_uri, dest_project, dest_dataset, table_prefix, num_copies):    
    
    client = bigquery.client.Client()
    
    client.create_dataset(dest_project + '.' + dest_dataset, exists_ok=True)
        
    dest_dataset_ref = client.dataset(dest_dataset, project=dest_project)
    
    config = bigquery.LoadJobConfig(source_format=bigquery.SourceFormat.AVRO)
    
    for i in range(0, num_copies):
        
        dest_table = table_prefix + '_' + str(i)
        
        dest_table_ref = dest_dataset_ref.table(dest_table)
        
        load_job = client.load_table_from_uri(src_uri, dest_table_ref, job_config=config)
    
        #print('starting load {}'.format(load_job.job_id))

        load_job.result()  
        print('finished load {}'.format(dest_table_ref.table_id))
                 

if __name__ == '__main__':
    
    src_uri = 'gs://austin_311/austin_311_service_requests_sample.avro'
    table_prefix = 'austin_311_service_requests'
    dest_project = 'warehouse-337221'
    dest_dataset = 'austin_311_500k' 
    num_copies = 5
    load_tables(src_uri, dest_project, dest_dataset, table_prefix, num_copies)    


