from google.cloud import bigquery
    
def load_tables(src_uri, dest_project, dest_dataset, table_prefix, start_index, end_index):    
    
    client = bigquery.client.Client()
    
    client.create_dataset(dest_project + '.' + dest_dataset, exists_ok=True)
        
    dest_dataset_ref = client.dataset(dest_dataset, project=dest_project)
    
    config = bigquery.LoadJobConfig(source_format=bigquery.SourceFormat.AVRO)
    
    for i in range(start_index, end_index):
        
        dest_table = table_prefix + '_' + str(i)
        
        dest_table_ref = dest_dataset_ref.table(dest_table)
        
        load_job = client.load_table_from_uri(src_uri, dest_table_ref, job_config=config)

        load_job.result()  
        print('loaded {}'.format(dest_table_ref.table_id))
                 
                 
if __name__ == '__main__':
    
    src_uri = 'gs://austin_311/austin_311_service_requests_sample.avro'
    dest_project = 'warehouse-337221'
    dest_dataset = 'austin_311_500k' 
    table_prefix = 'austin_311_service_requests'
    start_index = 252799
    end_index = 252800
    load_tables(src_uri, dest_project, dest_dataset, table_prefix, start_index, end_index)    


