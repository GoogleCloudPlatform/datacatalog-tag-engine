from google.cloud import bigquery
 
'''
create empty tables in BQ for the purpose of load testing static tag configs
'''  
def create_tables(dest_project, dest_dataset, table_prefix, start_index, end_index):    
    
    client = bigquery.client.Client()
    
    schema = [
        bigquery.SchemaField("unique_key", "STRING"),
        bigquery.SchemaField("complaint_type", "STRING"),
        bigquery.SchemaField("complaint_description", "STRING"),
        bigquery.SchemaField("owning_department", "STRING"),
        bigquery.SchemaField("source", "STRING"),
        bigquery.SchemaField("status", "STRING"),
        bigquery.SchemaField("status_change_date", "INTEGER"),
        bigquery.SchemaField("created_date", "INTEGER"),
        bigquery.SchemaField("last_update_date", "INTEGER"),
        bigquery.SchemaField("close_date", "INTEGER"),
        bigquery.SchemaField("incident_address", "STRING"),
        bigquery.SchemaField("street_number", "STRING"),
        bigquery.SchemaField("street_name", "STRING"),
        bigquery.SchemaField("city", "STRING"),
        bigquery.SchemaField("incident_zip", "INTEGER"),
        bigquery.SchemaField("county", "STRING"),
        bigquery.SchemaField("state_plane_x_coordinate", "STRING"),
        bigquery.SchemaField("state_plane_y_coordinate", "FLOAT"),
        bigquery.SchemaField("latitude", "FLOAT"),
        bigquery.SchemaField("longitude", "FLOAT"),
        bigquery.SchemaField("location", "STRING"),
        bigquery.SchemaField("council_district_code", "INTEGER"),
        bigquery.SchemaField("map_page", "STRING"),
        bigquery.SchemaField("map_tile", "STRING")
    ]
	
    for i in range(start_index, end_index):
        
        dest_table = dest_project + '.' + dest_dataset + '.' + table_prefix + '_' + str(i)
        
        table = bigquery.Table(dest_table, schema=schema)
        
        table = client.create_table(table)
        
        print('created table {}.{}.{}'.format(table.project, table.dataset_id, table.table_id))
                 
                 
if __name__ == '__main__':
    
    dest_project = 'warehouse-337221'
    dest_dataset = 'austin_311_1m' 
    table_prefix = 'austin_311_service_requests'
    start_index = 394906
    end_index = 1000000
    create_tables(dest_project, dest_dataset, table_prefix, start_index, end_index)    
