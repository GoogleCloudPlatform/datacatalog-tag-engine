import json
import time
import argparse
from datetime import timedelta
from datetime import datetime
from google.cloud import datacatalog
from google.cloud import bigquery

dc = datacatalog.DataCatalogClient()
   
def search_catalog(bq_project, bq_dataset, tag_template_project, tag_template1, tag_template2=None):
    
    scope = datacatalog.SearchCatalogRequest.Scope()
    scope.include_project_ids.append(bq_project)
    scope.include_project_ids.append(tag_template_project)

    request = datacatalog.SearchCatalogRequest()
    request.scope = scope
    
    parent = 'parent:' + bq_project + '.' + bq_dataset
    
    if tag_template2 != None:
        tag = 'tag:' + tag_template_project + '.' + tag_template1 + ' or tag:' + tag_template_project + '.' + tag_template2
    else:
        tag = 'tag:' + tag_template_project + '.' + tag_template1
    
    query = parent + ' ' + tag
    print('query string: ' + query)
    
    request.query = query
    request.page_size = 1
    
    num_results = 0

    start_time = time.time()

    for result in dc.search_catalog(request):
        #print('result: ' + str(result))
        num_results += 1
        print(str(num_results))
    
    print('tagged assets count: ' + str(num_results))
    
    end_time = time.time()
    run_time = (end_time - start_time)
    td = timedelta(seconds=run_time)
    print('search catalog runtime:', td)


def list_tags(bq_project, bq_dataset, tag_template_project, tag_template):
    
    with open('logs/list_' + bq_dataset + '.out', 'w') as log:
        log.write('started scanning ' + bq_dataset + ' at ' + datetime.now().strftime('%m/%d/%Y, %H:%M:%S') + '\n')
    
        bq = bigquery.Client()
        sql = 'select table_name from ' + bq_project + '.' + bq_dataset + '.INFORMATION_SCHEMA.TABLES'
        query_job = bq.query(sql)  
        rows = query_job.result()  

        for row in rows:
            table_name = row.table_name
            #print('processing ' + table_name)
    
            resource = '//bigquery.googleapis.com/projects/' + bq_project + '/datasets/' + bq_dataset + '/tables/' + table_name
            #print("resource: " + resource)
    
            request = datacatalog.LookupEntryRequest()
            request.linked_resource=resource
            entry = dc.lookup_entry(request)
        
            page_result = dc.list_tags(parent=entry.name)
            
            found_tag = False
            
            for tag in page_result:
                index = tag.template.rfind('/')
                attached_template = tag.template[index+1:]   
                #print('attached template: ' + attached_template)
                
                if attached_template == tag_template:
                    found_tag = True
                    break
                
            if found_tag != True:
                log.write(table_name + ' is untagged \n') 
        
        
        log.write('finished scanning at ' + datetime.now().strftime('%m/%d/%Y, %H:%M:%S') + '\n')
        
if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='runs check_tags.py')
    parser.add_argument('option', help='Choose search or list')
    args = parser.parse_args()
    
    bq_project = 'warehouse-337221'
    bq_dataset = 'austin_311_500k'
    
    tag_template_project = 'tag-engine-vanilla-337221'
    tag_template = 'data_governance_500k'

    if args.option == 'search':
        search_catalog(bq_project, bq_dataset, tag_template_project, tag_template)
    
    if args.option == 'list':
        list_tags(bq_project, bq_dataset, tag_template_project, tag_template)
 