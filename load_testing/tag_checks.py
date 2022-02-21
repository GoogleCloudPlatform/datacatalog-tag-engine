import json
import time
from datetime import timedelta
from google.cloud import datacatalog

dc = datacatalog.DataCatalogClient()
   
def get_tag_count(bq_project, bq_dataset, tag_template_project, tag_template1, tag_template2=None):
    
    scope = datacatalog.SearchCatalogRequest.Scope()
    scope.include_project_ids.append(bq_project)
    scope.include_project_ids.append(tag_template_project)

    request = datacatalog.SearchCatalogRequest()
    request.scope = scope
    
    parent = 'parent:' + bq_project + '.' + bq_dataset
    tag = 'tag:' + tag_template_project + '.' + tag_template1 + ' or tag:' + tag_template_project + '.' + tag_template2
    
    query = parent + ' ' + tag
    print('query string: ' + query)
    
    request.query = query
    request.page_size = 1
    
    num_results = 0

    start_time = time.time()

    for result in dc.search_catalog(request):
        print('result: ' + str(result))
        num_results += 1
        print(str(num_results))
    
    print('tagged assets found: ' + str(num_results))
    
    end_time = time.time()
    run_time = (end_time - start_time)
    td = timedelta(seconds=run_time)
    print('runtime:', td)
    
        
if __name__ == '__main__':

    bq_project = 'warehouse-337221'
    bq_dataset = 'cities_311'
    
    tag_template_project = 'tag-engine-vanilla-337221'
    tag_template1 = 'cities_311'
    tag_template2 = 'data_governance'
    
    get_tag_count(bq_project, bq_dataset, tag_template_project, tag_template1, tag_template2)
    
 