# Copyright 2020-2023 Google, LLC.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import google.auth
from google.api_core.client_info import ClientInfo
from google.cloud import bigquery
from google.cloud import storage
from google.cloud import resourcemanager_v3
import constants, configparser

USER_AGENT = 'cloud-solutions/datacatalog-tag-engine-v2'

class Resources:
    
    bigquery_resource = "bigquery"
    pubsub_resource = "pubsub"
    gcs_resource = "gs:"
    
    def __init__(self, credentials):
        
        self.bq_client = bigquery.Client(credentials=credentials, client_info=ClientInfo(user_agent=USER_AGENT))
        self.gcs_client = storage.Client(credentials=credentials, client_info=ClientInfo(user_agent=USER_AGENT))

    def get_resources(self, included_uris, excluded_uris):
        
        #print('enter get_resources()')
        #print('included_uris: ' + included_uris)
        
        # find out what kind of resource we have
        included_uris_list = included_uris.split(',')
        resource_type = included_uris_list[0].strip().split('/')[0]
        #print("resource_type: " + resource_type)
    
        if resource_type == self.bigquery_resource:
                    
            included_resources = self.find_bq_resources(included_uris)
            #print("included_resources: " + str(included_resources))
        
            if excluded_uris is None or excluded_uris == "" or excluded_uris.isspace():
                return included_resources
            else:
                #print("excluded_uris: " + excluded_uris)
                excluded_resources = self.find_bq_resources(excluded_uris)
                #print("excluded_resources: " + str(excluded_resources))
        
        
        elif resource_type == self.gcs_resource:
            
            included_resources = self.find_gcs_resources(included_uris)
        
            if excluded_uris is None or excluded_uris == "" or excluded_uris.isspace():
                return included_resources
            else:
                #print("excluded_uris: " + excluded_uris)
                excluded_resources = self.find_gcs_resources(excluded_uris)
                #print("excluded_resources: " + str(excluded_resources))
        
        else:
            print('Error: expected to get a bigquery or gcs resource type, but found this: ' + resource_type)
            return None
        
        remaining_resources = included_resources.difference(excluded_resources)
        
        return remaining_resources


    def get_resources_by_project(self, projects):
        
        print('projects:', projects)
        
        uris = []
        
        for project in projects:
            
            print('project:', project)
            datasets = list(self.bq_client.list_datasets(project=project))
            
            for dataset in datasets:
                print('dataset:', dataset.dataset_id)
                
                formatted_dataset = self.format_dataset_resource(project + '.' + dataset.dataset_id)
                uris.append(formatted_dataset)
                
                tables = self.bq_client.list_tables(project + '.' + dataset.dataset_id)
                
                for table in tables:
                    
                    formatted_table = self.format_table_resource(table.full_table_id)
                    uris.append(formatted_table)
                    
        return uris
            

    def get_resources_by_folder(self, folder):

        if folder.replace('folders/', '').isnumeric() == False:
            print('Error: The folder parameter must be a numeric value')
            return
        
        if 'folders/' not in folder: 
            folder = 'folders/' + folder
        
        rm_client = resourcemanager_v3.ProjectsClient()

        request = resourcemanager_v3.ListProjectsRequest(
            parent=folder,
        )

        resp = rm_client.list_projects(request=request)
        
        projects = []
        
        for project in resp:
            projects.append(project.project_id)
        
        uris = self.get_resources_by_project(projects)
        
        return uris
        
              
    def format_table_resource(self, table_resource):
         # BQ table format: project:dataset.table
         # DC expected resource format: project_id + '/datasets/' + dataset + '/tables/' + short_table
         
        formatted = table_resource.replace(":", "/datasets/").replace(".", "/tables/")
        #print("formatted: " + table_resource)
         
        return formatted
                  
    def format_dataset_resource(self, dataset_resource):
         # BQ table format: project:dataset.table
         # DC expected resource format: project_id + '/datasets/' + dataset + '/tables/' + short_table
         
        formatted = dataset_resource.replace(".", "/datasets/")
        #print("formatted: " + table_resource)
         
        return formatted
    
    def get_datasets(self, dataset):
        
        dataset_list = []
        
        if dataset.endswith("*"):
            datasets = list(self.bq_client.list_datasets())  

            for ds in datasets:
                if dataset[:-1] in ds.dataset_id:
                    dataset_list.append(ds.dataset_id)
        else:
            dataset_list.append(dataset)
        
        return dataset_list
        
    
    def find_bq_resources(self, uris):
       
        # @input uris: comma-separated list of uri representing a BQ resource
        # BQ resources are specified as:  
        # bigquery/project/<project>/dataset/<dataset>/<table>
        # wildcards are allowed in the table and dataset components of the uri 
        resources = set()
        table_resources = set() 
        column_resources = set() 
        
        uri_list = uris.split(",")
        
        for uri in uri_list: 
            print("uri: " + uri)
            split_path = uri.strip().split("/")

            if split_path[1] != "project":
                print("Error: invalid URI " + path)
                return None
            
            project_id = split_path[2]
   
            path_length = len(split_path)
            #print("path_length: " + str(path_length))
            
            if path_length == 4:
                
                print('uri ' + uri + ' is at the project level')
                
                datasets = list(self.bq_client.list_datasets(project=project_id))
                
                for dataset in datasets:
                    tables = list(self.bq_client.list_tables(dataset.dataset_id))
        
                    for table in tables:
                        table_resources.add(table.full_table_id)
                
                tag_type = constants.BQ_TABLE_TAG
             
            if path_length > 4:
               
                dataset = split_path[4]
                dataset_list = self.get_datasets(dataset)                
                 
                for dataset_name in dataset_list:            
                    dataset_id = project_id + "." + dataset_name
            
                    print("path_length: ", path_length)
                    print("dataset_id: " + dataset_id)
                
                    if path_length == 5: 
                        tag_type = constants.BQ_DATASET_TAG
                        dataset_resource = self.format_dataset_resource(dataset_id)
                        resources.add(dataset_resource)
                        continue
                
                    table_expression = split_path[5]
                    print("table_expression: " + table_expression)

                    if path_length != 6:
                        print("Error. Invalid URI " + path)
                        return None
                    else:
                        tag_type = constants.BQ_TABLE_TAG

                    if table_expression == "*":
                        #print("list tables in dataset")
                        tables = list(self.bq_client.list_tables(self.bq_client.get_dataset(dataset_id)))
            
                        for table in tables:
                            #print("full_table_id: " + str(table.full_table_id))
                            table_resources.add(table.full_table_id)
                    
                    elif "*" in table_expression:
                        #print("table expression contains wildcard")
                        table_substrings = table_expression.split("*")
                        tables = list(self.bq_client.list_tables(self.bq_client.get_dataset(dataset_id)))
                    
                        for table in tables:
                            is_match = True
                            for substring in table_substrings:
                                if substring not in table.full_table_id:
                                    is_match = False
                                    break
                        
                            if is_match == True:
                                table_resources.add(table.full_table_id)
                
                    else:
                        table_id = dataset_id + "." + table_expression
                
                        try:
                            table = self.bq_client.get_table(table_id)
                            table_resources.add(table.full_table_id)
                    
                        except NotFound:
                            print("Error: " + table_id + " not found.")
            
                    
            if tag_type == constants.BQ_TABLE_TAG:
                for table in table_resources:
                    formatted_table = self.format_table_resource(table)
                    resources.add(formatted_table)
        
        return resources      
                  
    def find_gcs_resources(self, uris):
    
        resources = set()
        
        uris_list = uris.split(',')
        
        for uri in uris_list:
            
            # remove the 'gs://' prefix from the uri
            short_uri = uri[5:].strip()
            #print('short_uri: ' + short_uri)
            
            split_uri = short_uri.split('/')
            bucket_name = split_uri[0]
            #print('bucket_name: ' + bucket_name)
            
            # uri contains a folder
            # examples: discovery-area/cities_311/* or discovery-area/cities_311/austin_311_service_requests.parquet
            if len(split_uri) > 2:
                folder_start_index = len(bucket_name) + 1
                #print('folder_start_index: ', folder_start_index)
                
                # uri points to a folder
                if short_uri.endswith('/*'):    
                    folder_end_index = short_uri.index('/*') 
                    folder = short_uri[folder_start_index:folder_end_index]
                    #print('folder: ' + folder)
                    
                    for blob in self.gcs_client.list_blobs(bucket_name, prefix=folder):
                        if blob.name == folder + '/' or blob.name.endswith('/'):
                            continue
                        resources.add((bucket_name, blob.name))
                        
                # uri points to a specific file
                # example: discovery-area/cities_311/austin_311_service_requests.parquet    
                else:
                    filename = short_uri[folder_start_index:]
                    #print('filename: ' + filename) 
                    bucket = self.gcs_client.get_bucket(bucket_name)
                    blob = bucket.blob(filename)
                    if blob.exists():
                        resources.add((bucket_name, blob.name))
            
            # uri does not contain a folder
            # examples: discovery-area/* or discovery-area/austin_311_service_requests.parquet  
            elif len(split_uri) == 2:    
                
                if short_uri.endswith('/*'):  
                    for blob in self.gcs_client.list_blobs(bucket_name):
                        if blob.name.endswith('/'):
                            continue
                        #print('blob: ' + str(blob.name))
                        resources.add((bucket_name, blob.name))
                else:
                    file_index_start = short_uri.index('/') + 1 
                    filename = short_uri[file_index_start:]
                    #print('filename: ' + filename)
                    bucket = self.gcs_client.get_bucket(bucket_name)
                    blob = bucket.blob(filename)
                    if blob.exists():
                        if blob.name.endswith('/') == False:
                            resources.add((bucket_name, blob.name))    
            else:
                print('Error: invalid uri provided: ' + uri)
                
        return resources
        
if __name__ == '__main__':
    
    credentials, _ = google.auth.default()
    res = Resources(credentials)
    uris = res.get_resources('bigquery/project/tag-engine-run/dataset/GCP_Mockup/*', None)
    #uris = get_resources_by_project(['record-manager-service'])
    #uris = get_resources_by_folder('folders/a593258468753') 
    print(uris)   