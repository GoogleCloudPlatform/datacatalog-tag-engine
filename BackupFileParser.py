# Copyright 2022 Google, LLC.
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

from google.cloud import storage
import jsonlines

columns = 'columns'
tags = 'tags'
create_time = 'createTime'
update_time = 'updateTime'
snapshot_time = 'snapshotTime'

class BackupFileParser:

    @staticmethod
    def match_template_id(json_obj, source_template_id):
    
        tag_template = "'templateId': '{}'".format(source_template_id)
        
        if tag_template in str(json_obj):
            return True
        else:
            return False

    
    @staticmethod
    def match_template_id_project(json_obj, source_template_id, source_template_project):
    
        is_match = False
        
        tag_template = "'templateId': '{}'".format(source_template_id)
        project_id = "'projectId': '{}'".format(source_template_project)
        
        #print('tag_template: ', tag_template)
        #print('project_id: ', project_id)
        
        if tag_template in str(json_obj) and project_id in str(json_obj):
            is_match = True
        
        return is_match

    
    @staticmethod
    def extract_tags(source_template_id, source_template_project, backup_file):
        
        gcs_client = storage.Client()
        extracted_tags = [] # stores the result set

        # download the backup file from GCS
        bucket_name, filename = backup_file
        bucket = gcs_client.get_bucket(bucket_name)
        blob = bucket.get_blob(filename)
        
        tmp_file = '/tmp/' + filename
        blob.download_to_filename(filename=tmp_file)
        
        with jsonlines.open(tmp_file) as reader:
    
            index = 0
    
            for obj in reader:
        
                if BackupFileParser.match_template_id(str(obj), source_template_id) == False:
                    continue
            
                #print(str(index) + ': ' + str(obj))
                unwanted_keys = [] 
        
                for k, v in obj.items():
                    #print('** key: ', k, ', value: ', v)
            
                    if k == columns and BackupFileParser.match_template_id_project(v, source_template_id, source_template_project) == True:
                        #print('$$$$ column ', v)
                
                        columns_copy = v.copy()
                
                        for element in columns_copy:
                            #print('$$$$$ element ', element)
                            if BackupFileParser.match_template_id(element, source_template_id) == False:
                               v.remove(element)
            
                    if k == columns and BackupFileParser.match_template_id_project(v, source_template_id, source_template_project) == False:
                        #print('added ', k, ' to unwanted_keys')
                        unwanted_keys.append(k)
            
                    if k == tags and BackupFileParser.match_template_id_project(v, source_template_id, source_template_project) == True:
                
                        tags_copy = v.copy()
                
                        for element in tags_copy:
                            #print('$$$$ element ', element)
                            # looking for elements with references to tag template
                            if BackupFileParser.match_template_id(element, source_template_id) == False:
                               v.remove(element)
                       
                        #print('#### we are left with: ', v)
                    
                    if k == tags and BackupFileParser.match_template_id_project(v, source_template_id, source_template_project) == False:
                        #print('added ', k, ' to unwanted_keys')
                        unwanted_keys.append(k)
                    
                    if k == create_time or k == update_time or k == snapshot_time:
                        #print('added ', k, ' to unwanted_keys')
                        unwanted_keys.append(k)
        
                #print('unwanted_keys: ', unwanted_keys)
                for k in unwanted_keys:
                    del obj[k]
        
                if BackupFileParser.match_template_id_project(obj, source_template_id, source_template_project):
                    extracted_tags.append(obj)
            
                index = index + 1
        
        return extracted_tags
    
if __name__ == '__main__':
    
    bkp_file = ('catalog_metadata_exports', 'Exported_Metadata_Project_tag-engine-develop_2022-08-04T15-23-28Z_UTC.jsonl')
    extracted_tags = BackupFileParser.extract_tags('data_resource', 'data-mesh-344315', bkp_file)
    print('extracted_tags: ', extracted_tags)
    
    bkp_file = ('catalog_metadata_exports', 'Exported_Metadata_Project_tag-engine-develop_2022-08-02T22-09-14Z_UTC.jsonl')
    extracted_tags = BackupFileParser.extract_tags('data_attribute', 'data-mesh-344315', bkp_file)
    print('extracted_tags: ', extracted_tags)
        