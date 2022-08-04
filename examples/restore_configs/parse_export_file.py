import jsonlines

#tag_template = "'templateId': 'data_resource'"
#tag_template = "'templateId': 'data_attribute'"
#location_id = "'locationId': 'us'"
#file_name = 'Exported_Metadata_Project_tag-engine-develop_2022-07-13T17-54-39Z_UTC.jsonl'

tag_template = "'templateId': 'file_metadata'"
project_id = "'projectId': 'tag-engine-develop'"
location_id = "'locationId': 'us-central1'"
file_name = 'Exported_Metadata_Project_tag-engine-develop_2022-07-19T22-31-25Z_UTC.jsonl'


columns = 'columns'
tags = 'tags'
create_time = 'createTime'
update_time = 'updateTime'
snapshot_time = 'snapshotTime'


def match_template(json_obj):
    
    if tag_template in str(json_obj):
        return True
    else:
        return False

def match_full_template(json_obj):
    
    if tag_template in str(json_obj) and project_id in str(json_obj) and location_id in str(json_obj):
        return True
    else:
        return False

with jsonlines.open(file_name) as reader:
    
    index = 0
    
    for obj in reader:
        
        if tag_template in str(obj):
            
            #print(str(index) + ': ' + str(obj))
            unwanted_keys = [] 
            
            for k, v in obj.items():
                #print('** key: ', k, ', value: ', v)
                
                if k == columns and match_template(v) == True:
                    #print('$$$$ column ', v)
                    
                    columns_copy = v.copy()
                    
                    for element in columns_copy:
                        #print('$$$$$ element ', element)
                        if match_template(element) == False:
                           v.remove(element)
                
                if k == columns and match_template(v) == False:
                    #print('added ', k, ' to unwanted_keys')
                    unwanted_keys.append(k)
                
                if k == tags and match_template(v) == True:
                    
                    tags_copy = v.copy()
                    
                    for element in tags_copy:
                        #print('$$$$ element ', element)
                        # looking for elements with references to tag template
                        if match_template(element) == False:
                           v.remove(element)
                           
                    #print('#### we are left with: ', v)
                        
                if k == tags and match_template(v) == False:
                    #print('added ', k, ' to unwanted_keys')
                    unwanted_keys.append(k)
                        
                if k == create_time or k == update_time or k == snapshot_time:
                    #print('added ', k, ' to unwanted_keys')
                    unwanted_keys.append(k)
            
            #print('unwanted_keys: ', unwanted_keys)
            for k in unwanted_keys:
                del obj[k]
            
            if match_full_template(obj):
                print('final obj: ', obj)
            
        else:
            print('didn\'t find tag template') 
            print('******')
        
        index = index + 1
        

    
    