# Copyright 2025 Google, LLC.
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

import yaml
import sys

parent_dir = '..'
sys.path.append(parent_dir)

import constants as constants 
from common import log_info, log_error
from TagEngineStoreHandler import TagEngineStoreHandler

YAML_FILE = 'migrate/mappings.yaml'

def register_mappings():
    
    store = TagEngineStoreHandler()
    num_mappings = 0
    
    with open(YAML_FILE) as yf:
        full = yaml.full_load(yf)
        mappings = full.get("mappings")
        
        for mapping in mappings: 
            template_id = mapping['template_id']
            template_project = mapping['template_project']
            template_region = mapping['template_region']
            
            aspect_type_id = mapping['aspect_type_id']
            aspect_type_project = mapping['aspect_type_project']
            aspect_type_region = mapping['aspect_type_region']
                        
            try:
                template_uuid = store.write_tag_template(template_id, template_project, template_region)
                aspect_type_uuid = store.write_aspect_type(aspect_type_id, aspect_type_project, aspect_type_region)
                
                mapping['aspect_type_uuid'] = aspect_type_uuid
                success = store.create_update_mapping(template_uuid, mapping)
                                              
                if success:
                    num_mappings += 1
                    
            except Exception as e:
                print('Error occurred while registering mapping in Firestore. Error:', e)
                log_error('Error occurred while registering mapping in Firestore. Error:', e)
            
    print('Created', num_mappings, ' tag template to aspect type mappings.')
    log_info(f'Created {num_mappings} tag template to aspect type mappings.')
    
 
if __name__ == '__main__':
    
    register_mappings()
