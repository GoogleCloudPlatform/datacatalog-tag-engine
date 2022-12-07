#!/usr/bin/python
#
# Copyright 2022 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import argparse
from google.cloud import datacatalog
from google.cloud.datacatalog_v1beta1.types import Taxonomy

ptm = datacatalog.PolicyTagManagerClient()

def create_taxonomy(project_id, region, taxonomy_name):
    
    success = True
    
    location = f'projects/{project_id}/locations/{region}'
    taxonomy = datacatalog.Taxonomy()
    taxonomy.display_name = taxonomy_name
    
    taxonomy_name = None
    
    try:
        created_taxonomy = ptm.create_taxonomy(parent=location, taxonomy=taxonomy)
        taxonomy_name = created_taxonomy.name

    except Exception as e: 
        success = False
        print('Error while creating taxonomy: ', e)
    
    return success, taxonomy_name


def create_policy_tags(taxonomy, policy_tag_labels):
    
    policy_tag_labels_list = policy_tag_labels.split(',')
    
    for label in policy_tag_labels_list:
        
        policy_tag = datacatalog.PolicyTag()
        policy_tag.display_name = label.strip()
        
        request = datacatalog.CreatePolicyTagRequest(
                parent=taxonomy,
                policy_tag=policy_tag
            )

        try:
            created_policy_tag = ptm.create_policy_tag(request=request)
            print(created_policy_tag)
        except Exception as e: 
            print('Error while creating policy tags: ', e)
    

if __name__ == '__main__':
    
    parser = argparse.ArgumentParser(description="Creates a Data Catalog policy tag taxonomy.")
    parser.add_argument('project_id', help='The Google Cloud Project ID in which to create the policy tag taxonomy.')
    parser.add_argument('region', help='The Google Cloud region in which to create the tag template.')
    parser.add_argument('taxonomy_name', help='The name of the policy tag taxonomy to be created in Data Catalog. For example, data_sensitivity_categories')
    parser.add_argument('policy_tag_labels', help='The list of policy tag labels for the taxonomy. For example, [restricted, category1, category2, category3, category4]')
    args = parser.parse_args()
    
    print('policy_tags: ', args.policy_tag_labels)
    
    success, taxonomy_name = create_taxonomy(args.project_id, args.region, args.taxonomy_name)
    
    if success:
        create_policy_tags(taxonomy_name, args.policy_tag_labels)
