# Copyright 2021 Google, LLC.
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

import argparse
from google.cloud.firestore_admin_v1.services.firestore_admin.client import FirestoreAdminClient

fs_client = FirestoreAdminClient()

def create_index_1(project_id):
    
    coll = 'projects/{project_id}/databases/(default)/collectionGroups/propagated_config'.format(project_id=project_id)
    
    index = {
         "fields": [
        {
          "field_path": "template_uuid",
          "order": "ASCENDING"
        },
        {
          "field_path": "source_res",
          "order": "ASCENDING"
        },
        {
          "field_path": "view_res",
          "order": "ASCENDING"
        }],
        "query_scope": "COLLECTION"
        }

    resp = fs_client.create_index(parent=coll, index=index)


def create_index_2(project_id):
    
    coll = 'projects/{project_id}/databases/(default)/collectionGroups/tag_config'.format(project_id=project_id)
    
    index = {
         "fields": [
        {
          "field_path": "template_uuid",
          "order": "ASCENDING"
        },
        {
          "field_path": "config_status",
          "order": "ASCENDING"
        }],
        "query_scope": "COLLECTION"
        }

    resp = fs_client.create_index(parent=coll, index=index)
    

def create_index_3(project_id):
    
    coll = 'projects/{project_id}/databases/(default)/collectionGroups/logs'.format(project_id=project_id)
    
    index = {
         "fields": [
        {
          "field_path": "rs",
          "order": "ASCENDING"
        },
        {
          "field_path": "template_uuid",
          "order": "ASCENDING"
        },
        {
          "field_path": "ts",
          "order": "DESCENDING"
        }],
        "query_scope": "COLLECTION"
        }

    resp = fs_client.create_index(parent=coll, index=index)

def create_index_4(project_id):
    
    coll = 'projects/{project_id}/databases/(default)/collectionGroups/logs'.format(project_id=project_id)
    
    index = {
         "fields": [
        {
          "field_path": "dc_op",
          "order": "ASCENDING"
        },
        {
          "field_path": "res",
          "order": "ASCENDING"
        }],
        "query_scope": "COLLECTION"
        }

    resp = fs_client.create_index(parent=coll, index=index)

def create_index_5(project_id):
    
    coll = 'projects/{project_id}/databases/(default)/collectionGroups/tag_config'.format(project_id=project_id)
    
    index = {
         "fields": [
        {
          "field_path": "scheduling_status",
          "order": "ASCENDING"
        },
        {
          "field_path": "config_status",
          "order": "ASCENDING"
        },
        {
          "field_path": "next_run",
          "order": "ASCENDING"
        }],
        "query_scope": "COLLECTION"
        }

    resp = fs_client.create_index(parent=coll, index=index)


def create_index_6(project_id):
    
    coll = 'projects/{project_id}/databases/(default)/collectionGroups/logs'.format(project_id=project_id)
    
    index = {
         "fields": [
        {
          "field_path": "config_type",
          "order": "ASCENDING"
        },
        {
          "field_path": "res",
          "order": "ASCENDING"
        },
        {
          "field_path": "ts",
          "order": "DESCENDING"
        }],
        "query_scope": "COLLECTION"
        }

    resp = fs_client.create_index(parent=coll, index=index)


def create_index_7(project_id):
    
    coll = 'projects/{project_id}/databases/(default)/collectionGroups/tag_config'.format(project_id=project_id)
    
    index = {
         "fields": [
        {
          "field_path": "scheduling_status",
          "order": "ASCENDING"
        },
        {
          "field_path": "config_status",
          "order": "ASCENDING"
        }],
        "query_scope": "COLLECTION"
        }

    resp = fs_client.create_index(parent=coll, index=index)


def create_index_8(project_id):
    
    coll = 'projects/{project_id}/databases/(default)/collectionGroups/tag_template'.format(project_id=project_id)
    
    index = {
         "fields": [
        {
          "field_path": "template_id",
          "order": "ASCENDING"
        },
        {
          "field_path": "project_id",
          "order": "ASCENDING"
        },
        {
          "field_path": "region",
          "order": "ASCENDING"
        }],
        "query_scope": "COLLECTION"
        }

    resp = fs_client.create_index(parent=coll, index=index)


def create_index_9(project_id):
    
    coll = 'projects/{project_id}/databases/(default)/collectionGroups/tag_config'.format(project_id=project_id)
    
    index = {
         "fields": [
        {
          "field_path": "config_status",
          "order": "ASCENDING"
        },
        {
          "field_path": "refresh_mode",
          "order": "ASCENDING"
        },
        {
          "field_path": "scheduling_status",
          "order": "ASCENDING"
        },
        {
          "field_path": "next_run",
          "order": "ASCENDING"
        }],
        "query_scope": "COLLECTION"
        }

    resp = fs_client.create_index(parent=coll, index=index)


def create_index_10(project_id):
    
    coll = 'projects/{project_id}/databases/(default)/collectionGroups/tag_config'.format(project_id=project_id)
    
    index = {
         "fields": [
        {
          "field_path": "template_uuid",
          "order": "ASCENDING"
        },
        {
          "field_path": "included_uris",
          "order": "ASCENDING"
        },
        {
          "field_path": "tag_type",
          "order": "ASCENDING"
        },
        {
          "field_path": "config_status",
          "order": "ASCENDING"
        }],
        "query_scope": "COLLECTION"
        }

    resp = fs_client.create_index(parent=coll, index=index)


def create_index_11(project_id):
    
    coll = 'projects/{project_id}/databases/(default)/collectionGroups/logs'.format(project_id=project_id)
    
    index = {
         "fields": [
        {
          "field_path": "config_status",
          "order": "ASCENDING"
        },
        {
          "field_path": "config_type",
          "order": "ASCENDING"
        },
        {
          "field_path": "ts",
          "order": "DESCENDING"
        }],
        "query_scope": "COLLECTION"
        }

    resp = fs_client.create_index(parent=coll, index=index)


def create_index_12(project_id):
    
    coll = 'projects/{project_id}/databases/(default)/collectionGroups/propagated_config'.format(project_id=project_id)
    
    index = {
         "fields": [
        {
          "field_path": "template_uuid",
          "order": "ASCENDING"
        },
        {
          "field_path": "source_res",
          "order": "ASCENDING"
        },
        {
          "field_path": "view_res",
          "order": "ASCENDING"
        },
        {
          "field_path": "config_status",
          "order": "ASCENDING"
        }],
        "query_scope": "COLLECTION"
        }

    resp = fs_client.create_index(parent=coll, index=index)

   
if __name__ == '__main__':
    
    parser = argparse.ArgumentParser(description="create Firestore indexes")
    parser.add_argument('project_id', help='Google Cloud Project id')
    args = parser.parse_args()
     
    print('creating indexes on project ' + args.project_id)

    create_index_1(args.project_id)
    create_index_2(args.project_id)
    create_index_3(args.project_id)
    create_index_4(args.project_id)
    create_index_5(args.project_id)
    create_index_6(args.project_id)
    create_index_7(args.project_id)
    create_index_8(args.project_id)
    create_index_9(args.project_id)
    create_index_10(args.project_id)
    create_index_11(args.project_id)
    create_index_12(args.project_id)
    