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

import json, datetime

from google.cloud import pubsub
from google.cloud.exceptions import NotFound

import TagEngineUtils as te

class PubSubUtils:
    
    def __init__(self):
        
        self.client = pubsub.PublisherClient()
        
        store = te.TagEngineUtils()
        enabled, settings = store.read_tag_stream_settings()
        
        self.enabled = enabled
        self.project_id = settings['pubsub_project']
        self.topic = settings['pubsub_topic']
        
        #print('project_id: ' + self.project_id)
        #print('topic: ' + self.topic)
        

    def copy_tag(self, template_id, resource, column, fields):

        #print("*** enter PubSubUtils.copy_tag ***")
        #print("template_id: " + template_id)
        #print("resource: " + resource)
        #print("column: " + column)
        #print("fields: " + str(fields))
        
        topic_name = 'projects/' + self.project_id + '/topics/' + self.topic
        
        payload = {}
        payload['event_time'] = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%f')
        payload['asset_name'] = resource.replace("datasets", "dataset").replace("tables", "table")
        payload['template_id'] = template_id
        payload['tag'] = fields
        #print('payload: ' + str(payload))
        
        json_payload = json.dumps(payload, indent=4, sort_keys=True, default=str)
        #print('json_payload: ' + str(json_payload))
        
        resp = self.client.publish(topic_name, json_payload.encode('utf-8'))
        
        #print('pub/sub resp: ' + str(resp))
        
        