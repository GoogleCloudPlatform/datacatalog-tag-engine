# Copyright 2020-2022 Google, LLC.
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

import time, pytz
import json
from datetime import datetime
from datetime import timedelta
from google.cloud import firestore
from google.cloud import tasks_v2
from google.protobuf import timestamp_pb2

#@firestore.transactional requires function to be at the module level
@firestore.transactional
def update_doc_in_transaction(transaction, doc, update_dict):
    
    print('*** enter update_doc_in_transaction ***')
    
    # apply update if document is unchanged
    doc_ref = doc.reference
    snapshot = doc_ref.get(transaction=transaction)

    if doc.update_time == snapshot.update_time:
        transaction.update(doc_ref, update_dict)
        #log
        return True
    else:
        #log
        return False

class TagScheduler:
    """Class for managing scheduled tasks to update
    
    project = my-project 
    region = us-central1
    queue_name = tag-engine
    app_engine_uri = task handler uri set inside the 
                     app engine project hosting the cloud task queue
    stale_timer = age of PENDING tasks that gets reset to READY (in minutes)
    """
    def __init__(self,
                tag_engine_project,
                queue_region,
                queue_name, 
                app_engine_uri, 
                stale_time=10):

        self.tag_engine_project = tag_engine_project
        self.queue_region = queue_region
        self.queue_name = queue_name
        self.app_engine_uri = app_engine_uri
        self.stale_time = stale_time
        self.db = firestore.Client()

##################### API METHODS ############

    def scan_for_update_jobs(self):
        
        print('*** enter scan_for_update_jobs ***')
        
        db = firestore.Client()
        tag_ref = db.collection('tag_config')
        tag_ref = tag_ref.where("refresh_mode", "==", "AUTO")
        tag_ref = tag_ref.where("scheduling_status", "==", "READY")
        tag_ref = tag_ref.where("config_status", "==", "ACTIVE")
        tag_ref = tag_ref.where("next_run", "<=", datetime.utcnow())
        
        ready_configs = list(tag_ref.stream())
        print('ready_configs: ' + str(ready_configs))
        
        #TODO: consider running transactions async
        for config in ready_configs:
            
            print('found tag config to refresh')
            
            transaction = self.db.transaction()
            payload = self._set_status_pending(transaction, config)
            
            if payload:
                doc_id = payload[0]
                version = payload[1]
                
                print('doc_id: ' + doc_id)
                print('version: ' + str(version))
                
                response = self._send_cloud_task(doc_id, version)
                print('send_cloud_task response: ' + str(response))
                #log success
            else:
                pass
                print('invalid payload')
                #log fail
        return True      


    def reset_stale_jobs(self):
        
        print('*** enter reset_stale_jobs ***')

        tag_ref = self.db.collection("tag_config")
        tag_ref = tag_ref.where("scheduling_status", "==", "PENDING")
        tag_ref = tag_ref.where("config_status", "==", "ACTIVE")
        pending_configs = list(tag_ref.stream())
        
        for config in pending_configs:
            udt = config.update_time.replace(tzinfo=pytz.UTC)
            ts = datetime.utcnow().replace(tzinfo=pytz.UTC)
            if (udt + timedelta(minutes=self.stale_time)) < ts:
                print('found a stale config')
                self._set_status_ready(config)
        
        return True
        

    def schedule_job(self, doc_id):
        
        print('*** enter schedule_job ***')
        
        collection = self.db.collection("tag_config")
        tag_config = collection.document(doc_id).get()
        
        response = self._set_status_ready(tag_config)
        print('response: ' + str(response))
        #Log

    
    def get_config_and_template(self, doc_id):
        
        print('*** enter get_config_and_template ***')
        
        tag_config = self.db.collection('tag_config').document(doc_id).get()
        
        template_id = tag_config.get('template_uuid')
        template_config = self.db\
            .collection('tag_template').document(template_id).get()

        return tag_config, template_config 
    #End get_doc_snapshot


################ INTERNAL PROCESSING METHODS #################

    def _set_status_ready(self, doc):
        
        print('*** enter _set_status_ready ***')
        
        doc_ref = doc.reference
        snapshot = doc_ref.get()
        data = snapshot.to_dict()

        transaction = self.db.transaction()

        task = {
            'scheduling_status':'READY',
        }
        return update_doc_in_transaction(transaction, doc, task)


    def _set_status_pending(self, transaction, doc):
        
        print('*** enter _set_status_pending ***')
        
        data = doc.to_dict()

        version = data.get('version', 0) + 1
        delta = data.get('refresh_frequency', 24)
        unit = data.get('refresh_unit', 'hours')
        
        if unit == 'hours':
            next_run = datetime.utcnow() + timedelta(hours=delta)
        if unit == 'days':
            next_run = datetime.utcnow() + timedelta(days=delta)
        
        print('version: ' + str(version))
        print('delta: ' + str(delta))
        print('next_run: ' + str(next_run))
        
        task = {
            'version': version,
            'scheduling_status':'PENDING',
            'next_run' : next_run
        }

        if update_doc_in_transaction(transaction, doc, task):
            return doc.id, version
        else:
            return None


    def _send_cloud_task(self, doc_id, version):
        
        print('*** enter _send_cloud_task ***')
        
        client = tasks_v2.CloudTasksClient()
        parent = client.queue_path(self.tag_engine_project, self.queue_region, self.queue_name)
        
        task = {
            'app_engine_http_request': {  
                'http_method':  tasks_v2.HttpMethod.POST,
                'relative_uri': self.app_engine_uri
            }
        }
        
        task['app_engine_http_request']['headers'] = {'Content-type': 'application/json'}
        payload = {'doc_id':doc_id, 'version':version}
        print('payload: ' + str(payload))
        
        payload_utf8 = json.dumps(payload).encode()
        task['app_engine_http_request']['body'] = payload_utf8

        response = client.create_task(parent=parent, task=task)
        print('response: ' + str(response))
        
        return response

if __name__ == '__main__':

    config = configparser.ConfigParser()
    config.read("tagengine.ini")
    
    project = config['DEFAULT']['PROJECT']
    region = config['DEFAULT']['REGION']
    queue_name = config['DEFAULT']['QUEUE_NAME']
    app_engine_uri = '/dynamic_auto_update'
    ts = TagScheduler(project, region, queue_name, app_engine_uri)
    ts.reset_stale_jobs()
    ts.scan_for_update_jobs()
    
    print('done')



    

    

            
        
