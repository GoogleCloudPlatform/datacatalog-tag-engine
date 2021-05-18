# Copyright 2020 Google, LLC.
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

import time
import json
from datetime import datetime
from datetime import timedelta
from google.cloud import firestore
from google.cloud import tasks_v2
from google.protobuf import timestamp_pb2

#@firestore.transactional requires function to be at the module level
@firestore.transactional
def update_doc_in_transaction(transaction, doc, update_dict):
    """ 
    
    apply update if document is unchanged
    """
    doc_ref = doc.reference
    snapshot = doc_ref.get(transaction=transaction)

    if doc.update_time == snapshot.update_time:
        transaction.update(doc_ref, update_dict)
        #log
        return True
    else:
        #log
        return False

class ScheduleManager:
    """Class for managing scheduled tasks to update
    
    template_collection_name = firestore collection parent to the tag templates
    config_collection_name = firestore collection parent to the tag configs
    queue_id = "projects/{project}/locations/{region}/queues/{queue_name}"
    app_engine_uri = task handler uri set inside the 
                     app engine project hosting the cloud task queue
    fstore_client = configured firestore client, will default to
                    google.cloud.firestore.Client() if not set
    stale_timer = age of PENDING tasks that get reset to READY (in minutes)
    """
    def __init__(self,
                template_collection_name, 
                config_collection_name,
                queue_id, 
                app_engine_uri, 
                fstore_client=None,
                stale_time=30):

        self.template_collection_name = template_collection_name  
        self.config_collection_name = config_collection_name
        self.queue_id = queue_id
        self.app_engine_uri = app_engine_uri
        self.fclient = fstore_client if fstore_client else firestore.Client()
        self.stale_time = stale_time


##################### API METHODS ############

    def scan_for_update_jobs(self):
        
        #print('scan_for_update_jobs')
        #print('utcnow: ' + str(datetime.utcnow()))
        
        tag_ref = self.fclient.collection(self.config_collection_name)
        tag_ref = tag_ref.where("scheduling_status", "==", "READY")
        tag_ref = tag_ref.where("config_status", "==", "ACTIVE")
        tag_ref = tag_ref.where("next_run", "<=", datetime.utcnow())
        ready_configs = list(tag_ref.stream())
        
        #TODO: consider running transactions async
        for config in ready_configs:
            
            #print('we have a ready config')
            
            transaction = self.fclient.transaction()
            payload = self._set_status_pending(transaction, config)
            
            if payload:
                doc_id = payload[0]
                version = payload[1]
                
                #print('doc_id: ' + doc_id)
                #print('version: ' + str(version))
                
                response = self._send_cloud_task(doc_id, version)
                #print('send_cloud_task response: ' + str(response))
                #log success
            else:
                pass
                #log fail
        return True      


    def reset_stale_jobs(self):
        tag_ref = self.fclient.collection(self.config_collection_name)
        tag_ref = tag_ref.where("scheduling_status", "==", "PENDING")
        tag_ref = tag_ref.where("config_status", "==", "ACTIVE")
        pending_configs = list(tag_ref.stream())
        
        for config in pending_configs:
            udt = config.update_time.ToDatetime()
            if (udt + timedelta(minutes=self.stale_time)) < datetime.utcnow():
                self._set_status_ready(config)
        
        return True
        

    def schedule_job(self, doc_id):
        collection = self.fclient.collection(self.config_collection_name)
        tag_config = collection.document(doc_id).get()
        
        response = self._set_status_ready(tag_config)
        #print(response)
        #Log

    
    def get_config_and_template(self, doc_id):
        tag_config = self.fclient\
            .collection(self.config_collection_name).document(doc_id).get()
        
        template_id = tag_config.get('template_uuid')
        template_config = self.fclient\
            .collection(self.template_collection_name).document(template_id).get()

        return tag_config, template_config 
    #End get_doc_snapshot


################ INTERNAL PROCESSING METHODS #################

    def _set_status_ready(self, doc):
        doc_ref = doc.reference
        snapshot = doc_ref.get()
        data = snapshot.to_dict()

        transaction = self.fclient.transaction()

        task = {
            'scheduling_status':'READY',
        }
        return update_doc_in_transaction(transaction, doc, task)


    def _set_status_pending(self, transaction, doc):
        data = doc.to_dict()

        version = data.get('version', 0) + 1
        delta = data.get('refresh_frequency', 24)
        next_run = datetime.utcnow() + timedelta(hours=delta)
        
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
        client = tasks_v2.CloudTasksClient()
        task = {
            'app_engine_http_request': {  
                'http_method': 'POST',
                'relative_uri': self.app_engine_uri
            }
        }

        payload = {'doc_id':doc_id, 'version':version}
        payload_utf8 = json.dumps(payload).encode()
        task['app_engine_http_request']['body'] = payload_utf8

        response = client.create_task(self.queue_id, task)
        return response

if __name__ == '__main__':
    TEMPLATE_CONFIG = 'tag_template'
    TAG_CONFIG = 'tag_config'
    TASK_QUEUE = 'projects/tag-engine-283315/locations/us-east1/queues/tag-engine'

    db = firestore.Client('tag-engine-283315')
    sm = ScheduleManager(TEMPLATE_CONFIG, TAG_CONFIG, TASK_QUEUE, "/dynamic_catalog_update", db)

    sm.reset_stale_jobs()

    sm.scan_for_update_jobs()
    print("done")



    

    

            
        
