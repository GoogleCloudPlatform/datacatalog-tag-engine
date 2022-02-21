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

import uuid, hashlib, datetime, json, configparser
import constants
from google.cloud import firestore
from google.cloud import tasks_v2


class TaskManager:
    """Class for creating and managing work requests in the form of cloud tasks 
    
    project = App Engine project id (e.g. tag-engine-project)
    region = App Engine region (e.g. us-central1)
    queue_name = Cloud Task queue (e.g. tag-engine-queue)
    app_engine_uri = task handler uri set inside the 
                     App Engine project hosting the cloud task queue
    """
    def __init__(self,
                tag_engine_project,
                queue_region,
                queue_name, 
                app_engine_uri):

        self.tag_engine_project = tag_engine_project
        self.queue_region = queue_region
        self.queue_name = queue_name
        self.app_engine_uri = app_engine_uri

        self.db = firestore.Client()

##################### API METHODS #################
        
    def create_work_tasks(self, job_uuid, tag_uuid, uris):
        
        print('*** enter create_work_tasks ***')
 
        for uri in uris:
            
            task_id_raw = job_uuid + uri
            task_id = hashlib.md5(task_id_raw.encode()).hexdigest()
            
            print('generated task_id ' + task_id)
            
            task_uuid = self._record_task(job_uuid, tag_uuid, uri, task_id)
            self._create_task(job_uuid, tag_uuid, uri, task_id, task_uuid)

        
    def update_task_status(self, task_uuid, status):     

        if status == 'RUNNING':
            self._set_task_running(task_uuid)
        
        if status == 'COMPLETED':
            self._set_task_completed(task_uuid)
            
        if status == 'FAILED':
            self._set_task_failed(task_uuid)


################ INTERNAL PROCESSING METHODS #################


    def _record_task(self, job_uuid, tag_uuid, uri, task_id):
        
        print('*** _record_task ***')
        
        task_uuid = uuid.uuid1().hex
        
        task_ref = self.db.collection('tasks').document(task_uuid)

        task_ref.set({
            'task_uuid': task_uuid, # unique identifier for task record in Firestore
            'task_id': task_id,     # unique identifier for cloud task, based on uri
            'job_uuid': job_uuid,
            'tag_uuid': tag_uuid,
            'uri': uri,
            'status':  'PENDING',
            'creation_time': datetime.datetime.utcnow()
        })
        
        print('created task record ' + task_uuid)
         
        return task_uuid    
    
    
    def _create_task(self, job_uuid, tag_uuid, uri, task_id, task_uuid):
        
        print('*** enter _create_task ***')
 
        client = tasks_v2.CloudTasksClient()
        parent = client.queue_path(self.tag_engine_project, self.queue_region, self.queue_name)
        
        task = {
            'name': parent + '/tasks/' + task_id,
            'app_engine_http_request': {  
                'http_method':  tasks_v2.HttpMethod.POST,
                'relative_uri': self.app_engine_uri
            }
        }
        
        task['app_engine_http_request']['headers'] = {'Content-type': 'application/json'}
        payload = {'job_uuid': job_uuid, 'tag_uuid': tag_uuid, 'uri': uri, 'task_uuid': task_uuid}
        print('payload: ' + str(payload))
        
        payload_utf8 = json.dumps(payload).encode()
        task['app_engine_http_request']['body'] = payload_utf8

        try:
            task = client.create_task(parent=parent, task=task)
        
        except:
            print('Error: could not create task for task_uuid ' + task_uuid)
            self._set_task_failed(task_uuid)
            
        return task.name
        
    
    def _set_task_running(self, task_uuid):
        
        print('*** _set_task_running ***')
        
        task_ref = self.db.collection('tasks').document(task_uuid)

        task_ref.set({
            'status':  'RUNNING',
            'start_time': datetime.datetime.utcnow()
        }, merge=True)
        
        print('set task running.')
    
    
    def _set_task_completed(self, task_uuid):
        
        print('*** _set_task_completed ***')
        
        task_ref = self.db.collection('tasks').document(task_uuid)

        task_ref.set({
            'status':  'COMPLETED',
            'end_time': datetime.datetime.utcnow()
        }, merge=True)
        
        print('set task completed.')


    def _set_task_failed(self, task_uuid):
        
        print('*** _set_task_failed *** task_uuid: ' + task_uuid)
        
        task_ref = self.db.collection('tasks').document(task_uuid)

        task_ref.set({
            'status':  'FAILED',
            'end_time': datetime.datetime.utcnow()
        }, merge=True)
        
        print('set task failed.')
    

if __name__ == '__main__':

    config = configparser.ConfigParser()
    config.read("tagengine.ini")
    
    project = config['DEFAULT']['PROJECT']
    region = config['DEFAULT']['REGION']
    queue_name = config['DEFAULT']['WORK_QUEUE']
    app_engine_uri = '/_run_task'
    tm = TaskManager(project, region, queue_name, app_engine_uri)
    
    job_uuid = '3e7218ac85d011ecae18ab89ce97b309'
    tag_uuid = '1f1b4720839c11eca541e1ad551502cb'
    uris = ['bigquery/project/warehouse-337221/dataset/austin_311_500/austin_311_service_requests_0',\
            'bigquery/project/warehouse-337221/dataset/austin_311_500/austin_311_service_requests_1',\
            'bigquery/project/warehouse-337221/dataset/austin_311_500/austin_311_service_requests_2']
    
    tm.create_work_tasks(job_uuid, tag_uuid, uris)
    
    print('done')



    

    

            
        
