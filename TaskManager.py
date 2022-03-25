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

import uuid, hashlib, datetime, json, configparser, math
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
        self.tasks_per_shard = 1000

##################### API METHODS #################
        
    def create_work_tasks(self, job_uuid, tag_uuid, uris):
        
        print('*** enter create_work_tasks ***')
        
        # create shards of 5000 records
        if len(uris) > self.tasks_per_shard:
            shards = math.ceil(len(uris) / self.tasks_per_shard)
        else:
            shards = 1
            
        task_running_total = 0
        task_counter = 0
        
        for shard_index in range(0, shards):
            
            shard_id_raw = job_uuid + str(shard_index)
            shard_uuid = hashlib.md5(shard_id_raw.encode()).hexdigest()
            self._create_shard(job_uuid, shard_uuid)

            for uri_index, uri_val in enumerate(uris[task_running_total:], task_running_total):

                # create task
                task_id_raw = job_uuid + uri_val
                task_id = hashlib.md5(task_id_raw.encode()).hexdigest()
            
                task_uuid = self._record_task(job_uuid, tag_uuid, uri_val, shard_uuid, task_id)
                self._create_task(job_uuid, tag_uuid, uri_val, task_id, shard_uuid, task_uuid)
                
                task_counter += 1
                task_running_total += 1
                
                if task_counter == self.tasks_per_shard:
                    self._update_shard_tasks(job_uuid, shard_uuid, task_counter)
                    task_counter = 0
                    break
        
        # update shard with last task_counter
        if task_counter > 0:    
            self._update_shard_tasks(job_uuid, shard_uuid, task_counter)
 
         
    def update_task_status(self, shard_uuid, task_uuid, status):     

        if status == 'RUNNING':
            self._set_task_running(shard_uuid, task_uuid)
            self._set_rollup_tasks_running(shard_uuid)
        
        if status == 'COMPLETED':
            self._set_task_completed(shard_uuid, task_uuid)
            self._set_rollup_tasks_completed(shard_uuid)
            
        if status == 'FAILED':
            self._set_task_failed(shard_uuid, task_uuid)
            self._set_rollup_tasks_failed(shard_uuid)


################ INTERNAL PROCESSING METHODS #################

    def _create_shard(self, job_uuid, shard_uuid):
        
        print('*** _create_shard ***')
        
        shard_ref = self.db.collection('shards').document(shard_uuid)
        
        shard_ref.set({
            'shard_uuid': shard_uuid,   
            'job_uuid': job_uuid,
            'tasks_ran': 0,
            'tasks_completed': 0,
            'tasks_failed': 0,
            'creation_time': datetime.datetime.utcnow()
        })
        
    
    def _update_shard_tasks(self, job_uuid, shard_uuid, task_counter):
        
        print('*** _update_shard ***')

        self.db.collection('shards').document(shard_uuid).update({'task_count': task_counter});
        

    def _record_task(self, job_uuid, tag_uuid, uri, shard_uuid, task_id):
        
        print('*** _record_task ***')
        
        task_uuid = uuid.uuid1().hex
        
        task_ref = self.db.collection('shards').document(shard_uuid).collection('tasks').document(task_uuid)

        task_ref.set({
            'task_uuid': task_uuid,     # task identifier in Firestore
            'task_id': task_id,         # cloud task identifier, based on uri
            'shard_uuid': shard_uuid,   # shard which this task belongs to
            'job_uuid': job_uuid,
            'tag_uuid': tag_uuid,
            'uri': uri,
            'status':  'PENDING',
            'creation_time': datetime.datetime.utcnow()
        })
        
        #print('created task record ' + task_uuid + ' in shard ' + shard_uuid)
         
        return task_uuid    
    
    
    def _create_task(self, job_uuid, tag_uuid, uri, task_id, shard_uuid, task_uuid):
        
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
        payload = {'job_uuid': job_uuid, 'tag_uuid': tag_uuid, 'uri': uri, 'shard_uuid': shard_uuid, 'task_uuid': task_uuid}
        print('payload: ' + str(payload))
        
        payload_utf8 = json.dumps(payload).encode()
        task['app_engine_http_request']['body'] = payload_utf8

        try:
            task = client.create_task(parent=parent, task=task)
        
        except:
            print('Error: could not create task for task_uuid ' + task_uuid)
            self._set_task_failed(shard_uuid, task_uuid)
            
        return task.name
        
    
    def _set_task_running(self, shard_uuid, task_uuid):
        
        print('*** _set_task_running ***')
        
        task_ref = self.db.collection('shards').document(shard_uuid).collection('tasks').document(task_uuid)
        
        task_ref.set({
            'status':  'RUNNING',
            'start_time': datetime.datetime.utcnow()
        }, merge=True)
        
        print('set task running.')
    
    
    def _set_rollup_tasks_running(self, shard_uuid):
        
        shard_ref = self.db.collection('shards').document(shard_uuid)
        shard_ref.update({'tasks_running': firestore.Increment(1)})
    
    
    def _set_task_completed(self, shard_uuid, task_uuid):
        
        print('*** _set_task_completed ***')
        
        task_ref = self.db.collection('shards').document(shard_uuid).collection('tasks').document(task_uuid)

        task_ref.set({
            'status':  'COMPLETED',
            'end_time': datetime.datetime.utcnow()
        }, merge=True)
        
        print('set task completed.')


    def _set_rollup_tasks_completed(self, shard_uuid):
        
        shard_ref = self.db.collection('shards').document(shard_uuid)
        shard_ref.update({'tasks_completed': firestore.Increment(1), 'tasks_running': firestore.Increment(-1)})
        
        
    def _set_task_failed(self, shard_uuid, task_uuid):
        
        print('*** _set_task_failed ***')
        
        task_ref = self.db.collection('shards').document(shard_uuid).collection('tasks').document(task_uuid)

        task_ref.set({
            'status':  'FAILED',
            'end_time': datetime.datetime.utcnow()
        }, merge=True)
        
        print('set task failed.')
        
    
    def _set_rollup_tasks_failed(self, shard_uuid):
        
        shard_ref = self.db.collection('shards').document(shard_uuid)
        shard_ref.update({'tasks_failed': firestore.Increment(1), 'tasks_running': firestore.Increment(-1)})
    
    

if __name__ == '__main__':

    config = configparser.ConfigParser()
    config.read("tagengine.ini")
    
    project = config['DEFAULT']['TAG_ENGINE_PROJECT']
    region = config['DEFAULT']['QUEUE_REGION']
    queue_name = config['DEFAULT']['WORK_QUEUE']
    app_engine_uri = '/_run_task'
    tm = TaskManager(project, region, queue_name, app_engine_uri)
    
    job_uuid = '3e7218ac85d011ecae18ab89ce97b309'
    tag_uuid = '1f1b4720839c11eca541e1ad551502cb'
    uris = ['bigquery/project/warehouse-337221/dataset/austin_311_500/austin_311_service_requests_0',\
            'bigquery/project/warehouse-337221/dataset/austin_311_500/austin_311_service_requests_1',\
            'bigquery/project/warehouse-337221/dataset/austin_311_500/austin_311_service_requests_2',
            'bigquery/project/warehouse-337221/dataset/austin_311_500/austin_311_service_requests_3',
            'bigquery/project/warehouse-337221/dataset/austin_311_500/austin_311_service_requests_4'
            'bigquery/project/warehouse-337221/dataset/austin_311_500/austin_311_service_requests_5']
    
    tm.create_work_tasks(job_uuid, tag_uuid, uris)
    
    print('done')



    

    

            
        
