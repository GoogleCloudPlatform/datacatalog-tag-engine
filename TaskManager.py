# Copyright 2022-2023 Google, LLC.
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
from google.protobuf import duration_pb2

class TaskManager:
    """Class for creating and managing work requests in the form of cloud tasks 
    
    cloud_run_sa = Cloud Run service account
    project = Cloud Run project id (e.g. tag-engine-project)
    region = Cloud Run region (e.g. us-central1)
    queue_name = Cloud Task queue (e.g. tag-engine-queue)
    task_handler_uri = task handler uri in the Flask app hosted by Cloud Run 
    """
    def __init__(self,
                cloud_run_sa,
                tag_engine_project,
                queue_region,
                queue_name, 
                task_handler_uri):

        self.cloud_run_sa = cloud_run_sa
        self.tag_engine_project = tag_engine_project
        self.queue_region = queue_region
        self.queue_name = queue_name
        self.task_handler_uri = task_handler_uri

        self.db = firestore.Client()
        self.tasks_per_shard = 1000
        self.task_deadline = duration_pb2.Duration().FromSeconds(1800) # set the deadline per task to be 30 minutes (which is the max supported duration)

##################### API METHODS #################
        
    def create_config_uuid_tasks(self, tag_creator_account, tag_invoker_account, job_uuid, config_uuid, config_type, uris):
        
        # create shards of 1000 tasks
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

                # create the task
                if isinstance(uri_val, str):
                    task_id_raw = job_uuid + uri_val + str(datetime.datetime.utcnow())
                    
                if isinstance(uri_val, tuple):
                    task_id_raw = job_uuid + ''.join(uri_val) + str(datetime.datetime.utcnow()) # uri_val is a tuple when it contains a gcs path
                
                task_id = hashlib.md5(task_id_raw.encode()).hexdigest()
            
                task_uuid = self._record_config_uuid_task(job_uuid, shard_uuid, task_id, config_uuid, config_type, uri_val)
                self._create_config_uuid_task(tag_creator_account, tag_invoker_account, job_uuid, shard_uuid, task_uuid, task_id, config_uuid, config_type, uri_val)
                
                task_counter += 1
                task_running_total += 1
                
                if task_counter == self.tasks_per_shard:
                    self._update_shard_tasks(job_uuid, shard_uuid, task_counter)
                    task_counter = 0
                    break
        
        # update shard with last task_counter
        if task_counter > 0:    
            self._update_shard_tasks(job_uuid, shard_uuid, task_counter)

    
    def create_tag_extract_tasks(self, tag_creator_account, tag_invoker_account, job_uuid, config_uuid, config_type, tag_extract_list):
        
        # create shards of 5000 records
        if len(tag_extract_list) > self.tasks_per_shard:
            shards = math.ceil(len(tag_extract_list) / self.tasks_per_shard)
        else:
            shards = 1
            
        task_running_total = 0
        task_counter = 0
        
        for shard_index in range(0, shards):
            
            shard_id_raw = job_uuid + str(shard_index)
            shard_uuid = hashlib.md5(shard_id_raw.encode()).hexdigest()
            self._create_shard(job_uuid, shard_uuid)

            for extract_index, extract_val in enumerate(tag_extract_list[task_running_total:], task_running_total):
                
                print('task_running_total: ', task_running_total)
                print('extract_index: ', extract_index)
                print('extract_val: ', extract_val)
                
                # create the task
                task_id_raw = job_uuid + ''.join(str(extract_val)) + str(datetime.datetime.utcnow())
                task_id = hashlib.md5(task_id_raw.encode()).hexdigest()
                
                #print('task_id: ', task_id)

                task_uuid = self._record_tag_extract_task(job_uuid, shard_uuid, task_id, config_uuid, config_type, extract_val)
                self._create_tag_extract_task(tag_creator_account, tag_invoker_account, job_uuid, shard_uuid, task_uuid, task_id, config_uuid, config_type, extract_val)
                
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
        
        if status == 'SUCCESS':
            self._set_task_success(shard_uuid, task_uuid)
            self._set_rollup_tasks_success(shard_uuid)
            
        if status == 'ERROR':
            self._set_task_failed(shard_uuid, task_uuid)
            self._set_rollup_tasks_failed(shard_uuid)


################ INTERNAL PROCESSING METHODS #################

    def _create_shard(self, job_uuid, shard_uuid):
        
        print('*** _create_shard ***')
        print('job_uuid: ' + job_uuid + ', shard_uuid: ' + shard_uuid)
        
        shard_ref = self.db.collection('shards').document(shard_uuid)
        
        shard_ref.set({
            'shard_uuid': shard_uuid,   
            'job_uuid': job_uuid,
            'tasks_ran': 0,
            'tasks_success': 0,
            'tasks_failed': 0,
            'creation_time': datetime.datetime.utcnow()
        })
        
    
    def _update_shard_tasks(self, job_uuid, shard_uuid, task_counter):
        
        #print('*** _update_shard ***')

        self.db.collection('shards').document(shard_uuid).update({'task_count': task_counter});
        

    def _record_config_uuid_task(self, job_uuid, shard_uuid, task_id, config_uuid, config_type, uri):
        
        #print('*** _record_config_uuid_task ***')
        
        task_uuid = uuid.uuid1().hex
        
        task_ref = self.db.collection('shards').document(shard_uuid).collection('tasks').document(task_uuid)

        task_ref.set({
            'task_uuid': task_uuid,     # task identifier in Firestore
            'task_id': task_id,         # cloud task identifier, based on uri
            'shard_uuid': shard_uuid,   # shard which this task belongs to
            'job_uuid': job_uuid,
            'config_uuid': config_uuid,
            'config_type': config_type,
            'uri': uri,
            'status':  'PENDING',
            'creation_time': datetime.datetime.utcnow()
        })
        
        #print('created task record ' + task_uuid + ' in shard ' + shard_uuid)
         
        return task_uuid    
    
    
    def _record_tag_extract_task(self, job_uuid, shard_uuid, task_id, config_uuid, config_type, extract):
        
        print('*** _record_task ***')
        
        task_uuid = uuid.uuid1().hex
        
        task_ref = self.db.collection('shards').document(shard_uuid).collection('tasks').document(task_uuid)

        task_ref.set({
            'task_uuid': task_uuid,     # task identifier in Firestore
            'task_id': task_id,         # cloud task identifier, based on uri
            'shard_uuid': shard_uuid,   # shard which this task belongs to
            'job_uuid': job_uuid,
            'config_uuid': config_uuid,
            'config_type': config_type,
            'tag_extract': extract,
            'status':  'PENDING',
            'creation_time': datetime.datetime.utcnow()
        })
        
        #print('created task record ' + task_uuid + ' in shard ' + shard_uuid)
         
        return task_uuid
    
    
    def _create_config_uuid_task(self, tag_creator_account, tag_invoker_account, job_uuid, shard_uuid, task_uuid, task_id, \
                                 config_uuid, config_type, uri):
        
        success = True
        
        payload = {'job_uuid': job_uuid, 'shard_uuid': shard_uuid, 'task_uuid': task_uuid, 'config_uuid': config_uuid, \
                   'config_type': config_type, 'uri': uri, 'tag_creator_account': tag_creator_account, \
                   'tag_invoker_account': tag_invoker_account}
        
        client = tasks_v2.CloudTasksClient()
        parent = client.queue_path(self.tag_engine_project, self.queue_region, self.queue_name)
                
        task = {
            'name': parent + '/tasks/' + task_id,
            'dispatch_deadline': self.task_deadline,
            'http_request': {  
                'http_method': 'POST',
                'url': self.task_handler_uri,
                'headers': {'content-type': 'application/json'},
                'body': json.dumps(payload).encode(),
                'oidc_token': {'service_account_email': self.cloud_run_sa, 'audience': self.task_handler_uri}
            }
        }
        print('task request:', task)

        try:
            task = client.create_task(parent=parent, task=task)
            #print('task response:', task)
        
        except Exception as e:
            print('Error: could not create task for uri', self.task_handler_uri, '. Error: ', e)
            self._set_task_failed(shard_uuid, task_uuid)
            success = False
            
        return success
                  
        
    def _create_tag_extract_task(self, tag_creator_account, tag_invoker_account, job_uuid, shard_uuid, task_uuid, task_id, config_uuid, config_type, extract):
        
        success = True
    
        payload = {'job_uuid': job_uuid, 'shard_uuid': shard_uuid, 'task_uuid': task_uuid, 'config_uuid': config_uuid, \
                   'config_type': config_type, 'tag_extract': extract, 'tag_creator_account': tag_creator_account, \
                   'tag_invoker_account': tag_invoker_account}
        
        client = tasks_v2.CloudTasksClient()
        parent = client.queue_path(self.tag_engine_project, self.queue_region, self.queue_name)
        
        task = {
            'name': parent + '/tasks/' + task_id,
            'dispatch_deadline': self.task_deadline,
            'http_request': {  
                'http_method': 'POST',
                'url': self.task_handler_uri,
                'headers': {'content-type': 'application/json'},
                'body': json.dumps(payload).encode(),
                'oidc_token': {'service_account_email': self.cloud_run_sa, 'audience': self.task_handler_uri}
            }
        }
        
        print('task request:', task)

        try:
            task = client.create_task(parent=parent, task=task)
            print('task response:', task)
        
        except Exception as e:
            print('Error: could not create task for uri', self.task_handler_uri, '. Error: ', e)
            self._set_task_failed(shard_uuid, task_uuid)
            success = False
            
        return success
            
    
    def _set_task_running(self, shard_uuid, task_uuid):
        
        #print('*** _set_task_running ***')
        
        task_ref = self.db.collection('shards').document(shard_uuid).collection('tasks').document(task_uuid)
        
        task_ref.set({
            'status':  'RUNNING',
            'start_time': datetime.datetime.utcnow()
        }, merge=True)
        
        print('set task running.')
    
    
    def _set_rollup_tasks_running(self, shard_uuid):
        
        shard_ref = self.db.collection('shards').document(shard_uuid)
        shard_ref.update({'tasks_running': firestore.Increment(1)})
    
    
    def _set_task_success(self, shard_uuid, task_uuid):
        
        print('*** _set_task_success ***')
        
        task_ref = self.db.collection('shards').document(shard_uuid).collection('tasks').document(task_uuid)

        task_ref.set({
            'status':  'SUCCESS',
            'end_time': datetime.datetime.utcnow()
        }, merge=True)
        
        print('set task success.')


    def _set_rollup_tasks_success(self, shard_uuid):
        
        shard_ref = self.db.collection('shards').document(shard_uuid)
        shard_ref.update({'tasks_success': firestore.Increment(1), 'tasks_running': firestore.Increment(-1)})
        
        
    def _set_task_failed(self, shard_uuid, task_uuid):
        
        print('*** _set_task_failed ***')
        
        task_ref = self.db.collection('shards').document(shard_uuid).collection('tasks').document(task_uuid)

        task_ref.set({
            'status':  'ERROR',
            'end_time': datetime.datetime.utcnow()
        }, merge=True)
        
        print('set task failed.')
        
    
    def _set_rollup_tasks_failed(self, shard_uuid):
        
        shard_ref = self.db.collection('shards').document(shard_uuid)
        shard_ref.update({'tasks_failed': firestore.Increment(1), 'tasks_running': firestore.Increment(-1)})
    




    

    

            
        
