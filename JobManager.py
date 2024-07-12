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

import uuid, datetime, json, configparser
import constants
from google.cloud import firestore
from google.cloud.firestore_v1.base_query import FieldFilter
from google.cloud import tasks_v2
from google.api_core.client_info import ClientInfo

USER_AGENT = 'cloud-solutions/datacatalog-tag-engine-v2'

class JobManager:
    """Class for managing jobs for async task create and update requests
    
    cloud_run_sa = Cloud Run service account
    queue_project = Project where the queue is based (e.g. tag-engine-project)
    queue_region = Region where the queue is based (e.g. us-central1)
    queue_name = Name of the queue (e.g. tag-engine-queue)
    task_handler_uri = task handler uri in the Flask app hosted by Cloud Run 
    
    """
    def __init__(self,
                tag_engine_sa,
                tag_engine_project,
                tag_engine_region,
                tag_engine_queue, 
                task_handler_uri, 
                db_project,
                db_name):

        self.tag_engine_sa = tag_engine_sa
        self.tag_engine_project = tag_engine_project
        self.tag_engine_region = tag_engine_region
        self.tag_engine_queue = tag_engine_queue
        self.task_handler_uri = task_handler_uri
        
        self.db = firestore.Client(project=db_project, database=db_name, client_info=ClientInfo(user_agent=USER_AGENT))


##################### API METHODS #################

    def create_job(self, tag_creator_account, tag_invoker_account, config_uuid, config_type, metadata=None):
        
        job_uuid = self._create_job_record(config_uuid, config_type)
        
        if metadata != None:
            self._create_job_metadata_record(job_uuid, config_uuid, config_type, metadata)
        
        resp = self._create_job_task(tag_creator_account, tag_invoker_account, job_uuid, config_uuid, config_type)
                
        return job_uuid 
        
    
    def update_job_running(self, job_uuid):     

        #print('*** update_job_running ***')
        
        job_ref = self.db.collection('jobs').document(job_uuid)
        job_ref.update({'job_status': 'RUNNING'})
        
        print('Set job running.')

    
    def record_num_tasks(self, job_uuid, num_tasks):
        
        job_ref = self.db.collection('jobs').document(job_uuid)
        job_ref.update({'task_count': num_tasks})
        
        print('record_num_tasks')
        

    def calculate_job_completion(self, job_uuid):
        
        tasks_success = self._get_tasks_success(job_uuid)
        tasks_failed = self._get_tasks_failed(job_uuid)
              
        tasks_ran = tasks_success + tasks_failed
        
        print('tasks_success:', tasks_success)
        print('tasks_failed:', tasks_failed)
        print('tasks_ran:', tasks_ran)
        
        job_ref = self.db.collection('jobs').document(job_uuid)
        job = job_ref.get()

        if job.exists:
            
            job_dict = job.to_dict()
            task_count = job_dict['task_count']
            
            # job running
            if job_dict['task_count'] > tasks_ran:
                job_ref.update({
                    'tasks_ran': tasks_success + tasks_failed,
                    'job_status': 'RUNNING',
                    'tasks_success': tasks_success,
                    'tasks_failed': tasks_failed,
                })
                
                pct_complete = round(tasks_ran / task_count * 100, 2)
            
            # job completed
            if job_dict['task_count'] <= tasks_ran:
                
                if tasks_failed > 0:
                    
                    job_ref.update({
                        'tasks_ran': tasks_success + tasks_failed,
                        'tasks_success': tasks_success,
                        'tasks_failed': tasks_failed,
                        'job_status': 'ERROR',
                        'completion_time': datetime.datetime.utcnow()
                    })
                
                else:

                    job_ref.update({
                        'tasks_ran': tasks_success + tasks_failed,
                        'tasks_success': tasks_success,
                        'tasks_failed': tasks_failed,
                        'job_status': 'SUCCESS',
                        'completion_time': datetime.datetime.utcnow()
                    })
                
                pct_complete = 100     

        return tasks_success, tasks_failed, pct_complete
                  

    def get_job_status(self, job_uuid):
        
        job = self.db.collection('jobs').document(job_uuid).get()

        if job.exists:
            job_dict = job.to_dict()
            return job_dict


    def set_job_status(self, job_uuid, status):
        
        self.db.collection('jobs').document(job_uuid).update({
            'job_status': status
        })
            

################ INTERNAL PROCESSING METHODS #################

    def _create_job_record(self, config_uuid, config_type):
        
        print('*** _create_job_record ***')
        
        job_uuid = uuid.uuid1().hex

        job_ref = self.db.collection('jobs').document(job_uuid)

        job_ref.set({
            'job_uuid': job_uuid,
            'config_uuid': config_uuid,
            'config_type': config_type,
            'job_status':  'PENDING',
            'task_count': 0,
            'tasks_ran': 0,
            'tasks_success': 0,
            'tasks_failed': 0,
            'creation_time': datetime.datetime.utcnow()
        })
        
        print('Created job record.')
    
        return job_uuid

    
    def _create_job_task(self, tag_creator_account, tag_invoker_account, job_uuid, config_uuid, config_type):
                
        payload = {'job_uuid': job_uuid, 'config_uuid': config_uuid, 'config_type': config_type, \
                   'tag_creator_account': tag_creator_account, 'tag_invoker_account': tag_invoker_account}
        
        task = {
            'http_request': {  
                'http_method': 'POST',
                'url': self.task_handler_uri,
                'headers': {'content-type': 'application/json'},
                'body': json.dumps(payload).encode(),
                'oidc_token': {'service_account_email': self.tag_engine_sa, 'audience': self.task_handler_uri}
            }
        }
        
        print('task create:', task)
        
        client = tasks_v2.CloudTasksClient()
        parent = client.queue_path(self.tag_engine_project, self.tag_engine_region, self.tag_engine_queue)
        resp = client.create_task(parent=parent, task=task)
        print('task resp: ', resp)
        
        return resp
      
        
    def _get_task_count(job_uuid):
        
        job = self.db.collection('jobs').document(job_uuid).get()

        if job.exists:
            job_dict = job.to_dict()
            return job_dict['task_count']
        

    def _get_tasks_success(self, job_uuid):
        
        tasks_success = 0
        
        shards = self.db.collection('shards').where(filter=FieldFilter('job_uuid', '==', job_uuid)).stream()
        
        for shard in shards:
            tasks_success += shard.to_dict().get('tasks_success', 0) 
        
        return tasks_success
   
      
    def _get_tasks_failed(self, job_uuid):
       
       tasks_failed = 0
       
       shards = self.db.collection('shards').where(filter=FieldFilter('job_uuid', '==', job_uuid)).stream()
       
       for shard in shards:
           tasks_failed += shard.to_dict().get('tasks_failed', 0) 
       
       return tasks_failed


    def _create_job_metadata_record(self, job_uuid, config_uuid, config_type, metadata):
        
        print('*** _create_job_metadata_record ***')
        
        job_ref = self.db.collection('job_metadata').document(job_uuid)

        job_ref.set({
            'job_uuid': job_uuid,
            'config_uuid': config_uuid,
            'config_type': config_type,
            'metadata': metadata,
            'creation_time': datetime.datetime.utcnow()
        })
        
        print('Created job_metadata record.')
    
        
if __name__ == '__main__':

    config = configparser.ConfigParser()
    config.read("tagengine.ini")
    
    queue_project = config['DEFAULT']['QUEUE_PROJECT']
    queue_region = config['DEFAULT']['QUEUE_REGION']
    queue_name = config['DEFAULT']['INJECTOR_QUEUE']
    task_handler_uri = '/_split_work'
    db_project = config['DEFAULT']['FIRESTORE_PROJECT']
    db_name = config['DEFAULT']['FIRESTORE_DB']
    jm = JobManager(queue_project, queue_region, queue_name, task_handler_uri, db_project, db_name)
    
    config_uuid = '1f1b4720839c11eca541e1ad551502cb'
    jm.create_async_job(config_uuid)
    
    print('done')



    

    

            
        
