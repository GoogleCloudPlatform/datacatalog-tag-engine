# Copyright 2023-2024 Google, LLC.
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
import json

def log_error(msg, error='', job_uuid=None):
    error = {'msg': msg, 'python_error': str(error), 'job_uuid': job_uuid}
    print(json.dumps(error)) 

def log_error_tag_dict(msg, error='', job_uuid=None, tag_dict=None):
    error = {'msg': msg, 'python_error': str(error), 'job_uuid': job_uuid, 'tag_dict': tag_dict}
    print(json.dumps(error)) 

def log_info(msg, job_uuid=None):
    info = {'msg': msg, 'job_uuid': job_uuid}
    print(json.dumps(info))
    
def log_info_tag_dict(msg, job_uuid=None, tag_dict=None):
    info = {'msg': msg, 'job_uuid': job_uuid, 'tag_dict': tag_dict}
    print(json.dumps(info))
 
    

