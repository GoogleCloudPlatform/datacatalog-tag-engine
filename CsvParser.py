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

from google.cloud import storage
import csv

from common import log_error

class CsvParser:

    @staticmethod
    def extract_tags(credentials, csv_file):
        
        gcs_client = storage.Client(credentials=credentials)
        extracted_tags = [] # stores the result set

        # download CSV file from GCS
        bucket_name, file_path = csv_file
        bucket = gcs_client.get_bucket(bucket_name)
        blob = bucket.get_blob(file_path)
        
        file_splits = file_path.split('/')
        num_splits = len(file_splits)
        filename = file_splits[num_splits-1]
        
        tmp_file = '/tmp/' + filename
        
        try:
            blob.download_to_filename(filename=tmp_file)
        except Exception as e:
            msg = 'Could not download CSV {}'.format(tmp_file)
            log_error(msg, e)
            
        with open(tmp_file, 'r') as f:
            
            reader = csv.reader(f)
            
            for i, row in enumerate(reader):
                
                if i == 0:
                    header = row 
                else:
                    tag_extract = {}
                    
                    for j, val in enumerate(row):
                        
                        tag_extract[header[j]] = val.rstrip()
                    
                    extracted_tags.append(tag_extract)       
        
        return extracted_tags

if __name__ == '__main__':
    
    import google.auth, configparser
    from google.auth import impersonated_credentials
    SCOPES = ['openid', 'https://www.googleapis.com/auth/cloud-platform', 'https://www.googleapis.com/auth/userinfo.email']
    
    config = configparser.ConfigParser()
    config.read("tagengine.ini")
    
    source_credentials, _ = google.auth.default() 
    target_service_account = config['DEFAULT']['TAG_CREATOR_SA']
    
    credentials = impersonated_credentials.Credentials(source_credentials=source_credentials,
        target_principal=target_service_account,
        target_scopes=SCOPES,
        lifetime=1200)

    csv_file = ("tag-import", "csv/sakila_column_tags.csv")
    CsvParser.extract_tags(credentials, csv_file)
