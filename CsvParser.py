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

from google.cloud import storage
import csv

class CsvParser:

    @staticmethod
    def extract_tags(credentials, csv_file):
        
        gcs_client = storage.Client(credentials=credentials)
        extracted_tags = [] # stores the result set

        # download CSV file from GCS
        bucket_name, filename = csv_file
        bucket = gcs_client.get_bucket(bucket_name)
        blob = bucket.get_blob(filename)
        
        tmp_file = '/tmp/' + filename
        blob.download_to_filename(filename=tmp_file)
        
        with open(tmp_file, 'r') as f:
            
            reader = csv.reader(f)
            
            for i, row in enumerate(reader):
                
                if i == 0:
                    header = row 
                else:
                    tag_extract = {}
                    
                    for j, val in enumerate(row):
                        
                        if val != '':
                            tag_extract[header[j]] = val.rstrip()
                    
                    extracted_tags.append(tag_extract)       
        
        return extracted_tags

        