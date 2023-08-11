# Copyright 2023 Google, LLC.
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
from google.cloud import storage
from google.cloud import bigquery

GCS_BUCKET = 'query-cookbook-prompts'
FIELDS_PROMPT = 'fields_prompt.txt'
JOINS_PROMPT = 'joins_prompt.txt'
WHERES_PROMPT = 'wheres_prompt.txt'
GROUP_BYS_PROMPT = 'group_bys_prompt.txt'
FUNCTIONS_PROMPT = 'functions_prompt.txt'

def event_handler(request):
    
    request_json = request.get_json()
    operation = request_json['calls'][0][0].strip()
    project = request_json['calls'][0][1].strip()
    region = request_json['calls'][0][2].strip()
    dataset = request_json['calls'][0][3].strip()
    table = request_json['calls'][0][4].strip()

    if request_json['calls'][0][5]:
        excluded_accounts = request_json['calls'][0][5]
        print('excluded_accounts:', excluded_accounts)
    else:
        excluded_accounts = None
    
    try:
        html_predictions, error_message = main(operation, project, region, dataset, table, excluded_accounts)
        
        if error_message:
            return json.dumps({"replies": [error_message]})
        else:
            return json.dumps({"replies": [html_predictions]})
    
    except Exception as e:
        print("Exception caught: " + str(e))
        return json.dumps({"errorMessage": str(e)}), 400 


def main(operation, project, region, dataset, table, excluded_accounts=None):
    
    bq = bigquery.Client(location=region)
    
    if 'FIELD' in operation.upper():
        prompt = download_prompt(FIELDS_PROMPT)

    elif 'JOIN' in operation.upper():
        prompt = download_prompt(JOINS_PROMPT)

    elif 'WHERE' in operation.upper():
        prompt = download_prompt(WHERES_PROMPT)

    elif 'GROUP_BY' in operation.upper():
        prompt = download_prompt(GROUP_BYS_PROMPT)
        
    elif 'FUNCTION' in operation.upper():
        prompt = download_prompt(FUNCTIONS_PROMPT)
    
    else:
        prompt = None
    
    if prompt == None:
        error_message = 'Error: prompt is empty'
        print(error_message)
        return None, error_message
    else:
        unique_predictions = extract_sql(operation, prompt, bq, project, region, dataset, table, excluded_accounts)
    
    if len(unique_predictions) > 0:
        html_predictions = convert_to_html(unique_predictions)
        error_message = None
    else:
        error_message = 'Opps! no predictions found'
        print(error_message)
        html_predictions = None
    
    return html_predictions, error_message


def download_prompt(prompt_file):
    
    gcs_client = storage.Client()
    bucket = gcs_client.get_bucket(GCS_BUCKET)
    blob = bucket.get_blob(prompt_file)
    downloaded_prompt = blob.download_as_string().decode()
    
    return downloaded_prompt


def extract_sql(operation, prompt, bq, project, region, dataset, table, excluded_accounts=None):
    
    print('extract_sql')
    
    predictions = []
    
    sql = "SELECT * "
    sql += "FROM ML.GENERATE_TEXT("
    sql += "MODEL llm.model_v1, "
    sql += "("
    sql += "SELECT CONCAT('" + prompt + "', query) AS prompt " 
    sql += "FROM `" + project + "`.`region-" + region + "`.INFORMATION_SCHEMA.JOBS_BY_PROJECT "
    sql += "WHERE query like '%" + dataset + "." + table + "%' "
    sql += "AND query not like '%INFORMATION_SCHEMA.JOBS_BY_PROJECT%' "
    sql += "AND query not like '%summarize_sql%' "
    sql += "AND state = 'DONE' "
    sql += "AND error_result is null "
    
    if excluded_accounts:
        sql += " and user_email not in ("
        
        index = 0
        
        for account in excluded_accounts:
            
            if index > 0:
                sql += ","
            
            sql += "'" + account + "'"
            
            index += 1
            
        sql += ")"
    
    sql += "),"
    sql += "STRUCT("
    sql += "0 AS temperature, 1000 AS max_output_tokens, 0.0 AS top_p, 1 AS top_k))"
    
    print(sql)
    
    query_job = bq.query(sql)  
    rows = query_job.result()
    
    prediction_count = 0  
      
    for row in rows:
        
        text_result = row.ml_generate_text_result
        json_result = json.loads(text_result)
        prediction = json_result['predictions'][0]['content']
        #print('prediction:', prediction)
        
        # remove false positives
        if 'oltp.WatchHistoryHistorical' in prediction:
            continue  
        else:
            
            if 'FIELD' in operation.upper() and 'FROM ' in prediction.upper():
                end_index = prediction.upper().index('FROM ')
                prediction = prediction[0:end_index]
            
            if 'FIELD' in operation.upper():
                if 'SELECT *' in prediction.upper():
                    continue
            
            if 'FUNCTION' in operation.upper():
                if any(s in prediction.upper() for s in ('SELECT','FROM','ORDER','LIMIT','PRIMARY KEY', 'FOREIGN KEY')):
                    continue
                    
            if 'GROUP_BY' in operation.upper():
                if 'GROUP BY' not in prediction.upper():
                    continue

            predictions.append(prediction.replace('\n', ' ').strip())
            prediction_count += 1
        
        if prediction_count == 3:
            break
    
    unique_predictions = set(predictions)
    
    return unique_predictions 
 
        
def convert_to_html(predictions):
    
    html = '<html>'
    
    for prediction in predictions:
        
        if prediction == '':
            continue
        
        html += prediction + '<br>'
        print('html:', html)
        
    html = html[0:-4]
    html += '</html>'
    
    return html    


if __name__ == "__main__":
    html_predictions, error_message = main('group_by', 'tag-engine-run-iap', 'us-central1', 'tickit', 'sales')
    print('html_predictions:', html_predictions)
    print('error_message:', error_message)