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
        operation = 'FIELD'
        prompt = download_prompt(FIELDS_PROMPT)

    elif 'JOIN' in operation.upper():
        operation = 'JOIN'
        prompt = download_prompt(JOINS_PROMPT)

    elif 'WHERE' in operation.upper():
        operation = 'WHERE'
        prompt = download_prompt(WHERES_PROMPT)

    elif 'GROUP_BY' in operation.upper():
        operation = 'GROUP_BY'
        prompt = download_prompt(GROUP_BYS_PROMPT)
        
    elif 'FUNCTION' in operation.upper():
        operation = 'FUNCTION'
        prompt = download_prompt(FUNCTIONS_PROMPT)
    
    else:
        prompt = None
    
    if prompt == None:
        error_message = 'Error: prompt is empty'
        print(error_message)
        return None, error_message
    else:
        raw_predictions = extract_sql(operation, prompt, bq, project, region, dataset, table, excluded_accounts)
    
    if len(raw_predictions) > 0:
        html_predictions = format_predictions(raw_predictions, operation)
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
    
    #print('extract_sql')
    
    raw_predictions = []
    
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
    sql += "AND destination_table.project_id is not null "
    sql += "AND statement_type != 'ALTER_TABLE'"
    
    if excluded_accounts:
        sql += " AND user_email not in ("
        
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
        print('raw prediction:', prediction)
        
        # remove false positives
        if 'oltp.WatchHistoryHistorical' in prediction:
            continue  
        else:
            
            raw_predictions.append(prediction)
            prediction_count += 1
        
        if prediction_count == 3:
            break
    
    return raw_predictions 
 
        
def format_predictions(raw_predictions, operation):
    
    print('format_predictions')
    print('input:', raw_predictions)
    print('count:', len(raw_predictions))
    
    filtered_predictions = []
    
    for raw in raw_predictions:
        #print('raw:', raw)
    
        line_splits = raw.split('\n')
        #print('line_splits:', line_splits)
        #print('count:', len(line_splits))
        
        count = 0
    
        for line_split in line_splits:
        
            count += 1
            
            if count < 3 and line_split != '' and line_split[0] == '|':
                continue
                        
            #print('line_split:', line_split)
        
            pipe_splits = line_split.split('|')
        
            for pipe_split in pipe_splits:
        
                token = pipe_split.strip()
                #print('raw token:', token)
                #print('filtered_predictions:', filtered_predictions)
                
                if token == '':
                    continue
                
                if token.isnumeric():
                    #print('token is numeric, skipping it')
                    continue
                    
                if token[0].isdigit():
                    token = token[2:].strip()
                    #print('removed digit from token')
                    #print('new token:', token)
                    
                if operation == 'JOIN' and len(filtered_predictions) > 0 and token.startswith('on '):
                    last_token = filtered_predictions[-1]
                    replaced_token = last_token + ' ' + token
                    filtered_predictions[-1] = replaced_token
                    #print('replaced last token', last_token, ' with replaced_token', replaced_token)
                
                else:
                    filtered_predictions.append(token)
                    #print('added token:', token)
    
    #print('filtered_predictions:', filtered_predictions)
    
    if len(filtered_predictions) == 0:
        return ''
    
    unique_predictions = set(filtered_predictions) 
    final_predictions = remove_false_positives(unique_predictions, operation)   
    
    if len(final_predictions) == 0:
        return 'None'
        
    html = '<html>'    
    html += '<br>'.join(final_predictions)
    html += '</html>'
    
    return html    


def remove_false_positives(predictions, operation):
    
    print('remove_false_positives')
    print('predictions:', predictions)
    
    if any(item == '' for item in predictions):
        predictions.remove('')
        
    if operation == 'FIELD':
        
        if any(item == 'frequency' for item in predictions):
            predictions.remove('frequency')
            
        if any(item == 'select clause' for item in predictions):
            predictions.remove('select clause')
            
    if operation == 'WHERE':
        
        if any(item == 'Where Clauses' for item in predictions):
            predictions.remove('Where Clauses')
            
        if any(item == 'Frequency' for item in predictions):
            predictions.remove('Frequency')
        
        if any(item == 'No WHERE clauses found' for item in predictions):
            predictions.remove('No WHERE clauses found')
            
        if any(item == 'Here are the most frequently occurring where clauses:' for item in predictions):
            predictions.remove('Here are the most frequently occurring where clauses:')  
        
    if operation == 'FUNCTION':
        
        if any(item == 'order by' for item in predictions):
            predictions.remove('order by') 
              
        if any(item == 'group by' for item in predictions):
            predictions.remove('group by')
            
        if any(item == 'Frequency' for item in predictions):
            predictions.remove('Frequency')
            
        if any(item == 'Function' for item in predictions):
            predictions.remove('Function')
        
        if any(item == 'where' for item in predictions):
            predictions.remove('where')    
    
    if operation == 'GROUP_BY':
        
        if any(item == 'Frequency' for item in predictions):
            predictions.remove('Frequency')
            
        if any(item == 'Group By Clause' for item in predictions):
            predictions.remove('Group By Clause') 
        
        if any(item == 'Rank' for item in predictions):
            predictions.remove('Rank')   
        
    if operation == 'JOIN':
        
        if any(item == 'FROM table_1 FULL JOIN table_2 ON table_1.column_name = table_2.column_name' for item in predictions):
            predictions.remove('FROM table_1 FULL JOIN table_2 ON table_1.column_name = table_2.column_name')

        if any(item == 'FROM table_1 JOIN table_2 ON table_1.column_name = table_2.column_name' for item in predictions):
            predictions.remove('FROM table_1 JOIN table_2 ON table_1.column_name = table_2.column_name')

        if any(item == 'FROM table_1 LEFT JOIN table_2 ON table_1.column_name = table_2.column_name' for item in predictions):
            predictions.remove('FROM table_1 LEFT JOIN table_2 ON table_1.column_name = table_2.column_name')
        
        if any(item == 'FROM table_1 RIGHT JOIN table_2 ON table_1.column_name = table_2.column_name' for item in predictions):
           predictions.remove('FROM table_1 RIGHT JOIN table_2 ON table_1.column_name = table_2.column_name')

        if any(item == 'No joins detected.' for item in predictions):
            predictions.remove('No joins detected.')
        
        if any(item == 'No joins were found in the query.' for item in predictions):
            predictions.remove('No joins were found in the query.')    
        
        if any(item == 'Here are the most frequently occurring joins:' for item in predictions):
            predictions.remove('Here are the most frequently occurring joins:') 
        
        if any(item == 'Here are the most frequent joins:' for item in predictions):
            predictions.remove('Here are the most frequent joins:')
          
        if any(item == 'The most frequently occurring join is "left outer join".' for item in predictions):
            predictions.remove('The most frequently occurring join is "left outer join".')
                
        if any(item == 'The most frequently occurring joins are:' for item in predictions):
            predictions.remove('The most frequently occurring joins are:')
            
        if any(item == 'The most frequent joins are:' for item in predictions):
            predictions.remove('The most frequent joins are:')
        
        if any(item == 'Join' for item in predictions):
            predictions.remove('Join')    
            
        if any(item == 'ON' for item in predictions):
            predictions.remove('ON')
            
        if any(item == 'FROM' for item in predictions):
            predictions.remove('FROM')
        
        if any(item == 'WHERE' for item in predictions):
            predictions.remove('WHERE')
            
        if any(item == '```' for item in predictions):
            predictions.remove('```')
            
        if any(item == 'JOIN' for item in predictions):
            predictions.remove('JOIN')
            
        if any(item == 'INNER JOIN' for item in predictions):
            predictions.remove('INNER JOIN')
        
        if any(item == 'LEFT JOIN' for item in predictions):
            predictions.remove('LEFT JOIN')
        
        if any(item == 'LEFT OUTER JOIN' for item in predictions):
            predictions.remove('LEFT OUTER JOIN')
            
        if any(item == 'RIGHT JOIN' for item in predictions):
            predictions.remove('RIGHT JOIN')
            
        if any(item == 'RIGHT OUTER JOIN' for item in predictions):
            predictions.remove('RIGHT OUTER JOIN')
            
        if any(item == 'FULL JOIN' for item in predictions):
            predictions.remove('FULL JOIN')
    
        if any(item == 'FULL OUTER JOIN' for item in predictions):
            predictions.remove('FULL OUTER JOIN')

        if any(item == 'CROSS JOIN' for item in predictions):
            predictions.remove('CROSS JOIN')
            
        if any(item == 'NATURAL JOIN' for item in predictions):
            predictions.remove('NATURAL JOIN')
        
        if any(item == 'NATURAL LEFT JOIN' for item in predictions):
            predictions.remove('NATURAL LEFT JOIN')

        if any(item == 'NATURAL RIGHT JOIN' for item in predictions):
            predictions.remove('NATURAL RIGHT JOIN')

        if any(item == 'NATURAL FULL JOIN' for item in predictions):
            predictions.remove('NATURAL FULL JOIN')

        if any(item == 'NATURAL LEFT OUTER JOIN' for item in predictions):
            predictions.remove('NATURAL LEFT OUTER JOIN')
            
        if any(item == 'NATURAL RIGHT OUTER JOIN' for item in predictions):
            predictions.remove('NATURAL RIGHT OUTER JOIN')
        
        if any(item == 'NATURAL FULL OUTER JOIN' for item in predictions):
            predictions.remove('NATURAL FULL OUTER JOIN')
            
        if any(item == '`left outer join`' for item in predictions):
            predictions.remove('`left outer join`')
        
        if any(item == '`join`' for item in predictions):
            predictions.remove('`join`')
        
        if any(item == '`full join`' for item in predictions):
            predictions.remove('`full join`')
        
        if any(item == '`cross join`' for item in predictions):
            predictions.remove('`cross join`')
        
        if any(item == '`right outer join`' for item in predictions):
            predictions.remove('`right outer join`')
        
        if any(item == '`using`' for item in predictions):
            predictions.remove('`using`')

    return predictions
    

if __name__ == "__main__":
    html_predictions, error_message = main('join', 'tag-engine-run-iap', 'us-central1', 'tickit', 'users') # group by, where, function, field
    print('html_predictions:', html_predictions)
    print('error_message:', error_message)