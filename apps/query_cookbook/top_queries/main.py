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

import hashlib
import base64
import json

from google.cloud import bigquery

from sql_translator.sql_parser import *
from sql_translator.sql_rewrite import *
from sql_translator.rfmt import * 

import QueryEntry as qe

bq = bigquery.Client()

def event_handler(request):
    request_json = request.get_json()
    print('request_json:', request_json)
    
    project = request_json['calls'][0][0].strip()
    print('project:', project)
    
    dataset = request_json['calls'][0][1].strip()
    print('dataset:', dataset)
    
    table = request_json['calls'][0][2].strip()
    print('table:', table)
    
    region = request_json['calls'][0][3].strip()
    print('region:', region)
    
    max_queries = request_json['calls'][0][4]
    print('max_queries:', max_queries)
    
    if request_json['calls'][0][5]:
        excluded_accounts = request_json['calls'][0][5]
        print('excluded_accounts:', excluded_accounts)
    else:
        excluded_accounts = None
    
    try:
        query_entries_table = process_query_log(project, dataset, table, region, excluded_accounts)
        qe_html = format_top_queries(query_entries_table, max_queries)
        return json.dumps({"replies": [qe_html]})
    
    except Exception as e:
        print("Exception caught: " + str(e))
        return json.dumps({"errorMessage": str(e)}), 400 

    
def process_query_log(project, dataset, table, region, excluded_accounts=None):
    
    print('enter process_query_log')
    
    sql = "select query, start_time "
    sql += "from `" + project + "`.`region-" + region + "`.INFORMATION_SCHEMA.JOBS_BY_PROJECT, unnest(referenced_tables) as rf "
    sql += "where statement_type = 'SELECT' "
    sql += "and query not like '%INFORMATION_SCHEMA%' "
    sql += "and state = 'DONE' "
    sql += "and error_result is null "
    sql += "and rf.project_id = '" + project + "' "
    sql += "and rf.dataset_id = '" + dataset + "' "
    sql += "and rf.table_id = '" + table + "'" 
       
    if excluded_accounts:
        sql += " and user_email not in ("
        
        index = 0
        
        for account in excluded_accounts:
            
            if index > 0:
                sql += ","
            
            sql += "'" + account + "'"
            
            index += 1
            
        sql += ")"
        
    sql += " order by start_time desc"
    print(sql)
    
    query_job = bq.query(sql)  
    results = query_job.result()
    
    query_entries_table = {} # sql_hash -> QueryEntry
    
    for result in results:
        
        raw_query = result.query
        query = result.query.replace('\n', ' ')
        start_time = result.start_time
        
        try:
            parsed = parse(query)
            tree = parsed.get_tree()
            redacted_sql = parsed.rewrite_tree(redact_strings).rewrite_tree(redact_numbers).as_sql()
            print('redacted_sql:', redacted_sql)  
                      
        except Exception as e:
            #print('Error during SQL parsing: ' + query + '. Error from parser: ' + str(e))
            # if the parser can't parse the SQL, use the raw SQL
            redacted_sql = query
        
        sql_hash = hashlib.md5(redacted_sql.encode()).hexdigest()
        print('sql_hash:', sql_hash)
        
        # get execution count for this query
        escaped_query = raw_query.replace("'", "\\'").replace('\r', '').replace('\n', '')
        
        #print('escaped_query:', escaped_query)
        
        if escaped_query[-1] != ';':
            select_count = "select count(*) count from `" + project + "`.`region-" + region + "`.INFORMATION_SCHEMA.JOBS_BY_PROJECT "
            select_count += " where query = '" + escaped_query + "' or query ='" + escaped_query + ";'"
        else:
            select_count = "select count(*) count from `" + project + "`.`region-" + region + "`.INFORMATION_SCHEMA.JOBS_BY_PROJECT "
            select_count += " where query = '" + escaped_query + "'"
        
        print('select_count:', select_count)

        # run select stmt
        try:
            count_job = bq.query(select_count)  
            count_results = count_job.result()

            for count_result in count_results:
                execution_count = count_result.count
                print('execution_count:', execution_count)

            if sql_hash not in query_entries_table:
                qentry = qe.QueryEntry(redacted_sql, start_time, execution_count)
                query_entries_table[sql_hash] = qentry
                
        except Exception as e:
            print('Error: ' + str(e))
            if sql_hash not in query_entries_table:
                qentry = qe.QueryEntry(redacted_sql, start_time, execution_count=1)
                query_entries_table[sql_hash] = qentry
                
    return query_entries_table


def redact_strings(expr):
  if isinstance(expr, const.SQLString):
    return const.SQLString(value='x')
  return expr


def redact_numbers(expr):
  if isinstance(expr, const.SQLNumber):
    return const.SQLNumber(value='x')
  return expr


def format_top_queries(query_entries_table, max_queries):
    
    # sort the queries run against a table     
    sorted_queries = []
        
    for sql_hash in query_entries_table.keys():
        query_entry = query_entries_table[sql_hash]
        sorted_queries.append(query_entry)
        
    sorted_queries.sort(key=lambda x: x.execution_count, reverse=True)   
    
    query_count = 0
        
    # convert to html
    qe_html = '<html><table>'
        
    for query_entry in sorted_queries:
        qe_html += query_entry.format_html()
        query_count += 1
        
        #print(query_entry.to_string())
        
        if query_count == max_queries:
            break
        
    qe_html += '</table></html>'
    
    print('qe_html:', qe_html)
            
    return qe_html        