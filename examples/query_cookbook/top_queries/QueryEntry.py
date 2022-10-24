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

from datetime import datetime

class QueryEntry:

    def __init__(self, redacted_sql, start_time, execution_count):
        self.redacted_sql = redacted_sql.strip()
        self.last_run = start_time
        self.execution_count = execution_count
            
    def get_redacted_sql(self):
        return self.redacted_sql
                  
    def get_last_run(self):
        return self.last_run.strftime("%Y-%m-%d %H:%M:%S")
    
    def get_execution_count(self):
        return str(self.execution_count)
    
    def to_string(self):
        return 'redacted_sql: ' + self.get_redacted_sql() + ', last_run: ' + self.get_last_run() + ', execution_count: ' + self.get_execution_count()
    
    def format_html(self):
        
        uc_keywords = ['SELECT', 'FROM', 'WHERE', 'AND', 'JOIN', 'GROUP BY', 'HAVING', 'ORDER BY', 'LIMIT', 'WITH']
        lc_keywords = ['select ', ' from ', ' where ', ' and ', ' join ', ' group by ', ' order by ', ' limit ', 'with ']
        
        sql = self.get_redacted_sql()
        
        for keyword in uc_keywords:
            if keyword == 'SELECT':
                sql = sql.replace(keyword, '<font color="blue">' + keyword + '</font>')
            else:
                sql = sql.replace(keyword, '<br><font color="blue">' + keyword + '</font>')
                
        for keyword in lc_keywords:
            sql = sql.replace(keyword, '<font color="blue">' + keyword + '</font>')

        html = '<tr><td>' + sql + '</td></tr>'
        
        return html
        