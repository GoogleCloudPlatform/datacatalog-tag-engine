# Copyright 2021 Google LLC
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      https://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      https://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

from datetime import datetime, timedelta
import json
from airflow import models

from airflow.contrib.operators import bigquery_operator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryDeleteDatasetOperator)
from airflow.providers.http.operators.http import SimpleHttpOperator

from airflow.operators import email_operator
from airflow.operators import bash_operator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago

utcnow = datetime.utcnow()
run_date = utcnow.strftime('%Y-%m-%d-%H-%M-%S')

USER_EMAIL = 'scohen@google.com'
GCS_BUCKET = 'cold_retail_data'

INACTIVE_CUSTOMERS_SQL = '''create table retail_temp.inactive_customers as 
                           select distinct c_customer_sk from
                           (select * from retail_views.customer_orders_inactive 
                           except distinct 
                           select * from retail_views.customer_orders_active)'''
                           
INACTIVE_ONLINE_SALES_SQL = '''create table retail_temp.inactive_online_sales as 
                               select distinct cs_order_number from
                               (select * from retail_views.customer_orders_inactive 
                               except distinct 
                               select * from retail_views.customer_orders_active)'''

INACTIVE_CUSTOMERS_EXPORT_SQL = '''create or replace table retail_temp.inactive_customers_export as
	                            select * from retail.customer 
	                            where c_customer_sk in (select c_customer_sk from retail_temp.inactive_customers)''' 

	
INACTIVE_ONLINE_SALES_EXPORT_SQL = '''create or replace table retail_temp.inactive_online_sales_export as
	                              select * from retail.online_sales 
	                              where cs_order_number in (select cs_order_number from retail_temp.inactive_online_sales)''' 
                                  
                                  
EXTRACT_INACTIVE_CUSTOMERS_CMD = 'bq extract --destination_format=CSV --field_delimiter="," --print_header=true \
                                  retail_temp.inactive_customers_export \
                                  gs://{gcs_bucket}/inactive_customers/{run_date}-*.csv'.format(gcs_bucket=GCS_BUCKET, run_date=run_date)
                                  

EXTRACT_INACTIVE_ONLINE_SALES_CMD = 'bq extract --destination_format=CSV --field_delimiter="," --print_header=true \
                                    retail_temp.inactive_online_sales_export \
                                    gs://{gcs_bucket}/inactive_online_sales/{run_date}-*.csv'.format(gcs_bucket=GCS_BUCKET, run_date=run_date)


DELETE_INACTIVE_RECORDS_SQL = '''BEGIN
                                 BEGIN TRANSACTION;
                                 delete from retail.customer where c_customer_sk in (select c_customer_sk from retail_temp.inactive_customers);	
                                 delete from retail.online_sales where cs_order_number in (select cs_order_number from
                                                                                        retail_temp.inactive_online_sales);
                                 COMMIT TRANSACTION;
                                 EXCEPTION WHEN ERROR THEN
                                   SELECT @@error.message;
                                   ROLLBACK TRANSACTION;
                                 END'''

def report_failure(context):
  send_email = email_operator.EmailOperator(
      task_id="failure",
      to=USER_EMAIL,
      start_date=days_ago(1),
      subject='Inactive customers job failed'
  )

DEFAULT_DAG_ARGS = {
  'start_date': days_ago(1)
}

with models.DAG(dag_id='inactive_customers_dag',
                description='',
                schedule_interval=None, default_args=DEFAULT_DAG_ARGS) as dag:
                
  create_temp_dataset = BigQueryCreateEmptyDatasetOperator(
      task_id='create_temp_dataset', 
      dataset_id='retail_temp'
  )
  
  create_inactive_customers_table = bigquery_operator.BigQueryOperator(
      task_id='create_inactive_customers_table',
      sql=INACTIVE_CUSTOMERS_SQL,
      use_legacy_sql=False,
      on_failure_callback=report_failure
  )

  create_inactive_online_sales_table = bigquery_operator.BigQueryOperator(
      task_id='create_inactive_online_sales_table',
      sql=INACTIVE_ONLINE_SALES_SQL,
      use_legacy_sql=False,
      on_failure_callback=report_failure
  )
  
  create_inactive_customers_export = bigquery_operator.BigQueryOperator(
      task_id='create_inactive_customers_export',
      sql=INACTIVE_CUSTOMERS_EXPORT_SQL,
      use_legacy_sql=False,
      on_failure_callback=report_failure
  )
  
  create_inactive_online_sales_export = bigquery_operator.BigQueryOperator(
      task_id='create_inactive_online_sales_export',
      sql=INACTIVE_ONLINE_SALES_EXPORT_SQL,
      use_legacy_sql=False,
      on_failure_callback=report_failure
  )
  
  extract_inactive_customers = bash_operator.BashOperator(
      task_id='extract_inactive_customers', 
      bash_command=EXTRACT_INACTIVE_CUSTOMERS_CMD,
      on_failure_callback=report_failure
  )
      
  extract_inactive_online_sales = bash_operator.BashOperator(
      task_id='extract_inactive_online_sales', 
      bash_command=EXTRACT_INACTIVE_ONLINE_SALES_CMD,
      on_failure_callback=report_failure
  )
    
  delete_inactive_records = bigquery_operator.BigQueryOperator(
      task_id='delete_inactive_records',
      sql=DELETE_INACTIVE_RECORDS_SQL,
      use_legacy_sql=False,
      on_failure_callback=report_failure
   )    
          
  delete_temp_dataset = BigQueryDeleteDatasetOperator(
      task_id='delete_temp_dataset', 
      dataset_id='retail_temp', 
      delete_contents=True
  )
  
  update_dynamic_tags = SimpleHttpOperator(
      task_id='update_dynamic_tags',
      method='POST',
      data=json.dumps({'template_id': 'customer_lifecycle', 'project_id': 'tag-engine-283315', 'region': 'us-central1', 'included_uris_hash': \
                       'e4d12f3ec5a4b70c773f1e99220dbac5'}),
      endpoint='dynamic_ondemand_update'
  )
  
  start_parallel_tasks = DummyOperator(task_id='start_parallel_tasks',retries=0)
  end_parallel_tasks = DummyOperator(task_id='end_parallel_tasks',retries=0)
  
  create_temp_dataset >> start_parallel_tasks
  start_parallel_tasks >> create_inactive_customers_table >> create_inactive_customers_export >> extract_inactive_customers >> end_parallel_tasks
  start_parallel_tasks >> create_inactive_online_sales_table >> create_inactive_online_sales_export >> extract_inactive_online_sales \
  >> end_parallel_tasks
  end_parallel_tasks >> delete_inactive_records >> delete_temp_dataset >> update_dynamic_tags


