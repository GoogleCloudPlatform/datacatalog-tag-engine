## Usage Tracking

This folder contains a series of cloud functions that generate an event click stream in BigQuery based on Data Catalog user activity:

1) `entry_clicks`: which users have clicked on which entries in the catalog over time 
2) `tag_creates`: which users have created a tag on which entries in the catalog over time
3) `tag_updates`: which users have updated a tag on which entries in the catalog over time
4) `tag_deletes`: which users have deleted a tag on which entries in the catalog over time

The functions analyze the Data Catalog audit log entries which are synced to BigQuery. They map the entry ID to the resource name and output a summary for each event that is easier to consume. The cloud function is wrapped by a remote BigQuery function and runs on a daily schedule. 


### How to Deploy

#### Step 1: Enable audit logging 

From the GCP console, go to the IAM section and open the Audit Logs page. Search on `Service:Data Catalog` and enable Data Read and Data Write audit logs for Data Catalog.  

#### Step 2: Create a log sync to BigQuery

```
gcloud logging sinks create data-catalog-audit-sink bigquery.googleapis.com/projects/$BIGQUERY_PROJECT/datasets/$BIGQUERY_DATASET \
 	--log-filter='resource.type="protoPayload.serviceName="datacatalog.googleapis.com"'
```

#### Step 3: Create a Cloud Functions service account 

Designate a service account for running the functions and authorize the account to connect to the cloud functions, create the reporting tables, and run the queries in BigQuery.   

```
export SA="data-catalog-auditor@PROJECT.iam.gserviceaccount.com"
	
gcloud projects add-iam-policy-binding $BIGQUERY_PROJECT \
    --member=serviceAccount:$SA \
    --role=roles/bigquery.connectionUser

gcloud projects add-iam-policy-binding $BIGQUERY_PROJECT \
    --member=serviceAccount:$SA \
    --role=roles/bigquery.dataEditor

gcloud projects add-iam-policy-binding $BIGQUERY_PROJECT \
    --member=serviceAccount:$SA \
    --role=roles/bigquery.jobUser

gcloud projects add-iam-policy-binding $BIGQUERY_PROJECT \
    --member=serviceAccount:$SA \
    --role=roles/bigquery.resourceViewer

```

#### Step 4: Create a BigQuery cloud resource connection 

```
bq mk --connection --display_name='cloud function connection' --connection_type=CLOUD_RESOURCE \
	--project_id=$BIGQUERY_PROJECT --location=$BIGQUERY_REGION remote-connection

bq show --location=$BIGQUERY_REGION --connection remote-connection
```

The expected output from the `bq show` command contains a "serviceAccountId" property for the connection resource that starts with `bqcx-` and ends with `@gcp-sa-bigquery-condel.iam.gserviceaccount.com`. We'll refer to this service account in the following steps as `CONNECTION_SA`. 

Once the cloud functions are created, we'll need to assign the Cloud Functions Invoker role (`roles/cloudfunctions.invoker`) to the `CONNECTION_SA`. 

For more details on creating cloud resource connections, refer to the [product documentation](https://cloud.google.com/bigquery/docs/reference/standard-sql/remote-functions#sample_code). 


#### Step 5: Create the cloud functions

The remote functions in BigQuery each call a cloud function by the same name. 

The source for the cloud functions is split up into subfolders under `datacatalog-tag-engine/apps/usage_tracking` 

Create the cloud functions by running these commands:

```
cd datacatalog-tag-engine/apps/usage_tracking

gcloud functions deploy entry_clicks \
    --region=$BIGQUERY_REGION \
    --source=entry_clicks \
    --entry-point=event_handler \
    --gen2 \
    --runtime=python311 \
    --trigger-http \
    --service-account=$SA \
    --timeout=540s \
    --no-allow-unauthenticated

gcloud functions deploy tag_creates \
    --region=$BIGQUERY_REGION \
    --source=tag_creates \
    --entry-point=event_handler \
    --gen2 \
    --runtime=python311 \
    --trigger-http \
    --service-account=$SA \
    --timeout=540s \
    --no-allow-unauthenticated
	
gcloud functions deploy tag_deletes \
    --region=$BIGQUERY_REGION \
    --source=tag_creates \
    --entry-point=event_handler \
	--gen2 \
    --runtime=python311 \
    --trigger-http \
    --service-account=$SA \
    --timeout=540s \
    --no-allow-unauthenticated

gcloud functions deploy tag_updates \
    --region=$BIGQUERY_REGION \
    --source=tag_creates \
    --entry-point=event_handler \
	--gen2 \
    --runtime=python311 \
    --trigger-http \
    --service-account=$SA \
    --timeout=540s \
    --no-allow-unauthenticated	
```

#### Step 6: Grant `$CONNECTION_SA` permissions to run the functions

Note: `$CONNECTION_SA` is the account that runs the resource connection (which was created by the `bq mk` command):

```
gcloud functions add-iam-policy-binding entry_clicks \
   --member=serviceAccount:$CONNECTION_SA \
   --role="roles/cloudfunctions.invoker" \
   --project=$BIGQUERY_PROJECT
   
gcloud functions add-invoker-policy-binding entry_clicks --member=serviceAccount:$CONNECTION_SA

gcloud functions add-iam-policy-binding tag_creates \
   --member=serviceAccount:$CONNECTION_SA \
   --role="roles/cloudfunctions.invoker" \
   --project=$BIGQUERY_PROJECT
   
gcloud functions add-invoker-policy-binding tag_creates --member=serviceAccount:$CONNECTION_SA
   
gcloud functions add-iam-policy-binding tag_deletes \
   --member=serviceAccount:$CONNECTION_SA \
   --role="roles/cloudfunctions.invoker" \
   --project=$BIGQUERY_PROJECT

gcloud functions add-invoker-policy-binding tag_deletes --member=serviceAccount:$CONNECTION_SA
   
gcloud functions add-iam-policy-binding tag_updates \
   --member=serviceAccount:$CONNECTION_SA \
   --role="roles/cloudfunctions.invoker" \
   --project=$BIGQUERY_PROJECT
   
gcloud functions add-invoker-policy-binding tag_updates --member=serviceAccount:$CONNECTION_SA

```


#### Step 7: Create the remote functions in BigQuery

Create a BigQuery dataset for storing the remote functions:

```
CREATE SCHEMA `$BIGQUERY_PROJECT`.usage_tracking 
	OPTIONS (location='$BIGQUERY_REGION');
```

Create each remote function:

```
CREATE OR REPLACE FUNCTION `$BIGQUERY_PROJECT`.usage_tracking.entry_clicks(
		log_sync_project STRING, 
		log_sync_dataset STRING, 
		reporting_project STRING, 
		reporting_dataset STRING, 
		start_date DATE) RETURNS STRING 
      	REMOTE WITH CONNECTION `$BIGQUERY_PROJECT.$BIGQUERY_REGION.remote-connection` 
      	OPTIONS (endpoint = 'https://$BIGQUERY_REGION-$BIGQUERY_PROJECT.cloudfunctions.net/entry_clicks');


CREATE OR REPLACE FUNCTION `$BIGQUERY_PROJECT`.usage_tracking.tag_creates(
		log_sync_project STRING, 
		log_sync_dataset STRING, 
		reporting_project STRING, 
		reporting_dataset STRING, 
		start_date DATE) RETURNS STRING 
      	REMOTE WITH CONNECTION `$BIGQUERY_PROJECT.$BIGQUERY_REGION.remote-connection` 
      	OPTIONS (endpoint = 'https://$BIGQUERY_REGION-$BIGQUERY_PROJECT.cloudfunctions.net/tag_creates');


CREATE OR REPLACE FUNCTION `$BIGQUERY_PROJECT`.usage_tracking.tag_deletes(
		log_sync_project STRING, 
		log_sync_dataset STRING, 
		reporting_project STRING, 
		reporting_dataset STRING, 
		start_date DATE) RETURNS STRING 
      	REMOTE WITH CONNECTION `$BIGQUERY_PROJECT.$BIGQUERY_REGION.remote-connection` 
      	OPTIONS (endpoint = 'https://$BIGQUERY_REGION-$BIGQUERY_PROJECT.cloudfunctions.net/tag_deletes');


CREATE OR REPLACE FUNCTION `$BIGQUERY_PROJECT`.usage_tracking.tag_updates(
		log_sync_project STRING, 
		log_sync_dataset STRING, 
		reporting_project STRING, 
		reporting_dataset STRING, 
		start_date DATE) RETURNS STRING 
      	REMOTE WITH CONNECTION `$BIGQUERY_PROJECT.$BIGQUERY_REGION.remote-connection` 
      	OPTIONS (endpoint = 'https://$BIGQUERY_REGION-$BIGQUERY_PROJECT.cloudfunctions.net/tag_updates');
```

The parameter `start_date` indicates the oldest date from which to process the audit log entries. 
 

#### Step 8: Run a backfill to process all the log entries

Now call the remote functions, passing them your BQ project and dataset where the audit logs are stored, the BQ project and dataset where the reporting table should be stored, and NULL as the start date. When setting the start date to NULL, the functions process the audit log entries for all available dates in the audit log table in BigQuery.  

```
SELECT `tag-engine-develop`.usage_tracking.entry_clicks('tag-engine-develop', 'audit_logs', 'tag-engine-develop', 'reporting', NULL)
SELECT `tag-engine-develop`.usage_tracking.tag_creates('tag-engine-develop', 'audit_logs', 'tag-engine-develop', 'reporting', NULL)
SELECT `tag-engine-develop`.usage_tracking.tag_deletes('tag-engine-develop', 'audit_logs', 'tag-engine-develop', 'reporting', NULL)
SELECT `tag-engine-develop`.usage_tracking.tag_updates('tag-engine-develop', 'audit_logs', 'tag-engine-develop', 'reporting', NULL)
```


#### Step 9: Incremental updates to the reporting tables

To keep the reporting tables up-to-date, you can wrap the following queries into a scheduled query in BigQuery:

```
SELECT `$BIGQUERY_PROJECT`.usage_tracking.entry_clicks('$BIGQUERY_PROJECT', 'audit_logs', '$BIGQUERY_PROJECT', 'reporting', current_date()-1)
SELECT `$BIGQUERY_PROJECT`.usage_tracking.tag_creates('$BIGQUERY_PROJECT', 'audit_logs', '$BIGQUERY_PROJECT', 'reporting', current_date()-1)
SELECT `$BIGQUERY_PROJECT`.usage_tracking.tag_deletes('$BIGQUERY_PROJECT', 'audit_logs', '$BIGQUERY_PROJECT', 'reporting', current_date()-1)
SELECT `$BIGQUERY_PROJECT`.usage_tracking.tag_updates('$BIGQUERY_PROJECT', 'audit_logs', '$BIGQUERY_PROJECT', 'reporting', current_date()-1)
```

A start date of `current_date()-1` tells the function to process only the log entries available for yesterday. 

Here is the command to create the scheduled query for the entry clicks (and the same logic applies to the other functions):

```
bq query \
    --use_legacy_sql=false \
    --destination_table=reporting.scheduled_query_logs \
    --display_name='Daily Entry Clicks' \
    --schedule='every 24 hours' \
    --replace=true \
    'SELECT `$BIGQUERY_PROJECT`.usage_tracking.entry_clicks('$BIGQUERY_PROJECT', 'audit_logs', '$BIGQUERY_PROJECT', 'reporting', current_date()-1)'
```
