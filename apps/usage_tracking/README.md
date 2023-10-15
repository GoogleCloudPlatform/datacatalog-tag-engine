## Usage Tracking

This folder contains a series of cloud functions that generate an event click stream in BigQuery based on Data Catalog user activity:

1) `entry_views`: which users have viewed which entries in the catalog over time 
2) `tag_creates`: which users have created a tag on which entries in the catalog over time
3) `tag_updates`: which users have updated a tag on which entries in the catalog over time
4) `tag_deletes`: which users have deleted a tag on which entries in the catalog over time

The functions analyze the Data Catalog audit log entries which are synced to BigQuery. They map the entry ID to the resource name and output a summary for each event that is easier to consume. The cloud function is wrapped by a remote BigQuery function and runs on a daily schedule. 


### How to Deploy

#### Step 1: Enable audit logging 

#### Step 2: Create a log sync to BigQuery

#### Step 3: Create a service account for running the Cloud Functions

Create a service account for running these functions and grant this account the necessary permissions.   

```
gcloud iam service-accounts create query-cookbook

export QUERY_COOKBOOK_SA=query-cookbook@${TAG_ENGINE_PROJECT}.iam.gserviceaccount.com

gcloud projects add-iam-policy-binding $BIGQUERY_PROJECT \
    --member=serviceAccount:$QUERY_COOKBOOK_SA \
    --role=roles/bigquery.connectionUser

gcloud projects add-iam-policy-binding $BIGQUERY_PROJECT \
    --member=serviceAccount:$QUERY_COOKBOOK_SA \
    --role=roles/bigquery.dataViewer

gcloud projects add-iam-policy-binding $BIGQUERY_PROJECT \
    --member=serviceAccount:$QUERY_COOKBOOK_SA \
    --role=roles/bigquery.jobUser

gcloud projects add-iam-policy-binding $BIGQUERY_PROJECT \
    --member=serviceAccount:$QUERY_COOKBOOK_SA \
    --role=roles/bigquery.resourceViewer

gcloud projects add-iam-policy-binding $BIGQUERY_PROJECT \
    --member=serviceAccount:$QUERY_COOKBOOK_SA \
    --role=roles/storage.objectViewer
```

#### Step 4: Create a BigQuery cloud resource connection 

```
bq mk --connection --display_name='cloud function connection' --connection_type=CLOUD_RESOURCE \
	--project_id=PROJECT --location=REGION remote-connection

bq show --location=REGION --connection remote-connection
```

In the above command, replace PROJECT and REGION with your BigQuery project and region, respectively.  

The expected output from the `bq show` command contains a "serviceAccountId" property for the connection resource that starts with `bqcx-` and ends with `@gcp-sa-bigquery-condel.iam.gserviceaccount.com`. We'll refer to this service account in the following steps as `CONNECTION_SA`. Once the cloud functions are created (in the step 5), we'll need to assign the Cloud Functions Invoker role (`roles/cloudfunctions.invoker`) to the `CONNECTION_SA`. 

For more details on creating cloud resource connections, refer to the [product documentation](https://cloud.google.com/bigquery/docs/reference/standard-sql/remote-functions#sample_code). 


#### Step 5: Create the cloud functions

The `summarize_users` and `summarize_sql` remote functions in BigQuery both call a cloud function by the same name. 

The source for each one is contained in `main.py` under the `summarize_users` and `summarize_sql` folders. 

Open each `main.py and update the `GCS_BUCKET` variable on line 19 with your own bucket. 

Now we are ready to create the functions:

```
cd datacatalog-tag-engine/apps/query_cookbook

gcloud functions deploy summarize_users \
    --region=$BIGQUERY_REGION \
    --source=summarize_sql \
    --entry-point=event_handler \
    --runtime=python311 \
    --trigger-http \
    --service-account=$QUERY_COOKBOOK_SA \
    --timeout=540s \
    --no-allow-unauthenticated

gcloud functions deploy summarize_sql \
    --region=$BIGQUERY_REGION \
    --source=summarize_sql \
    --entry-point=event_handler \
    --runtime=python311 \
    --trigger-http \
    --service-account=$QUERY_COOKBOOK_SA \
    --timeout=540s \
    --no-allow-unauthenticated
```

#### Step 6: Assign the permissions to the CONNECTION_SA

```
gcloud functions add-iam-policy-binding summarize_sql \
   --member="${CONNECTION_SA}" \
   --role="roles/cloudfunctions.invoker" \
   --project="${TAG_ENGINE_PROJECT}"

gcloud functions add-iam-policy-binding summarize_users \
   --member="${CONNECTION_SA}" \
   --role="roles/cloudfunctions.invoker" \
   --project="${TAG_ENGINE_PROJECT}"
```


#### Step 7: Create the remote functions in BigQuery

Before creating the functions, we create the BigQuery dataset for holding them:

```
CREATE SCHEMA PROJECT:query_cookbook OPTIONS (location='REGION');
```

Replace PROJECT and REGION in the above command with the actual values of your BigQuery project and region. 


Create the `summarize_users` remote function:

```
CREATE OR REPLACE FUNCTION `PROJECT`.query_cookbook.summarize_users(project STRING, region STRING, dataset STRING, table STRING, 
      max_users INT64, excluded_accounts STRING) RETURNS STRING 
      REMOTE WITH CONNECTION `PROJECT.REGION.remote-connection` 
      OPTIONS 
      (endpoint = 'https://REGION-PROJECT.cloudfunctions.net/summarize_users');
```
Replace PROJECT and REGION in the above command with the actual values of your BigQuery project and region. 

The `max_users` parameter is the max number of user and service accounts to return from the function. 
Set it to the highest number of users you wish to include in a tag. 

The `excluded_accounts` parameter is a comma-separated list of user and service accounts to exclude or filter out. 
When passed in to the `top_queries`, it filters out any queries run from those accounts. 
When passed in to the `top_users` function, it excludes any top users that match the `excluded_accounts` value. 


Create the `summarize_sql` remote function:

```
CREATE OR REPLACE FUNCTION `PROJECT`.query_cookbook.summarize_sql(operation STRING, project STRING, region STRING, 
     dataset STRING, table STRING, excluded_accounts STRING) RETURNS STRING 
     REMOTE WITH CONNECTION `PROJECT.REGION.remote-connection` 
     OPTIONS 
     (endpoint = 'https://REGION-PROJECT.cloudfunctions.net/summarize_sql');
```

In the above commands, replace PROJECT and REGION with your BigQuery project and region, respectively. 


The `operation` parameter needs to be equal to one of 'fields', 'wheres', 'joins', 'group_bys', or 'functions'. 

The `excluded_accounts` parameter is a comma-separated list of user and service accounts to exclude or filter out. 


#### Step 8: Run a backfill


#### Step 9: Schedule the functions

