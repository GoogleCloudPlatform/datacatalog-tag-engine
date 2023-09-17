## Query Cookbook Workflow

This folder contains a workflow that computes access patterns for data assets in BigQuery and produces metadata tags in Data Catalog with the results. The workflow calls the `ML.GENERATE_TEXT` function in BigQuery, which uses the Vertex AI text-bison large language model (LLM) for inferences. More details on `ML.GENERATE_TEXT` are available in the [product documentation](https://cloud.google.com/bigquery/docs/generate-text). 

For each table or view in BigQuery, the Query Cookbook workflow computes a metadata tag with these fields: 
1) `top_users`: Most active users who have queried this data asset  
2) `top_fields`: Most commonly selected fields on this data asset
3) `top_wheres`: Most common where clauses on this data asset
4) `top_joins`: Most common joins on this data asset
5) `top_groupbys`: Most common group by clauses on this data asset
6) `top_functions`: Most common functions run on this data asset

The workflow extracts the query logs from the [INFORMATION_SCHEMA.JOBS](https://cloud.google.com/bigquery/docs/information-schema-jobs) view and summarizes their contents by calling Vertex AI's [text-bison](https://cloud.google.com/vertex-ai/docs/generative-ai/model-reference/text) LLM. This logic is wrapped into a cloud function, which is called by a BigQuery remote function.  


### Dependencies

In order to deploy this workflow, you must have a running instance of Tag Engine v2. Make sure you're running on 2.1.1 or higher. You can check your version from the home page of the UI or by calling the `[TAG_ENGINE_URL]/version` endpoint. To deploy Tag Engine, refer to this [guide](https://github.com/GoogleCloudPlatform/datacatalog-tag-engine/blob/cloud-run/README.md#deploy). 


### How to Deploy

The following procedure assumes that you have deployed Tag Engine in your Google Cloud project. 


#### Step 1: Create the Query Cookbook tag template

This workflow makes use of the Query Cookbook [tag template](https://github.com/GoogleCloudPlatform/datacatalog-templates/blob/master/query_cookbook.yaml). 

To create the template, clone the [tag template repository](https://github.com/GoogleCloudPlatform/datacatalog-templates.git) and run the [create_template.py](https://github.com/GoogleCloudPlatform/datacatalog-templates/blob/master/create_template.py) script as follows:

```
git clone https://github.com/GoogleCloudPlatform/datacatalog-templates.git

python create_template.py PROJECT REGION query_cookbook.yaml
```

In the above command, replace PROJECT and REGION with your BigQuery project and region, respectively.  


#### Step 2: Create the service account 

The Query Cookbook workflow comes with two BigQuery remote functions, one for retrieving the top users, which is called `summarize_users`, and the other for retrieving the top sql, which is called `summarize_sql`. 

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


#### Step 3: Create the BigQuery cloud resource connection 

```
bq mk --connection --display_name='cloud function connection' --connection_type=CLOUD_RESOURCE \
	--project_id=PROJECT --location=REGION remote-connection

bq show --location=REGION --connection remote-connection
```

In the above command, replace PROJECT and REGION with your BigQuery project and region, respectively.  

The expected output from the `bq show` command contains a "serviceAccountId" property for the connection resource that starts with `bqcx-` and ends with `@gcp-sa-bigquery-condel.iam.gserviceaccount.com`. We'll refer to this service account in the following steps as `CONNECTION_SA`. Once the cloud functions are created (in the step 5), we'll need to assign the Cloud Functions Invoker role (`roles/cloudfunctions.invoker`) to the `CONNECTION_SA`. 

For more details on creating cloud resource connections, refer to the [product documentation](https://cloud.google.com/bigquery/docs/reference/standard-sql/remote-functions#sample_code). 


#### Step 4: Create the ML model

```
CREATE SCHEMA llm OPTIONS (location = 'us-central1');

CREATE OR REPLACE MODEL llm.model_v1
    REMOTE WITH CONNECTION `PROJECT.REGION.remote_connection`
    OPTIONS (REMOTE_SERVICE_TYPE = 'CLOUD_AI_LARGE_LANGUAGE_MODEL_V1');
```

#### Step 5: Copy the prompts to Google Cloud Storage

The `summarize_sql` function passes a prompt when it calls the Vertex AI [text-bison](https://cloud.google.com/vertex-ai/docs/generative-ai/model-reference/text) LLM. Each SQL operation being summarized has a different prompt (select, join, where, group by, and functions) and each prompt is kept in its own text file. 

Review the prompts located at `summarize_sql/*_prompt.txt and customize them to your needs. 

Make a bucket in Google Cloud Storage and copy the files into it: 

```
gsutil mb -c standard -l REGION gs://BUCKET
gsutil cp summarize_sql/*_prompt.txt BUCKET
```

Replace REGION and BUCKET in the above commands with their actual values. 


#### Step 6: Create the cloud functions

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

#### Step 7: Assign the permissions to the CONNECTION_SA

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


#### Step 8: Create the remote functions in BigQuery

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


#### Step 9: Test the remote BigQuery functions

Test both functions on a table that has been queried over the past 180 days. 

```
select `PROJECT`.query_cookbook.summarize_users('PROJECT', 'DATASET', 'TABLE', 'REGION', 3, NULL);

select `PROJECT`.query_cookbook.summarize_sql('fields', 'PROJECT', 'REGION', 'DATASET', 'TABLE', NULL);
select `PROJECT`.query_cookbook.summarize_sql('wheres', 'PROJECT', 'REGION', 'DATASET', 'TABLE', NULL);
select `PROJECT`.query_cookbook.summarize_sql('joins', 'PROJECT', 'REGION', 'DATASET', 'TABLE', NULL);
select `PROJECT`.query_cookbook.summarize_sql('group_bys', 'PROJECT', 'REGION', 'DATASET', 'TABLE', NULL);
select `PROJECT`.query_cookbook.summarize_sql('functions', 'PROJECT', 'REGION', 'DATASET', 'TABLE', NULL);
```

If you do not see the expected output, consult the Cloud Function logs for errors.


#### Step 10: Raise the Tag Engine Cloud Run service's execution and memory limits

The integration with Vertex AI in the `summarize_sql` cloud function increases the execution time of the function. To avoid timeouts, we raise the Cloud Run [request timeout](https://cloud.google.com/run/docs/configuring/request-timeout) from 5 minutes to 60 minutes:

```
gcloud run services update tag-engine-api --timeout=60m
```

You'll want to run the same command on `tag-engine-ui` if you are planning to trigger the job from the Tag Engine UI. 


In addition, raise the memory limit of the Cloud Run service:

```
gcloud run services update tag-engine-api --memory 4G
```

Again, you'll want to run the same command on `tag-engine-ui` if you are planning to trigger the job from the Tag Engine UI. 


#### Step 10: Create the Tag Engine dynamic tag config

a) Set environment variables:

```
export TAG_ENGINE_URL=`gcloud run services describe tag-engine-api --format="value(status.url)"`
export GOOGLE_APPLICATION_CREDENTIALS="cloud-run-sa-private_key.json" 

export IAM_TOKEN=$(gcloud auth print-identity-token)
export OAUTH_TOKEN=$(gcloud auth application-default print-access-token)
```

b) Open `query_cookbook_config.json` and update the `"template_project"` and `"template_region"` fields to 
the project and region in which you created the Query Cookbook tag template. 

Replace the project references to `tag-engine-run-iap` in the query expressions with your BigQuery project. 
Replace the `included_tables_uris` value with the complete paths to your BigQuery datasets. 


c) Create the Tag Engine dynamic tag config from your edited json file:

```
curl -X POST $TAG_ENGINE_URL/create_dynamic_table_config -d @query_cookbook_config.json \
	-H "Authorization: Bearer $IAM_TOKEN" \
	-H "oauth_token: $OAUTH_TOKEN"
``` 

The output from this command should look like this:

```
{"config_type":"DYNAMIC_TAG_TABLE","config_uuid":"162ba61834ae11eea3b32b4db8329d44"}
```

d) Trigger the job to create the Data Catalog tags:

```
curl -i -X POST $TAG_ENGINE_URL/trigger_job \
  -d '{"config_type":"DYNAMIC_TAG_TABLE","config_uuid":"162ba61834ae11eea3b32b4db8329d44"}' \
  -H "Authorization: Bearer $IAM_TOKEN" \
  -H "oauth_token: $OAUTH_TOKEN"
```

Replace the `config_uuid` with your value in the above command. 

The output from this command should look like this:

```
{"job_uuid":"1e83cfe834ae11eea3b32b4db8329d44"}
```

e) Check the job status:

```
curl -X POST $TAG_ENGINE_URL/get_job_status -d '{"job_uuid":"1e83cfe834ae11eea3b32b4db8329d44"}' \
     -H "Authorization: Bearer $IAM_TOKEN" \
     -H "oauth_token: $OAUTH_TOKEN"
```

The output should look like:

```
{"job_status":"RUNNING","task_count":4,"tasks_completed":0,"tasks_failed":0,"tasks_ran":0}
```

You can keep polling `get_job_status` until the job finishes. 

Once the job completes, the output changes to:

```
{"job_status":"SUCCESS","task_count":4,"tasks_completed":4,"tasks_failed":0,"tasks_ran":4}
```

Open the Data Catalog UI to see the resulting tags. If the job errors out, consult the Cloud Run log for details<br>


