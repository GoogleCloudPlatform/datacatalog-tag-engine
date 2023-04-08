## Query Cookbook Overview

This folder contains a workflow that creates and populates the Query Cookbook tags on all Data Catalog entities representing BigQuery tables or views which have been recently queried.  

For each BigQuery table or view, the Query Cookbook workflow computes these tag fields: 
1) `top_queries`: the most frequent SQL queries which reference the BigQuery entity in question. 
2) `top_users`: the user or service accounts with the highest number of queries on the BigQuery entity in question.  

The workflow creates one tag per BigQuery entity in Data Catalog which has been queried over the last 180 days. BigQuery entities which have not been referenced by any queries will not be tagged. 

### Dependencies

In order to implement this workflow, you must have a running instance of Tag Engine. Moreover, your instance must be running Tag Engine version 1.0.5 or higher. You can check your version by opening the Tag Engine UI or by calling the `[TAG_ENGINE_URL]/version` endpoint. 

If you are new to Tag Engine, start with [this tutorial](https://cloud.google.com/architecture/tag-engine-and-data-catalog). You can follow [this README](https://github.com/GoogleCloudPlatform/datacatalog-tag-engine/blob/main/README.md) to deploy Tag Engine on Google Cloud Platform. 


### Deployment Procedure

The following procedure assumes that you have deployed Tag Engine in your Google Cloud project. 


#### Step 1: Create the tag template

This workflow makes use of the [Query Cookbook tag template](https://github.com/GoogleCloudPlatform/datacatalog-templates/blob/master/query_cookbook.yaml):

```
template:
- name: query_cookbook
  display_name: "Query Cookbook Template"
  public: False
  fields:
    - field: top_users
      type: richtext
      display: "Top users"
      description: "Top users who have queried this data asset in the past 180 days"
      required: true
      order: 1
    - field: top_queries
      type: richtext
      display: "Top queries"
      description: "Top queries run against this data asset in the past 180 days"
      required: true
      order: 0
```

To create the template, clone the [tag template repository](https://github.com/GoogleCloudPlatform/datacatalog-templates.git) and run the [create_template.py](https://github.com/GoogleCloudPlatform/datacatalog-templates/blob/master/create_template.py) script as follows:

```
git clone https://github.com/GoogleCloudPlatform/datacatalog-templates.git

python create_template.py [PROJECT] [REGION] query_cookbook.yaml

```

In the above command, replace [PROJECT] and [REGION] with your BigQuery project and region, respectively.  


#### Step 2: Assign IAM role to Tag Engine service account

The Query Cookbook functions query the `INFORMATION_SCHEMA.JOBS` view to retrieve the query logs. In order to query this view, you'll need to grant the BigQuery Resource Viewer role (`roles/bigquery.resourceViewer`) to your Tag Engine service account. 

The Tag Engine service account is the default App Engine service account unless you specified a user-managed service account when deploying Tag Engine. 

For more details on the `INFORMATION_SCHEMA.JOBS` view, refer to the [product documentation](https://cloud.google.com/bigquery/docs/information-schema-jobs). 


#### Step 3: Create the BigQuery cloud resource connection 

```
bq mk --connection --display_name='cloud function connection' --connection_type=CLOUD_RESOURCE \
	--project_id=[PROJECT] --location=[REGION] cloud-function-connection

bq show --location=[REGION] --connection cloud-function-connection
```

In the above command, replace [PROJECT] and [REGION] with your BigQuery project and region, respectively.  

The expected output from the `bq show` command contains a "serviceAccountId" property for the created connection. Assign the Cloud Functions Invoker role (`roles/cloudfunctions.invoker`) to this service account. 

For more details on creating cloud resource connections, refer to the [product documentation](https://cloud.google.com/bigquery/docs/reference/standard-sql/remote-functions#sample_code). 


#### Step 4: Create the Cloud Functions

Create the `top_queries` function in Cloud Functions: 

```
cd datacatalog-tag-engine/examples/query_cookbook/top_queries

gcloud functions deploy top_queries \
    --region=[REGION] \
    --source=archive.zip \
    --entry-point=event_handler \
    --runtime=python310 \
    --trigger-http \
    --service-account=$TAG_ENGINE_SA 
```

Create the `top_users` cloud function: 

```
cd datacatalog-tag-engine/examples/query_cookbook/top_users

gcloud functions deploy top_users \
    --region=[REGION] \
    --entry-point=event_handler \
    --runtime=python310 \
    --trigger-http \
    --service-account=[TAG_ENGINE_SA] 
```

In the above commands, replace [REGION] and [TAG_ENGINE_SA] with your BigQuery region and 
Tag Engine service account, respectively.  


#### Step 5: Create the remote functions in BigQuery


Create the BigQuery dataset for holding the two remote functions:

```
create schema [PROJECT]:remote_functions options (location='[REGION]');

```

Create the `top_queries` remote function:

```
create or replace function `[PROJECT]`.remote_functions.top_queries(project string, 
	dataset string, table string, region string, max_queries int64, excluded_accounts string) returns string
	remote with connection `[PROJECT].[REGION].cloud-function-connection`
	options (endpoint = 'https://[REGION]-[PROJECT].cloudfunctions.net/top_queries')
```

Create the `top_users` remote function:

```
create or replace function `[PROJECT]`.remote_functions.top_users(project string, 
	dataset string, table string, region string, max_users int64, excluded_accounts string) returns string
	remote with connection `[PROJECT].[REGION].cloud-function-connection`
	options (endpoint = 'https://[REGION]-[PROJECT].cloudfunctions.net/top_users')
```

In the above commands, replace [PROJECT] and [REGION] with your BigQuery project and region, respectively. 


The `max_queries` parameter is the max number of SQL queries returned from the function. 
Set it to the highest number of queries that you wish to include in a tag. 

The `max_users` parameter is the max number of user and service accounts to return from the function. 
Set it to the highest number of users you wish to include in a tag. 

The `excluded_accounts` parameter is a comma-separated list of user and service accounts to exclude or filter out. 
When passed in to the `top_queries`, it filters out any queries run from those accounts. 
When passed in to the `top_users` function, it excludes any top users that match the `excluded_accounts` value. 


#### Step 6: Test the remote BigQuery functions

Test both functions on a table that has been queried frequently over the past 180 days. 

```
select `[PROJECT]`.remote_functions.top_queries('[PROJECT]', '[DATASET]', '[TABLE]', '[REGION]', 6, NULL);

select `[PROJECT]`.remote_functions.top_users('[PROJECT]', '[DATASET]', '[TABLE]', '[REGION]', 6, NULL);
```

If you do not see the expected output, consult the Cloud Function logs for errors.


#### Step 7: Create the Tag Engine config

```
cd datacatalog-tag-engine/examples/query_cookbook
```

Open `query_cookbook_config.json` and update the `"template_project"` and `"template_region"` fields to 
the project and region in which you created the Query Cookbook tag template. 

Replace the references to `tag-engine-develop` in the query expressions with your BigQuery project. 
Replace the `"included_tables_uris"` value with the paths to your BigQuery datasets. 

Create the Tag Engine dynamic tag config with your edited json file:

```
curl -X POST [TAG_ENGINE_URL]/dynamic_table_tags -d @query_cookbook_config.json
``` 

The above command creates and populates the Query Cookbook tags in Data Catalog. 
The config also tells Tag Engine to update the tags every 24 hours using the `"refresh_mode"` field.  

Wait a few minutes and open the Data Catalog UI to see the resulting tags. 

If you don't see the expected out, consult the App Engine logs for errors:<br>

`gcloud app logs tail -s default`

