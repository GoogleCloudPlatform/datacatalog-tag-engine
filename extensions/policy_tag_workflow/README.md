## Policy Tag Workflow

This folder contains a cloud function that returns the policy tag that is attached to a column in a BigQuery table. The cloud function is wrapped by a remote BigQuery function so that it can be called by Tag Engine. The included Tag Engine configuration example (tag_engine_config.json)  shows how to call the cloud function. 

The workflow assumes that you already have a Data Catalog Taxonomy in place and policy tags created from this taxonomy. The workflow also assumes that you have attached the policy tags to the fields in your BigQuery tables which contain sensitive data. 

### How to Deploy

#### Step 1: Create the custom role PolicyTagReader

Create the PolicyTagReader customer role in the project in which your Data Catalog Policy Taxonomy exists. 

```
gcloud iam roles create PolicyTagReader \
	--project $TAXONOMY_PROJECT \
	--title PolicyTagReader \
	--description "Read Policy Tag Taxonomy" \
	--permissions datacatalog.taxonomies.get,datacatalog.taxonomies.list
```

#### Step 2: Set up a service account for running the cloud function

Create or designate a service account for running the cloud function. Be sure to authorize it to connect to the cloud function from BigQuery. 

```
export SA="cloud-function@PROJECT.iam.gserviceaccount.com"
	
gcloud projects add-iam-policy-binding $BIGQUERY_PROJECT \
    --member=serviceAccount:$SA \
    --role=roles/bigquery.connectionUser

gcloud projects add-iam-policy-binding $BIGQUERY_PROJECT \
    --member=serviceAccount:$SA \
    --role=roles/bigquery.dataViewer

gcloud projects add-iam-policy-binding $BIGQUERY_PROJECT \
    --member=serviceAccount:$SA \
    --role=roles/bigquery.jobUser

gcloud projects add-iam-policy-binding $BIGQUERY_PROJECT \
    --member=serviceAccount:$SA \
    --role=roles/bigquery.resourceViewer

gcloud projects add-iam-policy-binding $BIGQUERY_PROJECT \
    --member=serviceAccount:$SA \
    --role=projects/$TAXONOMY_PROJECT/roles/PolicyTagReader
```

#### Step 3: Create a BigQuery cloud resource connection 

```
bq mk --connection --display_name='cloud function connection' --connection_type=CLOUD_RESOURCE \
	--project_id=$BIGQUERY_PROJECT --location=$BIGQUERY_REGION remote-connection
```

The output should look like: `Connection 698093657015.us-central1.remote-connection successfully create`. 

```
bq show --location=$BIGQUERY_REGION --connection remote-connection
```

The output should include a `serviceAccountId`. Set an environment variable `CONNECTION_SA` to your `serviceAccountId` as we'll need to refer to it in a later step. 
 
```
export CONNECTION_SA=bqcx-698093657015-dehm@gcp-sa-bigquery-condel.iam.gserviceaccount.com
```

For more details on creating cloud resource connections, refer to the [product documentation](https://cloud.google.com/bigquery/docs/reference/standard-sql/remote-functions#sample_code). 


#### Step 4: Create the cloud function

```
cd datacatalog-tag-engine/extensions/policy_tag_workflow

gcloud services enable cloudfunctions.googleapis.com

gcloud functions deploy policy_tag_function \
    --region=$BIGQUERY_REGION \
    --source=policy_tag_function \
    --entry-point=event_handler \
    --gen2 \
    --runtime=python311 \
    --trigger-http \
    --service-account=$SA \
    --timeout=540s \
    --no-allow-unauthenticated
```

#### Step 5: Grant permissions to the SA to invoke the function

```
gcloud functions add-iam-policy-binding policy_tag_function \
   --member=serviceAccount:$CONNECTION_SA \
   --role="roles/cloudfunctions.invoker" \
   --project=$BIGQUERY_PROJECT
   
gcloud functions add-invoker-policy-binding policy_tag_function --member=serviceAccount:$CONNECTION_SA

gcloud run services get-iam-policy projects/$BIGQUERY_PROJECT/locations/$BIGQUERY_REGION/services/policy-tag-function
```

The output from the previous command should look similar to:

```
bindings:
- members:
  - serviceAccount:bqcx-698093657015-abcd@gcp-sa-bigquery-condel.iam.gserviceaccount.com
  role: roles/run.invoker
etag: BwYbB8lofco=
version: 1
```


#### Step 6: Create the remote function in BigQuery

Open BigQuery Studio and create the dataset and remote function with these commands:

```
CREATE SCHEMA remote_function 
	OPTIONS (location='us-central1');

CREATE OR REPLACE FUNCTION remote_function.policy_tag_function(
		project STRING, 
		region STRING, 
		dataset STRING, 
		table STRING, 
		column STRING) RETURNS STRING 
      	REMOTE WITH CONNECTION `$BIGQUERY_PROJECT.$BIGQUERY_REGION.remote-connection` 
      	OPTIONS (endpoint = 'https://$BIGQUERY_REGION-$BIGQUERY_PROJECT.cloudfunctions.net/policy_tag_function');
```


#### Step 7: Test the function

Open the BigQuery Studio and call the function on a column that has a policy tag. 

```
SELECT `tag-engine-run`.remote_function.policy_tag_function('tag-engine-run', 'us-central1', 'crm', 'InactCust', 'c_id')
```

Also, try calling it on a column without a policy tag. 

Once you have tested it, please refer to `config.json` to see how to call it from a Tag Engine configuration. 
