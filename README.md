## Tag Engine 2.0
This branch contains the Tag Engine 2.0 application, an early release of Tag Engine v2 that is hosted on Cloud Run (instead of App Engine) and is [VPC-SC compliant](https://cloud.google.com/vpc-service-controls/docs/supported-products). Tag Engine 2.0 supports authentication and the ability for multiple teams to securely tag their own data assets. 

Tag Engine is an open-source extension to Google Cloud's Data Catalog. Tag Engine automates the tagging of BigQuery tables and views as well as data lake files in Cloud Storage. You create a configuration, which contains SQL expressions that define how to populate the fields in the tags. Tag Engine runs the configuration either on demand or on a pre-defined schedule.

If you are new to Tag Engine, you may also want to walk through [this tutorial](https://cloud.google.com/architecture/tag-engine-and-data-catalog). Note that the tutorial is for Tag Engine v1 (as opposed to v2), but it will give you a sense of how Tag Engine works. We plan to publish a second tutorial for Tag Engine v2 soon. Stay tuned! 

This guide has three parts: [deployment procedure](#setup), [test procedure with user account](#testa), [test procedure with service account](#testb), and [next steps](#next). 

### <a name="setup"></a> Deployment Procedure

1. Create (or designate) three service accounts:

- A service account that runs the Tag Engine Cloud Run service, referred to below as `CLOUD_RUN_SA`. 
- A service account that performs the tagging in Data Catalog, and sourcing the contents of those tags from BigQuery, referred to below as `TAG_CREATOR_SA`. 
- A service account that interacts with the Tag Engine API, referred to below as `CLIENT_SA`. 


2. Set six environment variables which will be used throughout the setup procedure:

```
export TAG_ENGINE_PROJECT="<PROJECT>"  # GCP project id for running the Tag Engine service
export TAG_ENGINE_REGION="<REGION>"    # GCP region for running Tag Engine service, e.g. us-central1

export BIGQUERY_PROJECT="<PROJECT>"    # GCP project used by BigQuery data assets, can be equal to TAG_ENGINE_PROJECT. This variable is only used for setting IAM permissions in steps 10 and 11 
export BIGQUERY_REGION="<REGION>"      # GCP region in which data assets in BigQuery are stored, e.g. us-central1

export CLOUD_RUN_SA="<ID>@<PROJECT>.iam.gserviceaccount.com"     # email of your Cloud Run service account for running Tag Engine service
export TAG_CREATOR_SA="<ID>@<PROJECT>.iam.gserviceaccount.com"   # email of your Tag creator service account for running BQ queries and creating DC tags
```

<b>The key benefit of decoupling the `CLOUD_RUN_SA` from the `TAG_CREATOR_SA` is to limit the scope of what a Tag Engine client is allowed to tag.</b> When a client submits a request to Tag Engine, Tag Engine checks to see if they are authorized to use the `TAG_CREATOR_SA` before processing their request. A Tag Engine client can be either a user identity or a service account.  

If multiple teams own different data assets in BigQuery, they can each have their own `TAG_CREATOR_SA` to prevent unauthorized tagging. The `TAG_CREATOR_SA` in the `tagengine.ini` (in the next step) represents the <i>default</i> `TAG_CREATOR_SA` and clients can override this default account when they create Tag Engine configs using the `service_account` attribute of the config.     


3. Open `tagengine.ini` and set the following six variables in the file. The first five should be equal to the environment variables you set above in step 2:

```
TAG_ENGINE_PROJECT
TAG_ENGINE_REGION  
BIGQUERY_REGION
CLOUD_RUN_ACCOUNT
TAG_CREATOR_ACCOUNT
ENABLE_AUTH  
```

A couple of notes:

- `ENABLE_AUTH` is a boolean. When set to True, Tag Engine verifies that the client is authorized to use the `TAG_CREATOR_SA` prior to processing requests. 

- The `tagengine.ini` file also has two additional variables, `INJECTOR_QUEUE` and `WORK_QUEUE`. Those determine the names of the tasks queues. You do not need to change them. They are used in step 9 of the setup.  


4. Enable the required APIs:

`gcloud config set project $TAG_ENGINE_PROJECT`

```
gcloud services enable iam.googleapis.com
gcloud services enable cloudresourcemanager.googleapis.com
gcloud services enable firestore.googleapis.com
gcloud services enable cloudtasks.googleapis.com
gcloud services enable datacatalog.googleapis.com
```


5. Create the Firestore database: 

This command currently requires gcloud alpha. You can install it by running `gcloud components install alpha`.  

`gcloud alpha firestore databases create --project=$TAG_ENGINE_PROJECT --location=$TAG_ENGINE_REGION`

Note that Firestore is not available in every region. Consult [this list](https://cloud.google.com/firestore/docs/locations)
to see where it's available and choose the closest region if you can't run it in your preferred one. It's perfectly fine 
for the Firestore region to be different from the `TAG_ENGINE_REGION`. 


6. Create the Firestore indexes:

```
cd deploy
python create_indexes.py $TAG_ENGINE_PROJECT
```

This script is expected to run for 8-12 minutes. It creates 30+ composite indexes in Firestore which are needed for serving Tag Engine requests. 


7. Build and deploy the Cloud Run service:

This command currently requires gcloud beta. You can install it by running `gcloud components install beta`.  

```
gcloud beta run deploy tag-engine \
	--source . \
	--platform managed \
	--region $TAG_ENGINE_REGION \
	--no-allow-unauthenticated \
	--ingress=all \
	--service-account=$CLOUD_RUN_SA
```


8. Set one Cloud Run environment variable:

```
export SERVICE_URL=`gcloud run services describe tag-engine --format="value(status.url)"`
gcloud run services update tag-engine --set-env-vars SERVICE_URL=$SERVICE_URL
```


9. Create two task queues:

```
gcloud tasks queues create tag-engine-injector-queue \
	--location=$TAG_ENGINE_REGION --max-attempts=1 --max-concurrent-dispatches=100

gcloud tasks queues create tag-engine-work-queue \
	--location=$TAG_ENGINE_REGION --max-attempts=1 --max-concurrent-dispatches=100
```


10. Create two custom roles (required by the `SENSITIVE_COLUMN_CONFIG`):

```
gcloud iam roles create BigQuerySchemaUpdate \
	 --project $BIGQUERY_PROJECT \
	 --title BigQuerySchemaUpdate \
	 --description "Update table schema with policy tags" \
	 --permissions bigquery.tables.setCategory
```
```
gcloud iam roles create PolicyTagReader \
	--project $TAG_ENGINE_PROJECT \
	--title PolicyTagReader \
	--description "Read Policy Tag Taxonomy" \
	--permissions datacatalog.taxonomies.get,datacatalog.taxonomies.list
```

	
11. Grant the required roles and iam policy bindings to `CLOUD_RUN_SA` and `TAG_CREATOR_SA`:

```
gcloud projects add-iam-policy-binding $TAG_ENGINE_PROJECT \
	--member=serviceAccount:$CLOUD_RUN_SA \
	--role=roles/cloudtasks.enqueuer
	
gcloud projects add-iam-policy-binding $TAG_ENGINE_PROJECT \
	--member=serviceAccount:$CLOUD_RUN_SA \	
	--role=roles/cloudtasks.taskRunner
	
gcloud projects add-iam-policy-binding $TAG_ENGINE_PROJECT \
	--member=serviceAccount:$CLOUD_RUN_SA \	
	--role=roles/datastore.user 
	
gcloud projects add-iam-policy-binding $TAG_ENGINE_PROJECT \
	--member=serviceAccount:$CLOUD_RUN_SA \	
	--role=roles/run.invoker 
```

```
gcloud projects add-iam-policy-binding $TAG_ENGINE_PROJECT \
	--member=serviceAccount:$TAG_CREATOR_SA \
	--role=roles/datacatalog.tagEditor

gcloud projects add-iam-policy-binding $TAG_ENGINE_PROJECT \
	--member=serviceAccount:$TAG_CREATOR_SA \
	--role=roles/datacatalog.tagTemplateUser
	
gcloud projects add-iam-policy-binding $TAG_ENGINE_PROJECT \
	--member=serviceAccount:$TAG_CREATOR_SA \
	--role=roles/datacatalog.tagTemplateViewer

gcloud projects add-iam-policy-binding $TAG_ENGINE_PROJECT \
	--member=serviceAccount:$TAG_CREATOR_SA \
	--role=roles/datacatalog.viewer

gcloud projects add-iam-policy-binding $BIGQUERY_PROJECT \
	--member=serviceAccount:$TAG_CREATOR_SA \
	--role=roles/bigquery.dataEditor
	
gcloud projects add-iam-policy-binding $BIGQUERY_PROJECT \
	--member=serviceAccount:$TAG_CREATOR_SA \
	--role=roles/bigquery.jobUser

gcloud projects add-iam-policy-binding $BIGQUERY_PROJECT \
	--member=serviceAccount:$TAG_CREATOR_SA \
	--role=roles/bigquery.metadataViewer

gcloud projects add-iam-policy-binding $BIGQUERY_PROJECT \
	--member=serviceAccount:$TAG_CREATOR_SA \
	--role=projects/$BIGQUERY_PROJECT/roles/PolicyTagReader 

gcloud projects add-iam-policy-binding $BIGQUERY_PROJECT \
	--member=serviceAccount:$TAG_CREATOR_SA \
	--role=projects/$BIGQUERY_PROJECT/roles/BigQuerySchemaUpdate	   
```

```
gcloud iam service-accounts add-iam-policy-binding $CLOUD_RUN_SA \
	--member=serviceAccount:$CLOUD_RUN_SA --role roles/iam.serviceAccountUser
	
gcloud iam service-accounts add-iam-policy-binding $TAG_CREATOR_SA \
	--member=serviceAccount:$CLOUD_RUN_SA --role=roles/iam.serviceAccountUser

gcloud iam service-accounts add-iam-policy-binding $TAG_CREATOR_SA \
    --member=serviceAccount:$CLOUD_RUN_SA --role=roles/iam.serviceAccountTokenCreator 
```


Note: If you plan to create tags from CSV files, you also need to ensure that `TAG_CREATOR_SA` has the 
`storage.buckets.get` permission on the GCS bucket where the CSV files are stored. To do that, you can create a custom role with 
this permission or assign the `storage.legacyBucketReader` role:

```
gcloud storage buckets add-iam-policy-binding gs://<BUCKET> \
	--member=serviceAccount:$TAG_CREATOR_SA' \
	--role=roles/storage.legacyBucketReader
```


12. This is an optional step. If you plan to create Tag Engine configs which auto refresh your tags, you'll also need to make a Cloud Scheduler entry to trigger the tag updates:

```
gcloud services enable cloudscheduler.googleapis.com
```

To generate the `OAUTH_TOKEN` for the Cloud Scheduler entry, choose an account which has privileges to use `TAG_CREATOR_SA` and then you can run these two commands:
```
gcloud auth application-default login
export OAUTH_TOKEN=$(gcloud auth application-default print-access-token)
```

```
gcloud scheduler jobs create http scheduled_auto_updates \
	--description="Tag Engine scheduled jobs" \
	--location=$TAG_ENGINE_REGION --time-zone=America/Chicago \
	--schedule="0 */1 * * *" --uri="${SERVICE_URL}/scheduled_auto_updates" 	\
	--http-method=POST \
	--headers oauth_token=$OAUTH_TOKEN \
	--oidc-service-account-email=$CLIENT_SA \
	--oidc-token-audience=$SERVICE_URL 
```

This command created a Cloud Scheduler entry that will trigger tag updates every hour. If you want the tag updates to occur on a different schedule, you can adjust the value of the `schedule` parameter in the above command. 


### <a name="testa"></a>Testing your Tag Engine setup with a user account:

1. Create the sample `data_governance` tag template:

```
git clone https://github.com/GoogleCloudPlatform/datacatalog-templates.git` 
cd datacatalog-templates` 
`python create_template.py $TAG_ENGINE_PROJECT $TAG_ENGINE_REGION data_governance.yaml` 
```

2. Authorize a user account to use $TAG_CREATOR_SA and to invoke the Tag Engine Cloud Run service:

```
export USER_ACCOUNT="username@example.com"

gcloud iam service-accounts add-iam-policy-binding $TAG_CREATOR_SA \
    --member=serviceAccount:$USER_ACCOUNT --role=roles/iam.serviceAccountUser 


gcloud run services add-iam-policy-binding tag-engine \
    --member=serviceAccount:$USER_ACCOUNT --role=roles/run.invoker \
    --region=$TAG_ENGINE_REGION	
```

3. Generate an OAUTH token for your `USER_ACCOUNT`:

```
gcloud auth application-default login
export OAUTH_TOKEN=$(gcloud auth application-default print-access-token)
```

4. Generate an IAM token (aka Bearer token) for authenticating to the Tag Engine Cloud Run service:

```
gcloud auth login
export IAM_TOKEN=$(gcloud auth print-identity-token)
```

5. Create a dynamic table config:

Before running the next command, update the project and dataset values in `tests/configs/dynamic_table/dynamic_table_ondemand.json`. 

```
export TAG_ENGINE_URL=$SERVICE_URL

curl -X POST $TAG_ENGINE_URL/create_dynamic_table_config -d @tests/configs/dynamic_table/dynamic_table_ondemand.json \
	-H "Authorization: Bearer $IAM_TOKEN" \
	-H "oauth_token: $OAUTH_TOKEN"
```

The output from this command should look similar to:

```
{
  "config_type": "DYNAMIC_TAG_TABLE",
  "config_uuid": "416f9694e46911ed96c5acde48001122"
}
```

6. Trigger the job:

Before running the next command, update the `config_uuid` with your value. 

```
curl -i -X POST $TAG_ENGINE_URL/trigger_job \
  -d '{"config_type":"DYNAMIC_TAG_TABLE","config_uuid":"c255f764d56711edb96eb170f969c0af"}' \
  -H "Authorization: Bearer $IAM_TOKEN" \
  -H "oauth_token: $OAUTH_TOKEN"
```

The output from this command should look similar to:

```
{
  "job_uuid": "9c13357ee46911ed96c5acde48001122"
}
```

7. View the job status:


Before running the next command, update the `job_uuid` with your value. 

```
curl -X POST $TAG_ENGINE_URL/get_job_status -d '{"job_uuid":"9c13357ee46911ed96c5acde48001122"}' \
	-H "Authorization: Bearer $IAM_TOKEN" \
	-H "oauth_token: $OAUTH_TOKEN"
```

The output from this command should look like this:

```
{
  "job_status": "SUCCESS",
  "task_count": 1,
  "tasks_completed": 1,
  "tasks_failed": 0,
  "tasks_ran": 1
}
```

Open the Data Catalog UI and verify that your tag got created. If not, open the Cloud Run logs and investigate the problem. 


### <a name="testb"></a>Testing your Tag Engine setup with a service account:

1. Create the sample `data_governance` tag template (you can skip this step if you went through the previous test procedure):

```
git clone https://github.com/GoogleCloudPlatform/datacatalog-templates.git` 
cd datacatalog-templates` 
`python create_template.py $TAG_ENGINE_PROJECT $TAG_ENGINE_REGION data_governance.yaml` 
```

2. Authorize a service account to use $TAG_CREATOR_SA and to invoke the Tag Engine Cloud Run service:

```
export CLIENT_SA="tag-engine-client@<PROJECT>.iam.gserviceaccount.com"

gcloud iam service-accounts add-iam-policy-binding $TAG_CREATOR_SA \
    --member=serviceAccount:$CLIENT_SA --role=roles/iam.serviceAccountUser 


gcloud run services add-iam-policy-binding tag-engine \
    --member=serviceAccount:$CLIENT_SA --role=roles/run.invoker \
    --region=$TAG_ENGINE_REGION	
```

3. Generate an OAUTH token for your `CLIENT_SA`:

```
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/your/keyfile.json"
export OAUTH_TOKEN=$(gcloud auth application-default print-access-token)
```

4. Generate an IAM token (aka Bearer token) for authenticating to the Tag Engine Cloud Run service:

```
gcloud auth login
export IAM_TOKEN=$(gcloud auth print-identity-token)
```

5. Create a dynamic table config:

Before running the next command, update the project and dataset values in `tests/configs/dynamic_table/dynamic_table_ondemand.json`. 

```
export TAG_ENGINE_URL=$SERVICE_URL

curl -X POST $TAG_ENGINE_URL/create_dynamic_table_config -d @tests/configs/dynamic_table/dynamic_table_ondemand.json \
	-H "Authorization: Bearer $IAM_TOKEN" \
	-H "oauth_token: $OAUTH_TOKEN"
```

The output from this command should look similar to:

```
{
  "config_type": "DYNAMIC_TAG_TABLE",
  "config_uuid": "416f9694e46911ed96c5acde48001122"
}
```

6. Trigger the job:

Before running the next command, update the `config_uuid` with your value. 

```
curl -i -X POST $TAG_ENGINE_URL/trigger_job \
  -d '{"config_type":"DYNAMIC_TAG_TABLE","config_uuid":"c255f764d56711edb96eb170f969c0af"}' \
  -H "Authorization: Bearer $IAM_TOKEN" \
  -H "oauth_token: $OAUTH_TOKEN"
```

The output from this command should look similar to:

```
{
  "job_uuid": "9c13357ee46911ed96c5acde48001122"
}
```

7. View the job status:

Before running the next command, update the `job_uuid` with your value. 

```
curl -X POST $TAG_ENGINE_URL/get_job_status -d '{"job_uuid":"9c13357ee46911ed96c5acde48001122"}' \
	-H "Authorization: Bearer $IAM_TOKEN" \
	-H "oauth_token: $OAUTH_TOKEN"
```

The output from this command should look like this:

```
{
  "job_status": "SUCCESS",
  "task_count": 1,
  "tasks_completed": 1,
  "tasks_failed": 0,
  "tasks_ran": 1
}
```

Open the Data Catalog UI and verify that your tag was successfully created. If not, open the Cloud Run logs and investigate the problem. 


### <a name="next"></a>Next steps:

1. Explore the sample test scripts and run them:

Before running the scripts, open each file and update the `TAG_ENGINE_URL` on line 11 with your Cloud Run service URL. Also, update the project and dataset values in the json config file(s) which is referenced by each script. 

```
python tests/scripts/configure_tag_history.py
python tests/scripts/create_static_config_trigger_job.py
python tests/scripts/create_dynamic_table_config_trigger_job.py
python tests/scripts/create_dynamic_column_config_trigger_job
python tests/scripts/create_dynamic_dataset_config_trigger_job.py
python tests/scripts/create_import_config_trigger_job.py
python tests/scripts/create_export_config_trigger_job.py
python tests/scripts/list_configs.py
python tests/scripts/read_config.py
python tests/scripts/purge_inactive_configs.py
```
 
