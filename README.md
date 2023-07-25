## Tag Engine 2.0
This branch contains the Tag Engine 2.0 application, an early release of Tag Engine v2 that is hosted on Cloud Run (instead of App Engine) and is [VPC-SC compliant](https://cloud.google.com/vpc-service-controls/docs/supported-products). Tag Engine 2.0 supports authentication and the ability for multiple teams to securely tag their own data assets. 

Tag Engine is an open-source extension to Google Cloud's Data Catalog. Tag Engine automates the tagging of BigQuery tables and views as well as data lake files in Cloud Storage. You create a configuration, which contains SQL expressions that define how to populate the fields in the tags. Tag Engine runs the configuration either on demand or on a pre-defined schedule.

If you are new to Tag Engine, you may want to walk through [this tutorial](https://cloud.google.com/architecture/tag-engine-and-data-catalog). Note that the tutorial was written for Tag Engine v1 (as opposed to v2), but it will give you a sense of how Tag Engine works. We plan to publish a second tutorial for Tag Engine v2 soon. Stay tuned! 

This README is organized into four parts:  <br>
- Part 1: [Deployment Procedure](#setup) <br>
- Part 2: [Testing your Tag Engine Setup with a User Account](#testa)  <br>
- Part 3: [Testing your Tag Engine Setup with a Service Account](#testb)  <br>
- Part 4: [Next Steps](#next)  <br> 

### <a name="setup"></a> Part 1: Deployment Procedure

This procedure covers both the API and UI setups for Tag Engine v2. It has 13 required steps and 1 optional step. <br>

1. Create (or designate) two service accounts:

- A service account that runs the Tag Engine Cloud Run service, referred to below as `CLOUD_RUN_SA`. 
- A service account that performs the tagging in Data Catalog, and sourcing the contents of those tags from BigQuery, referred to below as `TAG_CREATOR_SA`. <br><br>


2. Set 6 environment variables which will be used throughout the deployment:

```
export TAG_ENGINE_PROJECT="<PROJECT>"  # GCP project id for running the Tag Engine service
export TAG_ENGINE_REGION="<REGION>"    # GCP region for running Tag Engine service, e.g. us-central1

export BIGQUERY_PROJECT="<PROJECT>"    # GCP project used by BigQuery data assets, can be equal to TAG_ENGINE_PROJECT. This variable is only used for setting IAM permissions in steps 10 and 11 
export BIGQUERY_REGION="<REGION>"      # GCP region in which data assets in BigQuery are stored, e.g. us-central1

export CLOUD_RUN_SA="<ID>@<PROJECT>.iam.gserviceaccount.com"     # email of your Cloud Run service account for running Tag Engine service
export TAG_CREATOR_SA="<ID>@<PROJECT>.iam.gserviceaccount.com"   # email of your Tag creator service account for running BQ queries and creating DC tags
```

<b>The key benefit of decoupling `CLOUD_RUN_SA` from `TAG_CREATOR_SA` is to limit the scope of what a Tag Engine client is allowed to tag.</b> More specifically, when a client submits a request to Tag Engine, Tag Engine checks to see if they are authorized to use `TAG_CREATOR_SA` before processing their request. A Tag Engine client can either be a user identity or a service account.  

If multiple teams want to share an instance of Tag Engine and they own different assets in BigQuery, they can each have their own `TAG_CREATOR_SA` to prevent one team from tagging another team's assets. `TAG_CREATOR_SA` is set in the `tagengine.ini` file (next step) with the default account for the entire Tag Engine instance. Tag Engine clients can override the default `TAG_CREATOR_SA` when creating tag configurations by specifying a `service_account` attribute in the json request (as shown [here](https://github.com/GoogleCloudPlatform/datacatalog-tag-engine/blob/cloud-run/tests/configs/dynamic_table/dynamic_dataset_non_default_service_account.json)).  <br><br>   


3. Create an OAuth client ID for your Tag Engine web application. 

- Designate a domain for your web application (e.g. tagengine.app). You can register one from [Cloud Domains](https://console.cloud.google.com/net-services/domains/) if you don't have one. 
- Create an OAuth client ID from API Credentials. Set the `Authorized redirect URI` to `https://[TAG_ENGINE_DOMAIN]/oauth2callback`, where [TAG_ENGINE_DOMAIN] is your actual domain name (e.g. `https://tagengine.app/oauth2callback`). 
- Download the OAuth client secret and save the json file to your local Tag Engine git repo (e.g. `datacatalog-tag-engine/client_secret.json`).  <br><br> 


4. Open `tagengine.ini` and set the following variables in this file. The first five should be equal to the environment variables you previously set in step 2:

```
TAG_ENGINE_PROJECT
TAG_ENGINE_REGION  
BIGQUERY_REGION
CLOUD_RUN_ACCOUNT
TAG_CREATOR_ACCOUNT
OAUTH_CLIENT_CREDENTIALS
ENABLE_AUTH  
```

A couple of notes:

- Set the variable `OAUTH_CLIENT_CREDENTIALS` to the name of your OAuth client secret file (e.g. `client_secret.json`). 

- The variable `ENABLE_AUTH` is a boolean. When set to `True`, Tag Engine verifies that the end user is authorized to use `TAG_CREATOR_SA` prior to processing their tag requests. This is the recommended value. 

- The `tagengine.ini` file also has two additional variables, `INJECTOR_QUEUE` and `WORK_QUEUE`. Those determine the names of the tasks queues. You do not need to change them. The queues are created in step 5 of this setup.  <br><br> 


5. Enable the required Google Cloud APIs in your project:

`gcloud config set project $TAG_ENGINE_PROJECT`

```
gcloud services enable iam.googleapis.com
gcloud services enable cloudresourcemanager.googleapis.com
gcloud services enable firestore.googleapis.com
gcloud services enable cloudtasks.googleapis.com
gcloud services enable datacatalog.googleapis.com
gcloud services enable artifactregistry.googleapis.com
gcloud services enable cloudbuild.googleapis.com
```
<br> 

6. Create two task queues, the first one is used to queue the tag request, the second is used to queue the individual work items:

```
gcloud tasks queues create tag-engine-injector-queue \
	--location=$TAG_ENGINE_REGION --max-attempts=1 --max-concurrent-dispatches=100

gcloud tasks queues create tag-engine-work-queue \
	--location=$TAG_ENGINE_REGION --max-attempts=1 --max-concurrent-dispatches=100
```
<br>

7. Create two custom IAM roles which are required by `SENSITIVE_COLUMN_CONFIG`, a configuration type that creates policy tags on sensitive columns:

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
<br> 
	
8. Grant the required IAM roles and policy bindings for the accounts `CLOUD_RUN_SA` and `TAG_CREATOR_SA`:

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
	--role=roles/datastore.indexAdmin  
	
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

gcloud projects add-iam-policy-binding $TAG_ENGINE_PROJECT \
	--member=serviceAccount:$TAG_CREATOR_SA \
	--role=roles/logging.viewer

gcloud projects add-iam-policy-binding $TAG_ENGINE_PROJECT \
	--member=serviceAccount:$TAG_CREATOR_SA \
	--role=projects/$TAG_ENGINE_PROJECT/roles/PolicyTagReader 

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
<br> 

	
9. Create the Firestore database, which is used to store the tag configurations: 

This command currently requires `gcloud alpha`. If you don't have it, you need to first install it before creating the database. 

```
gcloud components install alpha.  

gcloud alpha firestore databases create --project=$TAG_ENGINE_PROJECT --location=$TAG_ENGINE_REGION
```

Note that Firestore is not available in every region. Consult [this list](https://cloud.google.com/firestore/docs/locations) to see where it's available and choose the nearest region to `TAG_ENGINE_REGION`. It's perfectly fine for the Firestore region to be different from the `TAG_ENGINE_REGION`. <br><br> 
	
	
10. Firestore requires several composite indexes to service read requests. Create those indexes:

First, create a private key for your `$CLOUD_RUN_SA`:

```
gcloud iam service-accounts keys create private_key.json --iam-account=$CLOUD_RUN_SA
export GOOGLE_APPLICATION_CREDENTIALS="private_key.json"
```

Second, create the composite indexes which are needed for serving multiple read requests:

```
cd deploy
python create_indexes.py $TAG_ENGINE_PROJECT
cd ..
```

Note: the above script is expected to run for 10-12 minutes. As the indexes get created, you will see them show up in the Firestore console. There should be 36 indexes in total. <br><br> 
	


11. Build and deploy the Cloud Run service:

The next command requires `gcloud beta`. You can install it by running `gcloud components install beta`.  

It also requires you to have a VPC connector that is routing requests to private IPs. You can create one from Serverless VPC Access. 

```
gcloud beta run deploy tag-engine \
	--source . \
	--platform managed \
	--region $TAG_ENGINE_REGION \
	--allow-unauthenticated \
	--ingress=internal-and-cloud-load-balancing \
	--port=8080 \
	--min-instances=0 \
	--max-instances=5 \
	--service-account=$CLOUD_RUN_SA \
	--vpc-connector=projects/$TAG_ENGINE_PROJECT/locations/$TAG_ENGINE_REGION/connectors/$VPC_CONNECTOR \
	--vpc-egress=private-ranges-only
```
<br> 


12. Set Cloud Run environment variable:

```
export SERVICE_URL=`gcloud run services describe tag-engine --format="value(status.url)"`
gcloud run services update tag-engine --set-env-vars SERVICE_URL=$SERVICE_URL
```
<br> 


13. Put a load balancer in front of the Cloud Run service:

Create an application load balancer that accepts incoming https requests from your Tag Engine domain and forwards them to a [serverless network endpoint group](https://cloud.google.com/load-balancing/docs/negs/serverless-neg-concepts) that is tied to your Tag Engine Cloud Run service. 

Once your load balancer has been created, use its IP address to create an A record in Cloud DNS. 

If you created an external load balancer, enable Identity-Aware Proxy (IAP) on your load balancer's backend. Grant `IAP-secured Web App User` role to the user identities who are allowed to access the application. 
<br><br> 


14. This step is optional. If you plan to let Tag Engine to auto refresh your tags on a set schedule, you'll also need to make a Cloud Scheduler entry to trigger those tag updates:

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
	--schedule="0 */1 * * *" \
        --uri="${SERVICE_URL}/scheduled_auto_updates" \
	--http-method=POST \
	--headers oauth_token=$OAUTH_TOKEN \
	--oidc-service-account-email=$CLIENT_SA \
	--oidc-token-audience=$SERVICE_URL 
```

This command created a Cloud Scheduler entry that will trigger tag updates every hour. If you want the tag updates to occur on a different schedule, you can adjust the value of the `schedule` parameter in the above command. 
<br><br>

### <a name="testa"></a> Part 2: Testing your Tag Engine setup with a user account

1. Create the sample `data_governance` tag template:

```
git clone https://github.com/GoogleCloudPlatform/datacatalog-templates.git 
cd datacatalog-templates
python create_template.py $TAG_ENGINE_PROJECT $TAG_ENGINE_REGION data_governance.yaml 
```
<br>

2. Authorize a user account to use $TAG_CREATOR_SA and to invoke the Tag Engine Cloud Run service:

```
export USER_ACCOUNT="username@example.com"

gcloud iam service-accounts add-iam-policy-binding $TAG_CREATOR_SA \
    --member=user:$USER_ACCOUNT --role=roles/iam.serviceAccountUser 


gcloud run services add-iam-policy-binding tag-engine \
    --member=user:$USER_ACCOUNT --role=roles/run.invoker \
    --region=$TAG_ENGINE_REGION	
```
<br>

3. Test your Tag Engine UI path:

- Open a browser window
- Navigate to https://[TAG_ENGINE_DOMAIN] 
- If you enabled IAP, you should be prompted to sign in to `iap.googleapis.com`
- Once signed in, you should be directed to the Tag Engine home page (i.e. https://[TAG_ENGINE_DOMAIN]/home)
- Enter your template id, template project, and template region
- Enter your $TAG_CREATOR_SA as the service account
- Click on `Search Tag Templates` to continue to the next step and create a tag configuration

If you encouter a 500 error, open the Cloud Run logs to troubleshoot. 
<br><br>


4. Test your Tag Engine API path: 

a) Generate an OAUTH token for your `USER_ACCOUNT`:

```
gcloud auth application-default login
export OAUTH_TOKEN=$(gcloud auth application-default print-access-token)
```

b) Generate an IAM token (aka Bearer token) for authenticating to the Tag Engine Cloud Run service:

```
gcloud auth login
export IAM_TOKEN=$(gcloud auth print-identity-token)
```

c) Create a dynamic table config:

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

d) Trigger the job:

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

e) View the job status:


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

Open the Data Catalog UI and verify that your tag was successfully created. If your tags are not there or if you encounter an error with the previous commands, open the Cloud Run logs and investigate. 
<br><br>

### <a name="testb"></a> Part 3: Testing your Tag Engine Setup with a Service Account

1. Create the sample `data_governance` tag template (you can skip this step if you went through the previous test procedure):

```
git clone https://github.com/GoogleCloudPlatform/datacatalog-templates.git 
cd datacatalog-templates
python create_template.py $TAG_ENGINE_PROJECT $TAG_ENGINE_REGION data_governance.yaml
```
<br>

2. Authorize a service account to use $TAG_CREATOR_SA and to invoke the Tag Engine Cloud Run service:

```
export CLIENT_SA="tag-engine-client@<PROJECT>.iam.gserviceaccount.com"

gcloud iam service-accounts add-iam-policy-binding $TAG_CREATOR_SA \
    --member=serviceAccount:$CLIENT_SA --role=roles/iam.serviceAccountUser 


gcloud run services add-iam-policy-binding tag-engine \
    --member=serviceAccount:$CLIENT_SA --role=roles/run.invoker \
    --region=$TAG_ENGINE_REGION	
```
<br>

3. Generate an OAUTH token for your `CLIENT_SA`:

```
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/your/keyfile.json"
gcloud auth activate-service-account $CLIENT_SA --key-file $GOOGLE_APPLICATION_CREDENTIALS
export OAUTH_TOKEN=$(gcloud auth application-default print-access-token)
```
<br>

4. Generate an IAM token (aka Bearer token) for authenticating to the Tag Engine Cloud Run service:

```
gcloud auth login
export IAM_TOKEN=$(gcloud auth print-identity-token)
```
<br>

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
<br>

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
<br>

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
<br><br>

### <a name="next"></a> Part 4: Next Steps

1. Explore additional API methods and run them through curl commands:

Open `tests/unit_test.sh` and go through the different methods for interracting with Tag Engine, including `configure_tag_history`, `create_static_asset_config`, `create_dynamic_column_config`, etc. <br><br>


2. Explore the sample test scripts:

There are multiple test scripts in Python in the `tests/scripts` folder. These are intended to help you get started with the Tag Engine API. 

Before running the scripts, open each file and update the `TAG_ENGINE_URL` variable on line 11 with your own Cloud Run service URL. You'll also need to update the project and dataset values which may be in the script itself or in the referenced json config file. 

Here are some of the scripts you can look at and run:
```
python configure_tag_history.py
python create_static_config_trigger_job.py
python create_dynamic_table_config_trigger_job.py
python create_dynamic_column_config_trigger_job
python create_dynamic_dataset_config_trigger_job.py
python create_import_config_trigger_job.py
python create_export_config_trigger_job.py
python list_configs.py
python read_config.py
python purge_inactive_configs.py
```
<br>

3. Explore the sample workflow scripts:

The `apps/workflows/` contains some sample workflows. The `trigger_job.yaml` and `orchestrate_jobs.yaml` show how orchestrate Tag Engine tasks from a Cloud Workflow. To run the workflows, you need to enable the Cloud Workflows API (`workflows.googleapis.com`) and follow these steps:

```
export OAUTH_TOKEN=$(gcloud auth application-default print-access-token)

gcloud workflows deploy orchestrate-jobs --location=$TAG_ENGINE_REGION \
	--source=orchestrate_jobs.yaml --service-account=$CLOUD_RUN_SA

gcloud workflows run orchestrate-jobs --location=$TAG_ENGINE_REGION \
	--data='{"oauth_token":"'"$OAUTH_TOKEN"'"}'
``` 
<br>

4. Create your own Tag Engine configs with the UI and/or API. <br><br>


5. Open new [issues](https://github.com/GoogleCloudPlatform/datacatalog-tag-engine/issues) if you encounter bugs or would like to request a feature. 
