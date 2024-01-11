## Tag Engine 2.0
This is the main branch for Tag Engine. Tag Engine 2.0 is a flavor of Tag Engine that is hosted on Cloud Run instead of App Engine and is [VPC-SC compatible](https://cloud.google.com/vpc-service-controls/docs/supported-products). It supports user authentication and role based access control. Customers who have multiple teams using BigQuery and Cloud Storage can authorize each team to tag only their data assets. 

Tag Engine is an open-source extension to Google Cloud's Data Catalog which is now part of the Dataplex product suite. Tag Engine automates the tagging of BigQuery tables and views as well as data lake files in Cloud Storage. You create tag configurations that specify how to populate the various fields of a tag template through SQL expressions or static values. Tag Engine runs the configurations either on demand or on a schedule to create, update or delete the tags.

If you are new to Tag Engine, you may want to walk through a basic [tutorial](https://cloud.google.com/architecture/tag-engine-and-data-catalog). Please keep in mind that the tutorial was written with Tag Engine 1.0 in mind, and many of the API calls and UI elements have changed in 2.0. 

This README file contains deployment steps, testing procedures, and code samples. It is organized into five sections:  <br>
- Part 1: [Deploying Tag Engine v2](#deploy) <br>
- Part 2: [Testing your Tag Engine API Setup](#testa)  <br>
- Part 3: [Testing your Tag Engine UI Setup](#testb)  <br>
- Part 4: [Troubleshooting](#troubleshooting)  <br>
- Part 5: [Next Steps](#next)  <br> 

### <a name="deploy"></a> Part 1: Deploying Tag Engine v2

Tag Engine 2.0 comes with two Cloud Run services. One service is for the API (`tag-engine-api`) and the other is for the UI (`tag-engine-ui`). 

Both services use access tokens for authorization. The API service expects the client to pass in an access token when calling the API functions (`gcloud auth print-identity-token`) whereas the UI service uses OAuth to authorize the client from the front-end. Note that a client secret file is required for the OAuth flow.  

Follow the steps below to deploy Tag Engine with Terraform. 

Alternatively, you may choose to deploy Tag Engine with [gcloud commands](https://github.com/GoogleCloudPlatform/datacatalog-tag-engine/tree/cloud-run/docs/manual_deployment.md) instead of running the Terraform.

<br>
1. Create (or designate) two service accounts: <br><br>

   - A service account that runs the Tag Engine Cloud Run service, referred to as `TAG_ENGINE_SA`. 
   - A service account that performs the tagging in Data Catalog, and sourcing the contents of those tags from BigQuery, referred to as `TAG_CREATOR_SA`. <br>

   Why do we need two service accounts? The key benefit of decoupling them is to allow individual teams to have their own Tag Creator SAs. For API access, teams can then link 
   invoker SAs to Tag Creator SAs so that a single Tag Engine instance can be shared by multiple teams. Similarly, for UI access, teams can link user accounts to Tag Creator SAs 
   so that a single Tag Engine instance can be shared by multiple teams. <br><br>

2. Create an OAuth client:

   Open [API Credentials](https://console.cloud.google.com/apis/credentials).<br>

   Click on Create Credentials and select OAuth client ID and choose the following settings:<br>

   Application type: web application<br>
   Name: tag-engine-oauth<br>
   Authorized redirects URIs: <i>Leave this field blank for now.</i>  
   Click Create<br>
   Download the credentials as `te_client_secret.json` and place the file in the root of the `datacatalog-tag-engine` directory<br><br>

   Note: The client secret file is required for establishing the authorization flow from the UI.  

3. Open `datacatalog-tag-engine/tagengine.ini` and set the following variables in this file: 

	```
	TAG_ENGINE_PROJECT
	TAG_ENGINE_REGION  
	BIGQUERY_REGION
	TAG_ENGINE_SA
	TAG_CREATOR_SA
	ENABLE_AUTH
	OAUTH_CLIENT_CREDENTIALS
	ENABLE_TAG_HISTORY
	TAG_HISTORY_PROJECT
	TAG_HISTORY_DATASET
	ENABLE_JOB_METADATA
	JOB_METADATA_PROJECT
	JOB_METADATA_DATASET  
	```

   A couple of notes: <br>

   - The variable `ENABLE_AUTH` is a boolean. When set to `True`, Tag Engine verifies that the end user is authorized to use `TAG_CREATOR_SA` prior to processing their tag requests. This is the recommended value. <br>

   - The `tagengine.ini` file also has two additional variables, `INJECTOR_QUEUE` and `WORK_QUEUE`. These determine the names of the cloud task queues. You do not need to change them. If you change their name, you need to also change them in the `deploy/variables.tf`.<br>

   - Add `DB_NAME` if you want to use a firestore database with a name other than (default)<br><br>


4. Set the Terraform variables:

   Open `deploy/variables.tf` and change the default value of each variable.<br>
   Save the file.<br><br> 


5. Run the Terraform scripts:

	```
	cd deploy
	terraform init
	terraform plan
	terraform apply
	```

	When the Terraform finishes running, it should output two URIs. One for the API service (which looks like this https://tag-engine-api-xxxxxxxxxxxxx.a.run.app) and another for the UI service (which looks like this https://tag-engine-ui-xxxxxxxxxxxxx.a.run.app). <br><br>


6. Set the authorized redirect URI and add authorized users:

    - Re-open [API Credentials](https://console.cloud.google.com/apis/credentials)<br>

    - Under OAuth 2.0 Client IDs, edit the `tag-engine-oauth` entry which you created earlier. <br>

	- Under Authorized redirect URIs, add the URI:
      https://tag-engine-ui-xxxxxxxxxxxxx.a.run.app/oauth2callback
	
	- Replace xxxxxxxxxxxxx in the URI with the actual value from the Terraform. This URI will be referred to below as the `UI_SERVICE_URI`.

    - Open the OAuth consent screen page and under the Test users section, click on add users.

    - Add the email address of each user for which you would like to grant access to the Tag Engine UI. 

<br><br>
### <a name="testa"></a> Part 2: Testing your Tag Engine API setup

1. Create the sample `data_governance` tag template:

```
git clone https://github.com/GoogleCloudPlatform/datacatalog-templates.git 
cd datacatalog-templates
python create_template.py $TAG_ENGINE_PROJECT $TAG_ENGINE_REGION data_governance.yaml 
```
<br>

2. Grant permissions to invoker account (user or service)

If you are invoking the Tag Engine API with a user account, authorize your user account as follows:

```
export INVOKER_USER_ACCOUNT="username@example.com"

gcloud iam service-accounts add-iam-policy-binding $TAG_CREATOR_SA \
	--member=user:$INVOKER_USER_ACCOUNT --role=roles/iam.serviceAccountUser

gcloud iam service-accounts add-iam-policy-binding $TAG_CREATOR_SA \
	--member=user:$INVOKER_USER_ACCOUNT --role=roles/iam.serviceAccountTokenCreator 

gcloud run services add-iam-policy-binding tag-engine-api 
	--member=user:$INVOKER_USER_ACCOUNT --role=roles/run.invoker \
	--region=$TAG_ENGINE_REGION	
```

If you are invoking the Tag Engine API with a service account, authorize your service account as follows:

```
export INVOKER_SERVICE_ACCOUNT="tag-engine-invoker@<PROJECT>.iam.gserviceaccount.com"

gcloud iam service-accounts add-iam-policy-binding $TAG_CREATOR_SA \
	--member=serviceAccount:$INVOKER_SERVICE_ACCOUNT --role=roles/iam.serviceAccountUser 

gcloud iam service-accounts add-iam-policy-binding $TAG_CREATOR_SA \
	--member=serviceAccount:$INVOKER_SERVICE_ACCOUNT --role=roles/iam.serviceAccountTokenCreator 

gcloud run services add-iam-policy-binding tag-engine-api \
	--member=serviceAccount:$INVOKER_SERVICE_ACCOUNT --role=roles/run.invoker \
	--region=$TAG_ENGINE_REGION	
```

<b>Very important: Tag Engine requires that these roles be directly attached to your invoker account(s).</b> 

<br>

3. Generate an IAM token (aka Bearer token) for authenticating to Tag Engine: 


If you are invoking Tag Engine with a user account, run `gcloud auth login` and authenticate with your user account. 
If you are invoking Tag Engine with a service account, set `GOOGLE_APPLICATION_CREDENTIALS`. 

```
export IAM_TOKEN=$(gcloud auth print-identity-token)
```
<br>

4. Create your first Tag Engine config:

Tag Engine uses configurations (configs for short) to define tag requests. There are several types of configs, we will use the dynamic table config for this example. 

Open `examples/configs/dynamic_table/dynamic_table_ondemand.json` and update the project and dataset values in this file.  

```
export TAG_ENGINE_URL=$SERVICE_URL

curl -X POST $TAG_ENGINE_URL/create_dynamic_table_config -d @examples/configs/dynamic_table/dynamic_table_ondemand.json \
	 -H "Authorization: Bearer $IAM_TOKEN"
```

Note: `$SERVICE_URL` should be equal to your Cloud Run URL for `tag-engine-api`. 

The output from the previous command should look similar to:

```
{"config_type":"DYNAMIC_TAG_TABLE","config_uuid":"facb59187f1711eebe2b4f918967d564"}
```
<br>

5. Run your first job:

A Tag Engine job is an execution of a config. In this step, you execute the dynamic table config using the config_uuid from the previous step. 

Note: Before running the next command, please update the `config_uuid` with your value. 

```
curl -i -X POST $TAG_ENGINE_URL/trigger_job \
	-d '{"config_type":"DYNAMIC_TAG_TABLE","config_uuid":"facb59187f1711eebe2b4f918967d564"}' \
	-H "Authorization: Bearer $IAM_TOKEN"
```

The output from the previous command should look similar to:

```
{
	"job_uuid": "069a312e7f1811ee87244f918967d564"
}
```

If you enabled job metadata in `tagengine.ini`, you can optionally pass a job metadata object to the trigger_job call:
	
```
curl -i -X POST $TAG_ENGINE_URL/trigger_job \
	-d '{"config_type":"DYNAMIC_TAG_TABLE","config_uuid":"c255f764d56711edb96eb170f969c0af","job_metadata": {"source": "Collibra", "workflow": "process_sensitive_data"}}' \
	-H "Authorization: Bearer $IAM_TOKEN"
```
	
The job metadata parameter gets written into a BigQuery table that is associated with the job_uuid. 

<br>

6. View your job status:

Note: Before running the next command, please update the `job_uuid` with your value. 

```
curl -X POST $TAG_ENGINE_URL/get_job_status -d '{"job_uuid":"069a312e7f1811ee87244f918967d564"}' \
	-H "Authorization: Bearer $IAM_TOKEN"
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

Open the Data Catalog UI and verify that your tag was successfully created. If your tag is not there or if you encounter an error with the previous commands, open the Cloud Run logs for tag-engine-api and investigate. 

<br>

### <a name="testb"></a> Part 3: Testing your Tag Engine UI Setup

1. Open a browser window
2. Navigate to `UI_SERVICE_URI` 
3. You should be prompted to sign in with Google
4. Once signed in, you will be redirected to the Tag Engine home page (i.e. `UI_SERVICE_URI`/home)
5. Enter your template id, template project, and template region
6. Enter your `TAG_CREATOR_SA` as the service account
7. Click on `Search Tag Templates` to continue to the next step 
8. Create a tag configuration by selecting one of the options

If you encouter a 500 error, open the Cloud Run logs for tag-engine-ui to troubleshoot. 

<br>

### <a name="troubleshooting"></a> Part 4: Troubleshooting

There is a known issue with the Terraform. If you encounter the error `The requested URL was not found on this server` when you try to create a configuration from the API, the issue is that the container didn't build correctly. Try to rebuild and redeploy the Cloud Run API service with this command:

```
cd datacatalog-tag-engine
gcloud run deploy tag-engine-api \
 	--source . \
 	--platform managed \
 	--region $TAG_ENGINE_REGION \
 	--no-allow-unauthenticated \
 	--ingress=all \
 	--memory=1024Mi \
 	--service-account=$TAG_ENGINE_SA
```

Then, call the `ping` endpoint as follows:
```
curl $TAG_ENGINE_URL/ping -H "Authorization: Bearer $IAM_TOKEN"
```
You should see the following response:
```
Tag Engine is alive
```

<br>

### <a name="next"></a> Part 5: Next Steps

1. Explore additional API methods and run them through curl commands:

   Open `examples/unit_test.sh` and go through the different methods for interracting with Tag Engine, including `configure_tag_history`, `create_static_asset_config`, `create_dynamic_column_config`, etc. <br>

<br>

2. Explore the script samples:

   There are multiple test scripts in Python in the `examples/scripts` folder. These are intended to help you get started with the Tag Engine API. 

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

3. Explore sample workflows:

   The `extensions/orchestration/` folder contains some sample workflows implemented in Cloud Workflow. The `trigger_tag_export.yaml` and `trigger_tag_export_import.yaml` show how to orchestrate Tag Engine jobs. To run the workflows, enable the Cloud Workflows API (`workflows.googleapis.com`) and then follow these steps:

	```
	gcloud workflows deploy orchestrate-jobs --location=$TAG_ENGINE_REGION \
		--source=trigger_export_import.yaml --service-account=$CLOUD_RUN_SA

	gcloud workflows run trigger_export_import --location=$TAG_ENGINE_REGION
	```
	In addition to the Cloud Workflow examples, there are two examples for Airflow in the same folder, `dynamic_tag_update.py` and `pii_classification_dag.py`.  

<br>

4. Create your own Tag Engine configs with the API and/or UI. <br>

<br>

5. Open new [issues](https://github.com/GoogleCloudPlatform/datacatalog-tag-engine/issues) if you encounter bugs or would like to request a new feature or extension. 
