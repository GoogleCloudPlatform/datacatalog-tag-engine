## Tag Engine 2.0
This branch contains the Tag Engine 2.0 application, a recent release of Tag Engine v2 that is hosted on Cloud Run (instead of App Engine) and is [VPC-SC compatible](https://cloud.google.com/vpc-service-controls/docs/supported-products). Tag Engine 2.0 supports user authorization and the ability for multiple teams using BigQuery to tag only the data catalog entries which they have permission to use. 

Tag Engine is an open-source extension to Google Cloud's Data Catalog which is now part of the Dataplex suite. Tag Engine automates the tagging of BigQuery tables and views as well as data lake files in Cloud Storage. You create a configuration, for example, one that contains SQL expressions that define how to populate the fields in the tags. Tag Engine runs the configuration either on demand or on a schedule.

If you are new to Tag Engine, you may want to walk through [this](https://cloud.google.com/architecture/tag-engine-and-data-catalog) basic tutorial. Note that the tutorial was written with Tag Engine v1 in mind, but it will still give you a sense of how Tag Engine configurations works. We plan to publish a revised tutorial for Tag Engine v2 and will link it from here when ready. In the meantime, this README contains the deployment steps, the testing procedures, and references to multiple code samples to help you get started. 

This README is organized into four parts:  <br>
- Part 1: [Deploying Tag Engine v2](#deploy) <br>
- Part 2: [Testing your Setup with a User Account](#testa)  <br>
- Part 3: [Testing your Setup with a Service Account](#testb)  <br>
- Part 4: [Troubleshooting](#troubleshooting)  <br>
- Part 5: [What To Do Next](#next)  <br> 

### <a name="deploy"></a> Part 1: Deploying Tag Engine v2

Tag Engine v2 comes with two Cloud Run services. One service is for the API (`tag-engine-api`) and the other is for the UI (`tag-engine-ui`). 

Both services use access tokens for authorization. The API service expects the client to pass in an access token when calling the API functions whereas the UI service uses OAuth to authorize the client from the frontend. Note that a client secret file is required for the OAuth flow.  

Follow the 6 steps below to deploy Tag Engine v2 with Terraform and without a load balancer. 

Alternative 1: you can deploy Tag Engine v2 behind an [external load balancer](https://github.com/GoogleCloudPlatform/datacatalog-tag-engine/tree/cloud-run/docs/external_load_balancer.md). 

Alternative 2: you can choose to deploy Tag Engine v2 with [gcloud commands](https://github.com/GoogleCloudPlatform/datacatalog-tag-engine/tree/cloud-run/docs/manual_deployment.md) instead of running the Terraform.

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
   Download the credentials as `client_secret.json` and place the file in the root of the `datacatalog-tag-engine` directory<br><br>

   Note: The client secret file is required for establishing the authorization flow from the UI.  

3. Open `datacatalog-tag-engine/tagengine.ini` and set the following variables in this file: 

	```
	TAG_ENGINE_PROJECT
	TAG_ENGINE_REGION  
	BIGQUERY_REGION
	TAG_ENGINE_SA
	TAG_CREATOR_SA
	ENABLE_AUTH  
	```

   A couple of notes: <br>

   - The variable `ENABLE_AUTH` is a boolean. When set to `True`, Tag Engine verifies that the end user is authorized to use `TAG_CREATOR_SA` prior to processing their tag requests. This is the recommended value. <br>

   - The `tagengine.ini` file also has two variables, `INJECTOR_QUEUE` and `WORK_QUEUE`. Those determine the names of the tasks queues. You do not need to change them. If you change their name, you need to change them in the `deploy/variables.tf` as well.<br><br> 


4. Set the Terraform variables:

   Open `deploy/without_load_balancer/variables.tf` and change the default value of each variable.<br>
   Save the file.<br><br> 


5. Run the Terraform scripts:

	```
	cd deploy/without_load_balancer
	terraform init
	terraform plan
	terraform apply
	```

	When the Terraform finishes running, it should output two URIs. One for the API service (which looks like this https://tag-engine-api-xxxxxxxxxxxxx.a.run.app) and another for the UI service (which looks like this https://tag-engine-ui-xxxxxxxxxxxxx.a.run.app). <br><br>


6. Set the authorized redirect URI and add authorized users:

    Re-open [API Credentials](https://console.cloud.google.com/apis/credentials)<br>

    Under OAuth 2.0 Client IDs, edit the `tag-engine-oauth` entry which you created earlier. <br>

	Under Authorized redirect URIs, add the URI:
	
    https://tag-engine-ui-xxxxxxxxxxxxx.a.run.app/oauth2callback
	
	Replace xxxxxxxxxxxxx in the URI with the actual value from the Terraform. This URI will be referred to below as the `UI_SERVICE_URI`.

    Open the OAuth consent screen page and under the Test users section, click on add users.

    Add the email address of each user for which you would like to grant access to the Tag Engine UI. 

<br><br>
### <a name="testa"></a> Part 2: Testing your Tag Engine setup with a user account

1. Create the sample `data_governance` tag template:

```
git clone https://github.com/GoogleCloudPlatform/datacatalog-templates.git 
cd datacatalog-templates
python create_template.py $TAG_ENGINE_PROJECT $TAG_ENGINE_REGION data_governance.yaml 
```
<br>

2. Authorize a user account to use `$TAG_CREATOR_SA` and to invoke the Tag Engine API Cloud Run service:

```
export USER_ACCOUNT="username@example.com"

gcloud iam service-accounts add-iam-policy-binding $TAG_CREATOR_SA \
    --member=user:$USER_ACCOUNT --role=roles/iam.serviceAccountUser 


gcloud run services add-iam-policy-binding tag-engine-api \
    --member=user:$USER_ACCOUNT --role=roles/run.invoker \
    --region=$TAG_ENGINE_REGION	
```
<br>

3. Test your Tag Engine UI path:

   - Open a browser window
   - Navigate to `UI_SERVICE_URI` 
   - You should be prompted to sign in with Google
   - Once signed in, you will be redirected to the Tag Engine home page (i.e. `UI_SERVICE_URI`/home)
   - Enter your template id, template project, and template region
   - Enter your `TAG_CREATOR_SA` as the service account
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

      Note: Before running the next command, please update the `config_uuid` with your value. 

```
curl -i -X POST $TAG_ENGINE_URL/trigger_job \
	-d '{"config_type":"DYNAMIC_TAG_TABLE","config_uuid":"416f9694e46911ed96c5acde48001122"}' \
	-H "Authorization: Bearer $IAM_TOKEN" \
	-H "oauth_token: $OAUTH_TOKEN"
```

      The output from this command should look similar to:

```
	{
	  "job_uuid": "069a312e7f1811ee87244f918967d564"
	}
```

   e) View the job status:


      Note: Before running the next command, please update the `job_uuid` with your value. 

```
curl -X POST $TAG_ENGINE_URL/get_job_status -d '{"job_uuid":"069a312e7f1811ee87244f918967d564"}' \
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

2. Authorize a service account to use $TAG_CREATOR_SA and to invoke the Tag Engine API Cloud Run service:

```
	export CLIENT_SA="tag-engine-client@<PROJECT>.iam.gserviceaccount.com"

	gcloud iam service-accounts add-iam-policy-binding $TAG_CREATOR_SA \
	    --member=serviceAccount:$CLIENT_SA --role=roles/iam.serviceAccountUser 


	gcloud run services add-iam-policy-binding tag-engine-api \
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

### <a name="troubleshooting"></a> Part 4: Troubleshooting

If you encounter the error `The requested URL was not found on this server` after running the Terraform, the issue is that the Cloud Run API service didn't get built correctly. Try to rebuild and redeploy the Cloud Run API service with this command:

```
cd datacatalog-tag-engine
gcloud beta run deploy tag-engine-api \
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
curl $TAG_ENGINE_URL/ping -H "Authorization: Bearer $IAM_TOKEN" -H "oauth_token: $OAUTH_TOKEN"
```
You should see the following response:
```
Tag Engine is alive
```

### <a name="next"></a> Part 5: Next Steps

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

3. Explore the sample workflows:

   The `apps/workflows/` contains a few sample workflows implemented in Cloud Workflow. The `trigger_job.yaml` and `orchestrate_jobs.yaml` show how to orchestrate some tagging activities. To run the workflows, enable the Cloud Workflows API (`workflows.googleapis.com`) and then follow these steps:

	```
	export OAUTH_TOKEN=$(gcloud auth application-default print-access-token)

	gcloud workflows deploy orchestrate-jobs --location=$TAG_ENGINE_REGION \
		--source=orchestrate_jobs.yaml --service-account=$CLOUD_RUN_SA

	gcloud workflows run orchestrate-jobs --location=$TAG_ENGINE_REGION \
		--data='{"oauth_token":"'"$OAUTH_TOKEN"'"}'
	``` 
<br>

4. Create your own Tag Engine configs with the UI and/or API. <br><br>


5. Open new [issues](https://github.com/GoogleCloudPlatform/datacatalog-tag-engine/issues) if you encounter any bugs or would like to request a feature. 
