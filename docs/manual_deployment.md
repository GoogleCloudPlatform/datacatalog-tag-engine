### <a name="setup"></a> Manual Deployment

This procedure deploys the Tag Engine v2 components by hand. The steps are carried out via gcloud and in some cases using the Google Cloud console. For the Terraform deployment, please consult [README.md](https://github.com/GoogleCloudPlatform/datacatalog-tag-engine/blob/cloud-run/README.md).<br>

1. Create (or designate) two service accounts:

   - A service account that runs the Tag Engine Cloud Run service, referred to below as `TAG_ENGINE_SA`. 
   - A service account that performs the tagging in Data Catalog, and sourcing the contents of those tags from BigQuery, referred to below as `TAG_CREATOR_SA`. <br><br>


2. Define 6 environment variables which will be used throughout the deployment:

	```
	export TAG_ENGINE_PROJECT="<PROJECT>"  # GCP project id for running the Tag Engine service
	export TAG_ENGINE_REGION="<REGION>"    # GCP region for running Tag Engine service, e.g. us-central1

	export BIGQUERY_PROJECT="<PROJECT>"    # GCP project used by BigQuery data assets, can be equal to TAG_ENGINE_PROJECT. This variable is only used for setting IAM permissions in steps 10 and 11 
	export BIGQUERY_REGION="<REGION>"      # GCP region in which data assets in BigQuery are stored, e.g. us-central1

	export TAG_ENGINE_SA="<ID>@<PROJECT>.iam.gserviceaccount.com"    # email of your Cloud Run service account for running Tag Engine service
	export TAG_CREATOR_SA="<ID>@<PROJECT>.iam.gserviceaccount.com"   # email of your Tag creator service account for running BQ queries and creating DC tags
	```

<b>The key benefit of decoupling `TAG_ENGINE_SA` from `TAG_CREATOR_SA` is to limit the scope of what a Tag Engine client is allowed to tag.</b> More specifically, when a client submits a request to Tag Engine, Tag Engine checks to see if they are authorized to use `TAG_CREATOR_SA` before processing their request. A Tag Engine client can either be a user identity or a service account.  

If multiple teams want to share an instance of Tag Engine and they own different assets in BigQuery, they can each have their own `TAG_CREATOR_SA` to prevent one team from tagging another team's assets. `TAG_CREATOR_SA` is set in the `tagengine.ini` file (next step) with the default account for the entire Tag Engine instance. Tag Engine clients can override the default `TAG_CREATOR_SA` when creating tag configurations by specifying a `service_account` attribute in the json request (as shown [here](https://github.com/GoogleCloudPlatform/datacatalog-tag-engine/blob/cloud-run/tests/configs/dynamic_table/dynamic_dataset_non_default_service_account.json)).  <br><br>   


3. Create an OAuth client ID for your Tag Engine web application: 

   - This step is only required if you are deploying the Tag Engine UI. Skip this step if you are only deploying the API. 

   - Designate a custom domain for your web application (e.g. tagengine.app). You can register a domain from GCP with [Cloud Domains](https://console.cloud.google.com/net-services/domains/) if you need one. If you don't want to use a custom domain, you can use the Cloud Run service URL instead. 

   - Create an OAuth client ID from API Credentials. Set the `Authorized redirect URI` to `https://[TAG_ENGINE_DOMAIN]/oauth2callback`, where [TAG_ENGINE_DOMAIN] is your actual domain name (e.g. `https://tagengine.app/oauth2callback`). If you are planning to use the Cloud Run service URL, you can leave this field empty for now, and populate it at the end once you know your Cloud Run service URL for the Tag Engine UI. 

   - Download the OAuth client secret and save the json file to the root of your local Tag Engine repository as `te_client_secret.json`.  <br><br> 


4. Open `tagengine.ini` and set the following variables in this file. The first five should be equal to the environment variables you previously set in step 2:

	```
	TAG_ENGINE_PROJECT
	TAG_ENGINE_REGION  
	BIGQUERY_REGION
	CLOUD_RUN_ACCOUNT
	TAG_CREATOR_ACCOUNT
	OAUTH_CLIENT_CREDENTIALS
	ENABLE_AUTH
	TAG_HISTORY_PROJECT
	TAG_HISTORY_DATASET
	ENABLE_JOB_METADATA
	JOB_METADATA_PROJECT
	JOB_METADATA_DATASET   
	```

   A couple of notes:

   - Set the variable `OAUTH_CLIENT_CREDENTIALS` to the name of your OAuth client secret file (e.g. `te_client_secret.json`). If you are not deploying the UI, you don't need to set `OAUTH_CLIENT_CREDENTIALS`.  

   - The variable `ENABLE_AUTH` is a boolean. When set to `True`, Tag Engine verifies that the end user is authorized to use `TAG_CREATOR_SA` prior to processing their tag requests. This is the recommended value. 

   - The `tagengine.ini` file also has two additional variables, `INJECTOR_QUEUE` and `WORK_QUEUE`. These determine the names of the cloud tasks queues. You do not need to change them. The queues are created in step 6 of this setup.  <br><br> 


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

6. Create two cloud task queues, the first one is used to queue the entire job while the second is used to queue individual work items. If the first task fails, a second one will get created due to `max-attempts=2`:

	```
	gcloud tasks queues create tag-engine-injector-queue \
		--location=$TAG_ENGINE_REGION --max-attempts=2 --max-concurrent-dispatches=100

	gcloud tasks queues create tag-engine-work-queue \
		--location=$TAG_ENGINE_REGION --max-attempts=2 --max-concurrent-dispatches=100
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
	
8. Grant the required IAM roles and policy bindings for the accounts `TAG_ENGINE_SA` and `TAG_CREATOR_SA`:

	```
	gcloud projects add-iam-policy-binding $TAG_ENGINE_PROJECT \
		--member=serviceAccount:$TAG_ENGINE_SA \
		--role=roles/cloudtasks.enqueuer
	
	gcloud projects add-iam-policy-binding $TAG_ENGINE_PROJECT \
		--member=serviceAccount:$TAG_ENGINE_SA \
		--role=roles/cloudtasks.taskRunner
	
	gcloud projects add-iam-policy-binding $TAG_ENGINE_PROJECT \
		--member=serviceAccount:$TAG_ENGINE_SA \
		--role=roles/datastore.user

	gcloud projects add-iam-policy-binding $TAG_ENGINE_PROJECT \
		--member=serviceAccount:$TAG_ENGINE_SA \
		--role=roles/datastore.indexAdmin  
	
	gcloud projects add-iam-policy-binding $TAG_ENGINE_PROJECT \
		--member=serviceAccount:$TAG_ENGINE_SA \
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
	gcloud iam service-accounts add-iam-policy-binding $TAG_ENGINE_SA \
		--member=serviceAccount:$TAG_ENGINE_SA --role roles/iam.serviceAccountUser
	
	gcloud iam service-accounts add-iam-policy-binding $TAG_CREATOR_SA \
		--member=serviceAccount:$TAG_ENGINE_SA --role=roles/iam.serviceAccountUser

	gcloud iam service-accounts add-iam-policy-binding $TAG_CREATOR_SA \
	    --member=serviceAccount:$TAG_ENGINE_SA --role=roles/iam.serviceAccountViewer

	gcloud iam service-accounts add-iam-policy-binding $TAG_CREATOR_SA \
	    --member=serviceAccount:$TAG_ENGINE_SA --role=roles/iam.serviceAccountTokenCreator
			 
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

	
9. Create the Firestore database, used to store the tag configurations: 

   This command currently requires `gcloud alpha`. If you don't have it, you need to first install it before creating the database. 

	```
	gcloud components install alpha.  

	gcloud alpha firestore databases create --project=$TAG_ENGINE_PROJECT --location=$TAG_ENGINE_REGION
	```

   Note that Firestore is not available in every region. Consult [this list](https://cloud.google.com/firestore/docs/locations) to see where it's available and choose the nearest region to `TAG_ENGINE_REGION`. It's perfectly fine for the Firestore region to be different from the `TAG_ENGINE_REGION`. <br><br> 
	
	
10. Firestore requires several composite indexes to service read requests:

   First, you must download a private key for your `$TAG_ENGINE_SA`:

   ```
	gcloud iam service-accounts keys create private_key.json --iam-account=$TAG_ENGINE_SA
	export GOOGLE_APPLICATION_CREDENTIALS="private_key.json"
   ```

   Second, create the composite indexes which are needed for serving multiple read requests:

   ```
	pip install google-cloud-firestore
	cd deploy
	python create_indexes.py $TAG_ENGINE_PROJECT
	cd ..
   ```

   Note: the above script is expected to run for 10-12 minutes. As the indexes get created, you will see them show up in the Firestore console. There should be about 36 indexes. <br><br> 
	

11. Build and deploy the Cloud Run services:

   There is one service in Cloud Run for the API (tag-engine-api) and another service in Cloud Run for the UI (tag-engine-ui). They are both built from the same code base. 

   ```
	gcloud run deploy tag-engine-api \
		--source . \
		--platform managed \
		--region $TAG_ENGINE_REGION \
		--no-allow-unauthenticated \
		--ingress=all \
		--memory=1024Mi \
		--service-account=$TAG_ENGINE_SA
   ```

   To deploy the UI service without IAP: 
   
   ```
	gcloud run deploy tag-engine-ui \
		--source . \
		--platform managed \
		--region $TAG_ENGINE_REGION \
		--allow-unauthenticated \
		--ingress=all \
		--memory=1024Mi \
		--service-account=$TAG_ENGINE_SA
   ``` 
 
 <br> 


12. Set the `SERVICE_URL` environment variable:

   If you are deploying the API, run:

   ```
	export API_SERVICE_URL=`gcloud run services describe tag-engine-api --format="value(status.url)"`
	gcloud run services update tag-engine-api --set-env-vars SERVICE_URL=$API_SERVICE_URL
   ```

   If you are deploying the UI, run:
   
   ```
	export UI_SERVICE_URL=`gcloud run services describe tag-engine-ui --format="value(status.url)"`
	gcloud run services update tag-engine-ui --set-env-vars SERVICE_URL=$UI_SERVICE_URL
   ```

<br> 


This completes the manual setup of Tag Engine. Please consult [Part 2](https://github.com/GoogleCloudPlatform/datacatalog-tag-engine#testa) and [Part 3](https://github.com/GoogleCloudPlatform/datacatalog-tag-engine#testb) of [README.md](https://github.com/GoogleCloudPlatform/datacatalog-tag-engine/blob/cloud-run/README.md) for testing instructions. 

<br><br>
