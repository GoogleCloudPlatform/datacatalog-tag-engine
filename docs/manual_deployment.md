### <a name="setup"></a> Manual Deployment

This procedure deploys the Tag Engine v2 components by hand. The steps are carried out via gcloud and in some cases using the Google Cloud console. For the Terraform deployment, please consult [README.md](https://github.com/GoogleCloudPlatform/datacatalog-tag-engine/blob/cloud-run/README.md).<br>

1. Create (or designate) two service accounts:

   - A service account that runs the Tag Engine Cloud Run service, referred to below as `TAG_ENGINE_SA`. You should create this SA in the same project as where you are going to run the Cloud Run service(s) for Tag Engine. 
   - A service account that creates the metadata tags in Data Catalog and sources the contents of those tags from BigQuery or GCS, referred to below as `TAG_CREATOR_SA`. You should create this service account in your Data Catalog project. <br><br>


2. Define the environment variables to be used throughout the deployment:

	```
	export TAG_ENGINE_SA="<ID>@<TAG_ENGINE_PROJECT>.iam.gserviceaccount.com"  # Service account for running the Tag Engine Cloud Run service(s) 
	export TAG_CREATOR_SA="<ID>@<DATA_CATALOG_PROJECT>.iam.gserviceaccount.com" # Service account for creating the metadata tags from Tag Engine
	
	export TAG_ENGINE_PROJECT="<PROJECT>"     # GCP project id for running the Tag Engine Cloud Run service(s) and task queues
	export TAG_ENGINE_REGION="<REGION>"       # GCP region for running the Tag Engine Cloud Run service(s) and tasks queues 

	export FIRESTORE_PROJECT="<PROJECT>"     # GCP project id for running Firestore
	export FIRESTORE_REGION="<REGION>"       # GCP region of your Firestore database e.g. us-central1
	export FIRESTORE_DATABASE="(default)"    # The name of your Firestore database

	export DATA_CATALOG_PROJECT="<PROJECT>"  # GCP project id where you have created your tag templates
	export DATA_CATALOG_REGION="<REGION>"    # GCP region in which your tag templates reside

	export BIGQUERY_PROJECT="<PROJECT>"      # GCP project used by BigQuery data assets
	export BIGQUERY_REGION="<REGION>"        # GCP region in which data assets in BigQuery are stored, e.g. us-central1
	```

<b>The key benefit of decoupling `$TAG_ENGINE_SA` from `$TAG_CREATOR_SA` is to limit the scope of what a client is allowed to tag.</b> More specifically, when a client submits a request to Tag Engine, Tag Engine checks to see if they are authorized to use `TAG_CREATOR_SA` before processing their request. Note that a Tag Engine client can either be a user identity or a service account.  

If multiple teams want to share a single instance of Tag Engine and they own different assets in BigQuery, they can each have their own `TAG_CREATOR_SA` to prevent one team from tagging another team's assets. `TAG_CREATOR_SA` is set in the `tagengine.ini` file (next step) with the default account for the entire Tag Engine instance. Tag Engine clients can override the default `TAG_CREATOR_SA` when creating tag configurations by specifying a service_account attribute in the json request (as shown [here](https://github.com/GoogleCloudPlatform/datacatalog-tag-engine/blob/cloud-run/tests/configs/dynamic_table/dynamic_dataset_non_default_service_account.json)).  <br><br>   


3. Create an OAuth client ID for your Tag Engine web application: 

   - This step is only required if you are deploying the Tag Engine UI. Skip this step if you are only deploying the API. 

   - Designate a custom domain for your web application (e.g. tagengine.app). You can register a domain from GCP with [Cloud Domains](https://console.cloud.google.com/net-services/domains/) if you need one. If you don't want to use a custom domain, you can use the Cloud Run service URL instead. 

   - Create an OAuth client ID from API Credentials. Set the `Authorized redirect URI` to `https://[TAG_ENGINE_DOMAIN]/oauth2callback`, where [TAG_ENGINE_DOMAIN] is your actual domain name (e.g. `https://tagengine.app/oauth2callback`). If you are planning to use the Cloud Run service URL, you can leave this field empty for now, and populate it at the end once you know your Cloud Run service URL for the Tag Engine UI. 

   - Download the OAuth client secret and save the json file to the root of your local Tag Engine repository as `te_client_secret.json`.   

4. Open `tagengine.ini` and set the following variables in this file. 

	```
	TAG_ENGINE_ACCOUNT
	TAG_CREATOR_ACCOUNT
	TAG_ENGINE_PROJECT
	TAG_ENGINE_REGION
	FIRESTORE_PROJECT
	FIRESTORE_REGION
	FIRESTORE_DATABASE 
	BIGQUERY_REGION
	FILESET_REGION
	SPANNER_REGION
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

   - The `tagengine.ini` file also has two additional variables, `INJECTOR_QUEUE` and `WORK_QUEUE`. These determine the names of the cloud tasks queues. You do not need to change them. The queues are created in step 6 of this setup.   


5. Enable the required Google Cloud APIs:

	```
	gcloud config set project $TAG_ENGINE_PROJECT
	gcloud services enable iam.googleapis.com
	gcloud services enable run.googleapis.com
	gcloud services enable cloudresourcemanager.googleapis.com
	gcloud services enable artifactregistry.googleapis.com
	gcloud services enable cloudbuild.googleapis.com
	gcloud services enable cloudtasks.googleapis.com
	```	
	```
	gcloud config set project $FIRESTORE_PROJECT
	gcloud services enable firestore.googleapis.com
	```
	
	```
	gcloud config set project $DATA_CATALOG_PROJECT 
	gcloud services enable datacatalog.googleapis.com
	``` 

6. Create the two cloud task queues. The first queue is used to queue the entire job while the second is used to queue individual work items. If a task fails, a second one will get created due to `max-attempts=2`:

	```
	gcloud config set project $TAG_ENGINE_PROJECT

	gcloud tasks queues create tag-engine-injector-queue \
	--location=$TAG_ENGINE_REGION --max-attempts=2 --max-concurrent-dispatches=100

	gcloud tasks queues create tag-engine-work-queue \
	--location=$TAG_ENGINE_REGION --max-attempts=2 --max-concurrent-dispatches=100
	```

7. Create the Firestore database and indexes. 

	```
	gcloud config set project $FIRESTORE_PROJECT
	gcloud firestore databases create --database=$FIRESTORE_DATABASE --project=$FIRESTORE_PROJECT --location=$FIRESTORE_REGION
	```
	
	If you're not able to create the Firestore database in your preferred region, consult [the available](https://cloud.google.com/firestore/docs/locations) regions and choose the nearest region to what you set `TAG_ENGINE_REGION`. 
	
	```
	pip install google-cloud-firestore
	cd deploy
	python create_indexes.py $FIRESTORE_PROJECT $FIRESTORE_DATABASE
	cd ..
	```

    Creating the indexes can take a few minutes. As the indexes get created, you will see them show up in the Firestore console. There should be about 36 indexes in total. 

		
8. Grant the required IAM roles to the service accounts `$TAG_ENGINE_SA` and `$TAG_CREATOR_SA`:

	```
	gcloud projects add-iam-policy-binding $TAG_ENGINE_PROJECT \
	--member=serviceAccount:$TAG_ENGINE_SA \
	--role=roles/cloudtasks.enqueuer
	
	gcloud projects add-iam-policy-binding $TAG_ENGINE_PROJECT \
	--member=serviceAccount:$TAG_ENGINE_SA \
	--role=roles/cloudtasks.taskRunner

	gcloud projects add-iam-policy-binding $FIRESTORE_PROJECT \
	--member=serviceAccount:$TAG_ENGINE_SA \
	--role=roles/datastore.user

	gcloud projects add-iam-policy-binding $FIRESTORE_PROJECT \
	--member=serviceAccount:$TAG_ENGINE_SA \
	--role=roles/datastore.indexAdmin  

	gcloud projects add-iam-policy-binding $TAG_ENGINE_PROJECT \
	--member=serviceAccount:$TAG_ENGINE_SA \
	--role=roles/run.invoker
	```
	```
	gcloud projects add-iam-policy-binding $TAG_ENGINE_PROJECT \
	--member=serviceAccount:$TAG_CREATOR_SA \
	--role=roles/logging.viewer
	
	gcloud projects add-iam-policy-binding $TAG_ENGINE_PROJECT \
	--member=serviceAccount:$TAG_CREATOR_SA \
	--role=roles/bigquery.jobUser
	
	gcloud projects add-iam-policy-binding $DATA_CATALOG_PROJECT \
	--member=serviceAccount:$TAG_CREATOR_SA \
	--role=roles/datacatalog.tagEditor

	gcloud projects add-iam-policy-binding $DATA_CATALOG_PROJECT \
	--member=serviceAccount:$TAG_CREATOR_SA \
	--role=roles/datacatalog.tagTemplateUser
	
	gcloud projects add-iam-policy-binding $DATA_CATALOG_PROJECT \
	--member=serviceAccount:$TAG_CREATOR_SA \
	--role=roles/datacatalog.tagTemplateViewer

	gcloud projects add-iam-policy-binding $DATA_CATALOG_PROJECT \
	--member=serviceAccount:$TAG_CREATOR_SA \
	--role=roles/datacatalog.viewer

	gcloud projects add-iam-policy-binding $BIGQUERY_PROJECT \
	--member=serviceAccount:$TAG_CREATOR_SA \
	--role=roles/bigquery.dataEditor

	gcloud projects add-iam-policy-binding $BIGQUERY_PROJECT \
	--member=serviceAccount:$TAG_CREATOR_SA \
	--role=roles/bigquery.metadataViewer   
	```

9. Grant the necessary IAM roles to `$TAG_ENGINE_SA`:

	```
	gcloud iam service-accounts add-iam-policy-binding $TAG_ENGINE_SA \
	--member=serviceAccount:$TAG_ENGINE_SA --role roles/iam.serviceAccountUser --project $TAG_ENGINE_PROJECT 
	
	gcloud iam service-accounts add-iam-policy-binding $TAG_CREATOR_SA \
	--member=serviceAccount:$TAG_ENGINE_SA --role=roles/iam.serviceAccountUser --project $DATA_CATALOG_PROJECT

	gcloud iam service-accounts add-iam-policy-binding $TAG_CREATOR_SA \
	    --member=serviceAccount:$TAG_ENGINE_SA --role=roles/iam.serviceAccountViewer --project $DATA_CATALOG_PROJECT

	gcloud iam service-accounts add-iam-policy-binding $TAG_CREATOR_SA \
	    --member=serviceAccount:$TAG_ENGINE_SA --role=roles/iam.serviceAccountTokenCreator --project $DATA_CATALOG_PROJECT		 
	```

10. Optional step only needed if creating policy tags through Tag Engine. If you are not planning to use the `create_sensitive_config` endpoint, you can skip this step:

	```
	gcloud config set project $BIGQUERY_PROJECT
	
	gcloud iam roles create BigQuerySchemaUpdate \
	 --project $BIGQUERY_PROJECT \
	 --title BigQuerySchemaUpdate \
	 --description "Update table schema with policy tags" \
	 --permissions bigquery.tables.setCategory
	
	gcloud projects add-iam-policy-binding $BIGQUERY_PROJECT \
	--member=serviceAccount:$TAG_CREATOR_SA \
	--role=projects/$BIGQUERY_PROJECT/roles/BigQuerySchemaUpdate

	gcloud config set project $DATA_CATALOG_PROJECT
	
	gcloud iam roles create PolicyTagReader \
	--project $DATA_CATALOG_PROJECT \
	--title PolicyTagReader \
	--description "Read Policy Tag Taxonomy" \
	--permissions datacatalog.taxonomies.get,datacatalog.taxonomies.list
	
	gcloud projects add-iam-policy-binding $DATA_CATALOG_PROJECT \
	--member=serviceAccount:$TAG_CREATOR_SA \
	--role=projects/$DATA_CATALOG_PROJECT/roles/PolicyTagReader
	```

11. Optional step only needed if creating tags on Spanner data assets. If you don't have a Spanner database, you can skip this step:
	
	```
	gcloud iam roles create SpannerTagReadWrite \
	--project $SPANNER_PROJECT \
	--title SpannerTagReadWrite \
	--description "Read and Update Spanner metadata" \
	--permissions spanner.databases.get,spanner.databases.updateTag,spanner.instances.updateTag
	
	gcloud projects add-iam-policy-binding $SPANNER_PROJECT \
	--member=serviceAccount:$TAG_CREATOR_SA \
	--role=projects/$SPANNER_PROJECT/roles/SpannerTagReadWrite
	```

12.  Optional step needed only if creating tags from CSV files:

	Creating tags from CSV files requires `$TAG_CREATOR_SA` to have the `storage.buckets.get` permission on the GCS bucket in which the CSV files are stored. You can either create a custom role with this permission or assign the `storage.legacyBucketReader` role:
	
	```
	gcloud storage buckets add-iam-policy-binding gs://<BUCKET> \
		--member=serviceAccount:$TAG_CREATOR_SA' \
		--role=roles/storage.legacyBucketReader
	```
	

13. Build and deploy the Cloud Run services:

	There is one Cloud Run service for the API (`tag-engine-api`) and another for the UI (`tag-engine-ui`). They are both built from the same code base. You can build either one or the other, depending on your needs. The majority of Tag Engine customers use the API service and a few of them also use the UI service.  

	```
	gcloud config set project $TAG_ENGINE_PROJECT
	
	gcloud run deploy tag-engine-api \
	--source . \
	--platform managed \
	--project $TAG_ENGINE_PROJECT \
	--region $TAG_ENGINE_REGION \
	--no-allow-unauthenticated \
	--ingress=all \
	--memory=4G \
	--timeout=60m \
	--service-account=$TAG_ENGINE_SA

	gcloud run deploy tag-engine-ui \
	--source . \
	--platform managed \
	--project $TAG_ENGINE_PROJECT \
	--region $TAG_ENGINE_REGION \
	--allow-unauthenticated \
	--ingress=all \
	--memory=4G \
	--timeout=60m \
	--service-account=$TAG_ENGINE_SA
	``` 
 
14. Set the `SERVICE_URL` environment variable:

	If you are deploying the API, you also need to set the environment variable SERVICE_URL on `tag-engine-api`:

	```
	gcloud config set project $TAG_ENGINE_PROJECT
	export API_SERVICE_URL=`gcloud run services describe tag-engine-api --format="value(status.url)"`
	gcloud run services update tag-engine-api --set-env-vars SERVICE_URL=$API_SERVICE_URL
	```

	If you are deploying the UI, you also need to set the environment variable SERVICE_URL on `tag-engine-ui`:

	```
	gcloud config set project $TAG_ENGINE_PROJECT
	export UI_SERVICE_URL=`gcloud run services describe tag-engine-ui --format="value(status.url)"`
	gcloud run services update tag-engine-ui --set-env-vars SERVICE_URL=$UI_SERVICE_URL
	```


This completes the manual setup for Tag Engine. Please consult [Part 2](https://github.com/GoogleCloudPlatform/datacatalog-tag-engine#testa) and [Part 3](https://github.com/GoogleCloudPlatform/datacatalog-tag-engine#testb) for testing your installation and further steps. 

<br><br>
