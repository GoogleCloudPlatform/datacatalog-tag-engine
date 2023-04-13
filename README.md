## Tag Engine 2.0
This branch contains the Tag Engine 2.0 application, an early release of Tag Engine v2 that is hosted on Cloud Run (instead of App Engine) and is [VPC-SC compliant](https://cloud.google.com/vpc-service-controls/docs/supported-products). Tag Engine 2.0 supports authentication and the ability for multiple teams to securely tag their own data assets. 

Tag Engine is an open-source extension to Google Cloud's Data Catalog. Tag Engine automates the tagging of BigQuery tables and views as well as data lake files in Cloud Storage. You create a configuration, which contains SQL expressions that define how to populate the fields in the tags. Tag Engine runs the configuration either on demand or on a pre-defined schedule.

### Deployment Procedure

1. Create (or designate) three service accounts:

- A service account that runs the Tag Engine Cloud Run service, referred to below as `CLOUD_RUN_SA`. 
- A service account that performs the tagging in Data Catalog, and sourcing the contents of those tags from BigQuery, referred to below as `TAG_CREATOR_SA`. 
- A service account that interacts with the Tag Engine API, referred to below as `CLIENT_SA`. 


2. Set environment variables:

```
export TAG_ENGINE_PROJECT="tag-engine-run"
export TAG_ENGINE_REGION="us-central1"

export CLOUD_RUN_SA="<ID>@<PROJECT>.iam.gserviceaccount.com"     # used for running the Tag Engine service
export TAG_CREATOR_SA="<ID>@<PROJECT>.iam.gserviceaccount.com"   # used for running BQ queries and creating DC tags
export CLIENT_SA="<ID>@<PROJECT>.iam.gserviceaccount.com"        # used for calling the Tag Engine API from a script
```

3. Open `tagengine.ini` and set the values of these 6 variables:

```
TAG_ENGINE_PROJECT  
CLOUD_RUN_ACCOUNT
TAG_CREATOR_ACCOUNT
QUEUE_REGION
BIGQUERY_REGION
ENABLE_AUTH  
```

4. Enable the required APIs:

`gcloud config set project $TAG_ENGINE_PROJECT`

```
gcloud services enable iam.googleapis.com
gcloud services enable cloudresourcemanager.googleapis.com
gcloud services enable firestore.googleapis.com
gcloud services enable cloudtasks.googleapis.com
gcloud services enable cloudscheduler.googleapis.com
gcloud services enable datacatalog.googleapis.com
```

5. Create the Firestore database: 

`gcloud alpha firestore databases create --project=$TAG_ENGINE_PROJECT --region=$TAG_ENGINE_REGION`


6. Create the Firestore indexes:

`python deploy/create_indexes.py $TAG_ENGINE_PROJECT`

This script is expected to take ~10 minutes. There are 30+ indexes that must be built in Firestore. 


7. Build and deploy the Cloud Run service:

```
gcloud beta run deploy tag-engine \
	--source . \
	--platform managed \
	--region $TAG_ENGINE_REGION \
	--no-allow-unauthenticated \
	--ingress=all \
	--service-account=$CLOUD_RUN_SA
```

8. Set Cloud Run environment variable:

```
export SERVICE_URL=`gcloud run services describe tag-engine --format="value(status.url)"`
gcloud run services update tag-engine --set-env-vars SERVICE_URL=$SERVICE_URL
```

9. Create two custom roles:

```
gcloud iam roles create BigQuerySchemaUpdate \
	 --project $TAG_ENGINE_PROJECT \
	 --title BigQuerySchemaUpdate \
	 --description "Update table schema with policy tags" \
	 --permissions bigquery.tables.setCategory
```

```
gcloud iam roles create PolicyTagReader \
	--project $TAG_ENGINE_PROJECT \
	--title BigQuerySchemaUpdate \
	--description "Read Policy Tag Taxonomy" \
	--permissions datacatalog.taxonomies.get,datacatalog.taxonomies.list
```
	
10. Grant the required roles:

```
gcloud projects add-iam-policy-binding $TAG_ENGINE_PROJECT \
	--member='serviceAccount:$CLOUD_RUN_SA' \
	--role='projects/$TAG_ENGINE_PROJECT/roles/cloudtasks.enqueuer' \
	--role='projects/$TAG_ENGINE_PROJECT/roles/cloudtasks.taskRunner' \
	--role='projects/$TAG_ENGINE_PROJECT/roles/datastore.user' \
	--role='projects/$TAG_ENGINE_PROJECT/roles/run.invoker' 
```
```
gcloud projects add-iam-policy-binding $TAG_ENGINE_PROJECT \
	--member='serviceAccount:$TAG_CREATOR_SA' \
	--role='projects/$TAG_ENGINE_PROJECT/roles/datacatalog.tagEditor' \
	--role='projects/$TAG_ENGINE_PROJECT/roles/datacatalog.tagTemplateUser' \
	--role='projects/$TAG_ENGINE_PROJECT/roles/datacatalog.tagTemplateViewer' \
	--role='projects/$TAG_ENGINE_PROJECT/roles/datacatalog.viewer' \
	--role='projects/$TAG_ENGINE_PROJECT/roles/bigquery.dataEditor' \
	--role='projects/$TAG_ENGINE_PROJECT/roles/bigquery.jobUser' \
	--role='projects/$TAG_ENGINE_PROJECT/roles/bigquery.metadataViewer' \	  
	--role='projects/$TAG_ENGINE_PROJECT/roles/BigQuerySchemaUpdate' \
	--role='projects/$TAG_ENGINE_PROJECT/roles/PolicyTagReader' \
```

```
gcloud iam service-accounts add-iam-policy-binding $TAG_CREATOR_SA \
	--member=serviceAccount:$CLOUD_RUN_SA --role=roles/iam.serviceAccountUser
```

```
gcloud iam service-accounts add-iam-policy-binding $TAG_CREATOR_SA \
    --member=serviceAccount:$CLOUD_RUN_SA --role=roles/iam.serviceAccountTokenCreator 
```

```
gcloud iam service-accounts add-iam-policy-binding $CLOUD_RUN_SA \
	--member=serviceAccount:$CLOUD_RUN_SA --role roles/iam.serviceAccountUser
```

```
gcloud iam service-accounts add-iam-policy-binding $TAG_CREATOR_SA \
    --member=serviceAccount:$CLIENT_SA --role=roles/iam.serviceAccountUser 
```

```
gcloud run services add-iam-policy-binding tag-engine \
    --member=serviceAccount:$CLIENT_SA --role=roles/run.invoker \
    --region=$TAG_ENGINE_REGION	
```
	
11. Test the setup by creating a couple of simple configs (static and dynamic tags):

- Create the data_governance tag template: <br>
		`git clone https://github.com/GoogleCloudPlatform/datacatalog-templates.git` <br>
		`cd datacatalog-templates` <br>
		`python create_template.py $TAG_ENGINE_PROJECT $TAG_ENGINE_REGION data_governance.yaml` <br><br>
		
- Create a static asset config: <br>
		a) open `tests/configs/static_asset/static_asset_ondemand_bq.json` and update the `template_project`, `template_region`, and `included_assets_uris` values. <br>
		b) open `tests/configs/static_asset/create_static_config_trigger_job.py`
		and update the variable `TAG_ENGINE_URL` on line 11 to your Cloud Run service URL from step 8. <br>
		c) set the environment variable `GOOGLE_APPLICATION_CREDENTIALS` to the keyfile for your `$CLIENT_SA`
		   e.g. `export GOOGLE_APPLICATION_CREDENTIALS="/Users/scohen/keys/python-client.json"` <br>
		d) run the script: `python tests/scripts/create_static_config_trigger_job.py` <br>
		e) if the job succeeds, go to the Data Catalog UI and check out the tags. If the job fails, go to the Cloud Run UI and open the logs for the Tag Engine service to see the cause of the error. <br><br>		
		
- Create a dynamic table config: <br>
		a) open `tests/configs/dynamic_table/dynamic_table_ondemand_bq.json` and update the `template_project`, `template_region`, and `included_assets_uris` values. <br>
		b) open `tests/configs/dynamic_table/create_dynamic_table_config_trigger_job.py`
		and update the variable `TAG_ENGINE_URL` on line 11 to your Cloud Run service URL from step 8. <br>
		c) set the environment variable `GOOGLE_APPLICATION_CREDENTIALS` to the keyfile for your `$CLIENT_SA`
		   e.g. `export GOOGLE_APPLICATION_CREDENTIALS="/Users/scohen/keys/python-client.json"`. <br>
		d) run the script: `python tests/scripts/create_dynamic_table_config_trigger_job.py` <br>
		e) If the job succeeds, go to the Data Catalog UI and check out the tags. If the job fails, go to the Cloud Run UI and open the logs for your Tag Engine service to see the cause of the error.	<br>   
