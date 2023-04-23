## Tag Engine 2.0
This branch contains the Tag Engine 2.0 application, an early release of Tag Engine v2 that is hosted on Cloud Run (instead of App Engine) and is [VPC-SC compliant](https://cloud.google.com/vpc-service-controls/docs/supported-products). Tag Engine 2.0 supports authentication and the ability for multiple teams to securely tag their own data assets. 

Tag Engine is an open-source extension to Google Cloud's Data Catalog. Tag Engine automates the tagging of BigQuery tables and views as well as data lake files in Cloud Storage. You create a configuration, which contains SQL expressions that define how to populate the fields in the tags. Tag Engine runs the configuration either on demand or on a pre-defined schedule.

### Deployment Procedure

1. Create (or designate) three service accounts:

- A service account that runs the Tag Engine Cloud Run service, referred to below as `CLOUD_RUN_SA`. 
- A service account that performs the tagging in Data Catalog, and sourcing the contents of those tags from BigQuery, referred to below as `TAG_CREATOR_SA`. 
- A service account that interacts with the Tag Engine API, referred to below as `CLIENT_SA`. 


2. Set seven environment variables which will be used throughout the setup procedure:

```
export TAG_ENGINE_PROJECT="<PROJECT>"  # GCP project id for running the Tag Engine service
export TAG_ENGINE_REGION="<REGION>"    # GCP region for running Tag Engine service, e.g. us-central1

export BIGQUERY_PROJECT="<PROJECT>"    # GCP project used by BigQuery data assets, can be equal to TAG_ENGINE_PROJECT. This variable is used for setting permissions in steps 10 and 11 
export BIGQUERY_REGION="<REGION>"      # GCP region in which data assets in BigQuery are stored, e.g. us-central1

export CLOUD_RUN_SA="<ID>@<PROJECT>.iam.gserviceaccount.com"     # email of your Cloud Run service account for running Tag Engine service
export TAG_CREATOR_SA="<ID>@<PROJECT>.iam.gserviceaccount.com"   # email of your Tag creator service account for running BQ queries and creating DC tags
export CLIENT_SA="<ID>@<PROJECT>.iam.gserviceaccount.com"        # email of your client service account for calling the Tag Engine API from a script
```

3. Open `tagengine.ini` and set the following six variables in the file. The first five should be equal to the environment variables you set above in step 2:

```
TAG_ENGINE_PROJECT  
CLOUD_RUN_ACCOUNT
TAG_CREATOR_ACCOUNT
QUEUE_REGION
BIGQUERY_REGION
ENABLE_AUTH  
```

Note: `ENABLE_AUTH` is a boolean. When set to True, Tag Engine verifies that the client is authorized to use the `TAG_CREATOR_SA` prior to processing requests. 


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

10. Create two custom roles (required by the SENSITIVE_COLUMN_CONFIG):

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
	--title BigQuerySchemaUpdate \
	--description "Read Policy Tag Taxonomy" \
	--permissions datacatalog.taxonomies.get,datacatalog.taxonomies.list
```
	
11. Grant the required roles to `CLOUD_RUN_SA`, `TAG_CREATOR_SA`, and `CLIENT_SA`:

```
gcloud projects add-iam-policy-binding $TAG_ENGINE_PROJECT \
	--member=serviceAccount:$CLOUD_RUN_SA \
	--role=roles/cloudtasks.enqueuer \
	--role=roles/cloudtasks.taskRunner \
	--role=roles/datastore.user \
	--role=roles/run.invoker 
```
```
gcloud projects add-iam-policy-binding $TAG_ENGINE_PROJECT \
	--member=serviceAccount:$TAG_CREATOR_SA \
	--role=roles/datacatalog.tagEditor \
	--role=roles/datacatalog.tagTemplateUser \
	--role=roles/datacatalog.tagTemplateViewer \
	--role=roles/datacatalog.viewer
```
```
gcloud projects add-iam-policy-binding $BIGQUERY_PROJECT \
	--role=roles/bigquery.dataEditor \
	--role=roles/bigquery.jobUser \
	--role=roles/bigquery.metadataViewer \	  
	--role=projects/$BIGQUERY_PROJECT/roles/BigQuerySchemaUpdate \
	--role=roles/PolicyTagReader \
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

Note: If you plan to create tags from CSV files, you also need to ensure that `TAG_CREATOR_SA` has the 
`storage.buckets.get` permission on the GCS bucket where the CSV files are stored. To do that, you can create a custom role with 
this permission or assign the `storage.legacyBucketReader` role:

```
gcloud storage buckets add-iam-policy-binding gs://<BUCKET> \
	--member=serviceAccount:$TAG_CREATOR_SA' \
	--role=roles/storage.legacyBucketReader
```

12. This is an optional step. If you plan to create Tag Engine configs which auto refresh the tags, you'll also need to make a Cloud Scheduler entry to trigger the tag updates:

```
gcloud services enable cloudscheduler.googleapis.com

gcloud scheduler jobs create http scheduled_auto_updates1 \
	--description="Tag Engine scheduled jobs" \
	--location=$TAG_ENGINE_REGION --time-zone=America/Chicago \
	--schedule="0 */1 * * *" --uri="${SERVICE_URL}/scheduled_auto_updates" 	\
	--http-method=POST \
	--headers oauth_token=$OAUTH_TOKEN \
	--oidc-service-account-email=$CLIENT_SA \
	--oidc-token-audience=$SERVICE_URL 
```

This command created a Cloud Scheduler entry that will trigger tag updates every hour. If you want the tag updates to occur on a different schedule, you can adjust the value of the `schedule` parameter in the above command. 

Note: to generate the `OAUTH_TOKEN`, you'll need to choose an account which has privileges to use `TAG_CREATOR_SA` and then run these two commands:

```
gcloud auth application-default login
export OAUTH_TOKEN=$(gcloud auth application-default print-access-token)
```

13. Test your Tag Engine setup by creating a couple of simple configs (static and dynamic tags):

- Create the data_governance tag template: <br>
		`git clone https://github.com/GoogleCloudPlatform/datacatalog-templates.git` <br>
		`cd datacatalog-templates` <br>
		`python create_template.py $TAG_ENGINE_PROJECT $TAG_ENGINE_REGION data_governance.yaml` <br>
		
- Create a simple static asset config: <br>
		a) open `tests/configs/static_asset/static_asset_ondemand_bq.json` and update the `template_project`, `template_region`, and `included_assets_uris` values. <br>
		b) open `tests/scripts/static_asset/create_static_config_trigger_job.py`
		and update the variable `TAG_ENGINE_URL` on line 11 to your Cloud Run service URL from step 8. <br>
		c) set the environment variable `GOOGLE_APPLICATION_CREDENTIALS` to the keyfile of your `$CLIENT_SA`
		   e.g. `export GOOGLE_APPLICATION_CREDENTIALS="client-account.json"` <br>
		d) run the script: `python tests/scripts/create_static_config_trigger_job.py` <br>
		e) if the job succeeds, open the Data Catalog UI and check out the resulting tags. If the job fails, open the Cloud Run UI and review the logs for the Tag Engine service to determine the cause of the error. <br>		
		
- Create a dynamic table config: <br>
		a) open `tests/configs/dynamic_table/dynamic_table_ondemand_bq.json` and update the `template_project`, `template_region`, and `included_assets_uris` values. <br>
		b) open `tests/scripts/dynamic_table/create_dynamic_table_config_trigger_job.py`
		and update the variable `TAG_ENGINE_URL` on line 11 to your Cloud Run service URL from step 8. <br>
		c) set the environment variable `GOOGLE_APPLICATION_CREDENTIALS` to the keyfile of your `$CLIENT_SA`
		   e.g. `export GOOGLE_APPLICATION_CREDENTIALS="client-account.json"`. <br>
		d) run the script: `python tests/scripts/create_dynamic_table_config_trigger_job.py` <br>
		e) If the job succeeds, open the Data Catalog UI and check out the resulting tags. If the job fails, open the Cloud Run UI and review the logs for your Tag Engine service to determine the cause of the error.	<br> 
					
14. Congrats! If you made it this far, you've completed the setup and are ready to create your own Tag Engine configs. For additional examples, check out `tests/configs/*` and `tests/scripts/*`. If you are new to Tag Engine, you may also want to walk through [this tutorial](https://cloud.google.com/architecture/tag-engine-and-data-catalog). Note that the tutorial is for Tag Engine v1 (as opposed to v2), but it will give you a sense of how Tag Engine works. We plan to publish a second tutorial for Tag Engine v2 soon. Stay tuned!  
