## Tag Engine
This repository contains the Tag Engine application, which is an open-source extension to Google Cloud's Data Catalog. Tag Engine automates the tagging of BigQuery and Cloud Storage assets. 

### Documentation

* If you are new to Tag Engine, start with [this tutorial](https://cloud.google.com/architecture/tag-engine-and-data-catalog). 
* To learn about Tag Engine's API methods, read the [API reference guide](https://github.com/GoogleCloudPlatform/datacatalog-tag-engine/blob/main/docs/api_reference.md). 
* To learn about Tag Engine's UI features, read the [UI guide](https://github.com/GoogleCloudPlatform/datacatalog-tag-engine/blob/main/docs/ui_guide.md). 
* To upgrade your existing Tag Engine installation, read the [upgrade guide](https://github.com/GoogleCloudPlatform/datacatalog-tag-engine/blob/main/docs/upgrade_guide.md). 

### Deployment Procedure

Tag Engine requires both Google App Engine and Firestore. It also assumes that you will be tagging assets in BigQuery or Google Cloud Storage. Follow the steps below to deploy the Tag Engine application in your Google Cloud project.

Note: In the deployment procedure below, we use one GCP project for running Tag Engine and Data Catalog and another project for storing data assets in BigQuery. If this is your first time running Tag Engine, you may want to keep everything in one project for simplicity. 

#### Step 1: Set the required environment variables
```
export TAG_ENGINE_PROJECT=tag-engine-vanilla-337221
export TAG_ENGINE_REGION=us-central
export BQ_PROJECT=warehouse-337221
export TAG_ENGINE_SA=${TAG_ENGINE_PROJECT}@appspot.gserviceaccount.com
export TERRAFORM_SA=terraform@${TAG_ENGINE_PROJECT}.iam.gserviceaccount.com
gcloud config set project $TAG_ENGINE_PROJECT
```

#### Step 2: Enable the following Google Cloud APIs
```
gcloud services enable iam.googleapis.com
gcloud services enable appengine.googleapis.com
```

#### Step 3: Clone this code repository
```
git clone https://github.com/GoogleCloudPlatform/datacatalog-tag-engine.git
```

#### Step 4: Set the input variables
```
cd datacatalog-tag-engine
cat > deploy/variables.tfvars << EOL
tag_engine_project="${TAG_ENGINE_PROJECT}"
bigquery_project="${BQ_PROJECT}"
app_engine_region="${TAG_ENGINE_REGION}"
app_engine_subregion="${TAG_ENGINE_SUB_REGION}"
EOL
```

Edit the four variables in `datacatalog-tag-engine/tagengine.ini`: 
```
[DEFAULT]
TAG_ENGINE_PROJECT = tag-engine-develop
QUEUE_REGION = us-central1
INJECTOR_QUEUE = tag-engine-injector-queue
WORK_QUEUE = tag-engine-work-queue
```

#### Step 5: Create the Firestore database and deploy the App Engine application
```
gcloud alpha firestore databases create --project=$TAG_ENGINE_PROJECT --region=$TAG_ENGINE_REGION     
gcloud app create --project=$TAG_ENGINE_PROJECT --region=$TAG_ENGINE_REGION
gcloud app deploy datacatalog-tag-engine/app.yaml
```

#### Step 6: Secure App Engine with firewall rules  
```
gcloud app firewall-rules create 100 --action ALLOW --source-range [IP_RANGE]
gcloud app firewall-rules update default --action deny
```

Alternatively, control access to App Engine by user identity (instead of IP address) with [Identity-Aware Proxy (IAP)](https://cloud.google.com/iap/docs/concepts-overview). 

#### Step 7: Run the Terraform scripts
``` 
gcloud auth application-default login
cd datacatalog-tag-engine/deploy
terraform init
terraform apply -var-file=variables.tfvars
```  

Note: The deployment can take up to one hour due to the large number of index builds. There are 24 Firestore indexes that get created sequentially to avoid build failures. 

#### Step 8: Launch the Tag Engine UI
```
gcloud app browse
```

Hint: read [this tutorial](https://cloud.google.com/architecture/tag-engine-and-data-catalog) to learn about Tag Engine's static and dynamic tag configurations. <br><br>


### Common UI Commands:

* Open the Tag Engine UI:<br>
`gcloud app browse`

* Consult the App Engine logs if you encounter an error while using Tag Engine:<br>
`gcloud app logs tail -s default`


### Common API Commands:

* Create a static asset tags through API:<br>
`curl -X POST [TAG ENGINE URL]/static_asset_tags -d @examples/static_asset_configs/static_asset_create_auto_bq.json`

* Create a dynamic table tags through API:<br>
`curl -X POST [TAG ENGINE URL]/dynamic_table_tags -d @examples/dynamic_table_configs/dynamic_table_create_auto.json`

* Create a dynamic column tags through API:<br>
`curl -X POST [TAG ENGINE URL]/dynamic_column_tags -d @examples/dynamic_column_configs/dynamic_column_create_auto.json`

* Create a glossary asset tags through API:<br>
`curl -X POST [TAG ENGINE URL]/glossary_asset_tags -d @examples/glossary_asset_configs/glossary_asset_create_ondemand_bq.json`

* Create a sensitive column tags through API:<br>
`curl -X POST [TAG ENGINE URL]/sensitive_column_tags -d @examples/sensitive_column_configs/sensitive_column_create_auto.json`

* Create Data Catalog entries through API:<br>
`curl -X POST [TAG ENGINE URL]/entries -d @examples/entry_configs/entry_create_auto.json`

* Restore tags from metadata export through API:<br>
`curl -X POST [TAG ENGINE URL]/restore_tags -d @examples/restore_configs/restore_table_tags.json`

* Import tags from CSV through API:<br>
`curl -X POST [TAG ENGINE URL]/import_tags -d @examples/import_configs/import_column_tags.json`

* Get the status of a job through API:<br>
`curl -X POST [TAG ENGINE URL]/get_job_status -d '{"job_uuid":"47aa9460fbac11ecb1a0190a014149c1"}'`

