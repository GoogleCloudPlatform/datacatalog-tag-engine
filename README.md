## Tag Engine
This repository contains the Tag Engine application, which is an open-source extension to Google Cloud's Data Catalog. Tag Engine automates the tagging of BigQuery and Cloud Storage assets. 

### Documentation

* If you are new to Tag Engine, start with [this tutorial](https://cloud.google.com/architecture/tag-engine-and-data-catalog). 
* To learn about Tag Engine's API methods, read the [API reference guide](https://github.com/GoogleCloudPlatform/datacatalog-tag-engine/blob/main/docs/api_reference.md). 
* To learn about Tag Engine's UI features, read the [UI reference guide](https://github.com/GoogleCloudPlatform/datacatalog-tag-engine/blob/main/docs/ui_reference.md). 
* To upgrade your existing Tag Engine installation, read the [upgrade guide](https://github.com/GoogleCloudPlatform/datacatalog-tag-engine/blob/main/docs/upgrade_guide.md). 

### Deployment Procedure

Tag Engine requires both Google App Engine and Firestore. It also assumes that you will be tagging assets in BigQuery or Google Cloud Storage. Follow the steps below to deploy the Tag Engine application.  

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

Alternatively, you can use [IAP](https://cloud.google.com/iap/docs/concepts-overview) to control access to App Engine. 


#### Step 7: (Optional) Create a service account for running the Terraform scripts
```                
gcloud iam service-accounts create terraform
gcloud iam service-accounts keys create key.json --iam-account=$TERRAFORM_SA 
gcloud projects add-iam-policy-binding $TAG_ENGINE_PROJECT --member=serviceAccount:${TERRAFORM_SA} --role=roles/owner
gcloud projects add-iam-policy-binding $BQ_PROJECT --member=serviceAccount:${TERRAFORM_SA} --role=roles/owner
gcloud auth activate-service-account $TERRAFORM_SA --key-file=/tmp/key.json
```

#### Step 8: Run the Terraform scripts
```  
cd datacatalog-tag-engine/deploy
terraform init
terraform apply
```  

#### Step 9: (Optional) Delete the Terraform service account
```
gcloud iam service-accounts delete $TERRAFORM_SA
rm /tmp/key.json
```

#### Step 10: Launch the Tag Engine UI
```
gcloud app browse
```

Hint: read [this tutorial](https://cloud.google.com/architecture/tag-engine-and-data-catalog) to learn about Tag Engine's static and dynamic tagging configurations. <br><br>


#### Common Commands:

* Bring up the UI:<br>
`gcloud app browse`

* Create a static tag configuration through the API:<br>
`curl -X POST [TAG ENGINE URL]/static_create -d @examples/static_configs/static_create_ondemand.json`

* Create a dynamic tag configuration through the API:<br>
`curl -X POST [TAG ENGINE URL]/dynamic_create -d @examples/dynamic_configs/dynamic_create_auto.json`

* Get the job status of a tag configuration through the API:<br>
`curl -X POST [TAG ENGINE URL]/get_job_status -d '{"job_uuid":"47aa9460fbac11ecb1a0190a014149c1"}'`

* Consult the App Engine logs if you encounter an error while using Tag Engine:<br>
`gcloud app logs tail -s default`

