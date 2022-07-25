## Tag Engine
This repository contains the Tag Engine application, which is described in [this guide](https://cloud.google.com/architecture/tag-engine-and-data-catalog). 

### Usage

Follow the steps below to deploy Tag Engine on Google Cloud: 

#### Step 1: Set the required environment variables:
```
export TAG_ENGINE_PROJECT=tag-engine-vanilla-337221
export TAG_ENGINE_REGION=us-central
export BQ_PROJECT=warehouse-337221
export TAG_ENGINE_SA=${TAG_ENGINE_PROJECT}@appspot.gserviceaccount.com
export TERRAFORM_SA=terraform@${TAG_ENGINE_PROJECT}.iam.gserviceaccount.com
gcloud config set project $TAG_ENGINE_PROJECT
```

#### Step 2: Enable the following Google Cloud APIs:
```
gcloud services enable iam.googleapis.com
gcloud services enable appengine.googleapis.com
```

#### Step 3: Clone this code repository:
```
git clone https://github.com/GoogleCloudPlatform/datacatalog-tag-engine.git
```

#### Step 4: Set the input variables:
`datacatalog-tag-engine/deploy/variables.tf`: is used to define GCP projects, regions, and Google Cloud APIs, which are used during the deployment process.  
`datacatalog-tag-engine/tagengine.ini`: is used to define the Cloud Task queues, which are used to process tag write and update requests. 


#### Step 5: Create the database and deploy the application:
```
gcloud alpha firestore databases create --project=$TAG_ENGINE_PROJECT --region=$TAG_ENGINE_REGION     
gcloud app create --project=$TAG_ENGINE_PROJECT --region=$TAG_ENGINE_REGION
gcloud app deploy datacatalog-tag-engine/app.yaml

```

#### Step 6: Create a service account for running the Terraform scripts:
```                
gcloud iam service-accounts create terraform
gcloud iam service-accounts keys create key.json --iam-account=$TERRAFORM_SA 
gcloud projects add-iam-policy-binding $TAG_ENGINE_PROJECT --member=serviceAccount:${TERRAFORM_SA} --role=roles/owner
gcloud projects add-iam-policy-binding $BQ_PROJECT --member=serviceAccount:${TERRAFORM_SA} --role=roles/owner
gcloud auth activate-service-account $TERRAFORM_SA --key-file=/tmp/key.json
```

#### Step 7: Run the Terraform scripts:
```  
cd datacatalog-tag-engine/deploy
terraform init
terraform apply
```  

#### Step 8:  Delete the Terraform service account:
```
gcloud iam service-accounts delete $TERRAFORM_SA
rm /tmp/key.json
```

#### Step 9: Start using Tag Engine:

Read [this tutorial](https://cloud.google.com/architecture/tag-engine-and-data-catalog) to start using Tag Engine. 


#### Helpful Commands:

Bring up the UI:
`gcloud auth login`
`gcloud app browse`

Create a static config through the API:
`curl -X POST [TAG ENGINE URL]/static_create -d @examples/dynamic_configs/static_create_ondemand.json`

Create a dynamic config through the API:
`curl -X POST [TAG ENGINE URL]/dynamic_create -d @examples/dynamic_configs/dynamic_create_auto.json`

Get the job status through the API:
`curl -X POST [TAG ENGINE URL]/get_job_status -d '{"job_uuid":"47aa9460fbac11ecb1a0190a014149c1"}'`

Consult the App Engine logs if you encounter an error while using Tag Engine:
`gcloud app logs tail -s default`

