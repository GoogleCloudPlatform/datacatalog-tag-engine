Tag Engine is a self-service tool that makes it easier for Data Stewards to create bulk metadata in Google Cloud’s [Data Catalog](https://cloud.google.com/data-catalog/docs/concepts/overview). It enables them to create tags for their BigQuery tables, views and columns based on simple <b>SQL expressions</b> and <b>file path expressions</b>. It also keeps their tags up-to-date in accordance to a schedule. 
<br><br>
The tool comes with a UI and API. Data Stewards normally use the UI because it gives them the agility and autonomy to tag at scale whereas Data Engineers prefer to interact with Tag Engine through the API. The screenshot below shows the creation of a simple dynamic config using the UI.  
<br><br>
![](https://github.com/GoogleCloudPlatform/datacatalog-tag-engine/blob/main/static/screenshot.png)

### Follow the steps below to deploy Tag Engine on Google Cloud. 

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

#### Step 9: Launch the Tag Engine UI:
```
gcloud auth login
gcloud app browse
```

#### Troubleshooting:

Consult the App Engine logs if you encounter an error while running Tag Engine:

```
gcloud app logs tail -s default
```

