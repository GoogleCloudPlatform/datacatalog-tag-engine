Tag Engine is a self-service tool that makes it easier for Data Stewards to create business metadata in Google Cloud’s [Data Catalog](https://cloud.google.com/data-catalog/docs/concepts/overview). It enables them to create <b>bulk tags</b> for their BigQuery tables, columns, and views based on simple <b>SQL expressions</b> and <b>file path expressions</b>. It also refreshes their tags on a schedule or when triggered via the Tag Engine API. 
<br><br>
The tool comes with a friendly UI so that business users, who are often the Data Stewards in their organization, can be productive at tagging without much training. The screenshot below shows the creation of a dynamic tag in Tag Engine.  
<br><br>
![](https://github.com/GoogleCloudPlatform/datacatalog-tag-engine/blob/main/static/screenshot.png)

## Follow the steps below to set up Tag Engine on Google Cloud. 

### Step 1: (Required) Clone this repo:
```
git clone https://github.com/GoogleCloudPlatform/datacatalog-tag-engine.git
```

### Step 2: (Required) Create Firestore database:
1. Go to GCP Console, select the project that you want to run Tag Engine from, and click on Firestore.
2. In the Firestore Get Started page, click the SELECT NATIVE MODE button. 
3. Choose a database location from the drop-down (e.g. us-east1).
4. Click the CREATE DATABASE button. 
5. Once your database has been created, you should see "Your database is ready to go. Just add data".  


### Step 3: (Required) Create Firestore indexes:
#### Firestore indexes must be created prior to running Tag Engine. Creating the indexes can take a few minutes. 
1. Run the create_indexes.py script as follows:
```
cd datacatalog-tag-engine/setup
python create_indexes.py $PROJECT_ID
```
2. Go to the Firestore console and click on the indexes tab. Make sure that all 12 indexes have been created before proceeding. You will see a green checkbox next to the index once it's ready.  


### Step 4: (Optional) Set up Tag Engine to tag assets in multiple GCP projects:
1. Go to the IAM console and find the App Engine service account. This account is named [PROJECT_ID]@appspot.gserviceaccount.com. 
2. Switch to the project you would like to grant Tag Engine access to so that you can create tags for data assets in that project. 
3. Click Add member and enter the Service Account from step 1 into the New members field (e.g. [PROJECT_ID]@appspot.gserviceaccount.com). 
4. Assign these 5 roles: BigQuery Job User, BigQuery Metadata Viewer, BigQuery Data Viewer, Data Catalog Tag Editor, and Data Catalog Viewer. 
5. Click Save. 
6. Repeat steps 2-5 for each project you would like Tag Engine to be able to access for tagging.  


### Step 5: (Optional) Create an App Engine cloud task queue:
#### The cloud task queue is used to run scheduled dynamic tag updates. This step is only required if you want Tag Engine schedule your dynamic table updates. 
```
gcloud config set project $PROJECT_ID
gcloud tasks queues create tag-engine-queue
gcloud tasks queues describe tag-engine-queue
```

### Step 6: (Optional) Set config variables:
#### This step is required if you want Tag Engine to schedule your dynamic tag updates and/or you want Tag Engine to propagate your tags from tables to views.  

Open `tagengine.ini` and set the `PROJECT` to your App Engine project id, `REGION` to the region where you created your cloud task queue in step 5, and `QUEUE_NAME` to the cloud tasks queue name. The `ZETA_URL` variable should be set to your zeta cloud function (covered in step 8). You only need to set the `ZETA_URL` variable if you are using tag propagation. 

```
PROJECT = tag-engine-283315
REGION = us-east1
QUEUE_NAME = tag-engine-queue
ZETA_URL = https://us-central1-tag-engine-283315.cloudfunctions.net/zeta
```

### Step 7: (Optional) Create cron jobs through Cloud Scheduler: 
#### This step is only required if you want Tag Engine to either schedule your dynamic tag updates or run tag propagation or both. 
```
gcloud scheduler jobs create app-engine run-ready-jobs --schedule='every 60 minutes' --relative-url "/run_ready_jobs"
gcloud scheduler jobs create app-engine clear-stale-jobs --schedule='every 30 minutes' --relative-url "/clear_stale_jobs"
gcloud scheduler jobs create app-engine run-propagation --schedule='every 60 minutes' --relative-url "/run_propagation"
```

### Step 8: (Optional) Deploy Zeta cloud function:
#### The cloud function is used to analyze BQ views for tag propagation.  This step is only required if you want Tag Engine to propagate your tags from tables to views.  
```
cd tag-engine/zeta
gcloud functions deploy zeta --trigger-http --entry-point com.google.cloud.sa.tagengine.service.zeta.ZetaSqlParserFunction \
--runtime java11 --memory 1GB --allow-unauthenticated
```

### Step 9: (Required) Deploy Tag Engine:
```
gcloud app deploy
gcloud app browse
```

### Step 10: (Required) Configure Tag Engine settings:

On the Tag Engine landing page, follow the links in the Tag Engine settings section to configure your default tag template, coverage report, tag history, and tag propagation. There are instructions on each page to guide you through the different settings. 


### Troubleshooting:

The Tag Engine UI doesn't display the details of an error. If you encounter an error, an generic error message will show up on the page. In order to see the details of the error, you will need to stream the app engine log as follows:

```
gcloud app logs tail -s default
```

### Cleaning up:
#### When you are done using Tag Engine, you should delete the task queue and cron jobs as follows:
```
gcloud tasks queues delete tag-engine
gcloud scheduler jobs delete run-ready-jobs
gcloud scheduler jobs delete clear-stale-jobs
gcloud scheduler jobs delete run-propagation
```
 