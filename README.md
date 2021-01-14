## Steps for setting up Tag Engine

### Step 1: Set up Firestore
1. Go to GCP Console, select the project that you want to run Tag Engine from, and click on Firestore
2. In the Firestore Get Started page, click the SELECT NATIVE MODE button. 
3. Choose a database location from the drop-down (e.g. us-east1).
4. Click the CREATE DATABASE button. 
5. Once your database has been created, you should see "Your database is ready to go. Just add data". That's all you need to do to set up Firestore.  

### Step 2: (Optional) To set up Tag Engine to manage multiple GCP projects:
1. Identify the Service Account under which Tag Engine is running (e.g. tag-engine-283315@appspot.gserviceaccount.com). 
2. Open the GCP console and select one of the projects you would like Tag Engine to manage and click on IAM. 
3. Click Add member and enter the Service Account from step 1 into the New members field (e.g. tag-engine-283315@appspot.gserviceaccount.com). 
4. Assign these 5 roles: BigQuery Job User, BigQuery Metadata Viewer, BigQuery Data Viewer, Data Catalog Tag Editor, and Data Catalog Viewer. 
5. Click Save and you're almost done! 
6. Open Tag Engine and go to Report Settings. Add your project to the project_ids field so that it gets included in the Coverage Report. 

### Step 3: To run Tag Engine:
```
export REPO=https://source.developers.google.com/p/tag-engine-283315/r/tag-engine
git clone $REPO
gcloud app deploy
gcloud app browse
```

### Step 4: Create App Engine Task Queue 
#### Task queue is used to refresh dynamic tags
```
gcloud config set project $PROJECT_ID
gcloud tasks queues create tag-engine
gcloud tasks queues update tag-engine --max-attempts=3
```

### Step 5: Create cron jobs through Cloud Scheduler 
#### Cron jobs are used to refresh dynamic tags and to run tag propagation
```
gcloud scheduler jobs create app-engine run-ready-jobs --schedule='every 60 minutes' --relative-url "/run_ready_jobs"
gcloud scheduler jobs create app-engine clear-stale-jobs --schedule='every 30 minutes' --relative-url "/clear_stale_jobs"
gcloud scheduler jobs create app-engine run-propagation --schedule='every 60 minutes' --relative-url "/run_propagation"
```

### Step 6: Deploy Zeta cloud function
#### Cloud function is used to parse BQ view definitions when running tag propagation 
```
cd tag-engine/zeta
gcloud functions deploy zeta --trigger-http --entry-point com.google.cloud.sa.tagengine.service.zeta.ZetaSqlParserFunction \
--runtime java11 --memory 1GB --allow-unauthenticated
```

Open constants.py and set the ZETA_URL variable to your cloud function trigger URL:
`ZETA_URL = 'https://us-central1-tag-engine-283315.cloudfunctions.net/zeta'`

### To clean up the task queue and cron jobs:
```
gcloud tasks queues delete tag-engine
gcloud scheduler jobs delete run-ready-jobs
gcloud scheduler jobs delete clear-stale-jobs
gcloud scheduler jobs delete run-propagation
```
 