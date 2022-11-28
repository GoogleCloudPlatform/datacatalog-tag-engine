### Tag Engine Upgrade Guide

Context: In October 2022, we made a number of schema changes to Tag Engine to support new configuration types, such as the import and export config types. These schema changes are not backwards compatible. 

The following instructions are meant for Tag Engine users who are running on an older release of Tag Engine, prior to <b>Tag Engine v1.0.0</b>, and who want to upgrade to the most recent release. As of today Nov. 28, 2022, the most current release is v1.0.3. 

<b>Note that this upgrade destroys your existing Tag Engine configurations, so please make note of those details before starting this activity. You can recreate the configs through the Tag Engine UI and/or API once the upgrade is complete.</b> 


#### 1. Pull down the latest code from this repo:

```
git pull
```

#### 2. Define the following environment variables:

```
export TAG_ENGINE_PROJECT=[PROJECT_ID]
export TAG_ENGINE_REGION=[REGION] (e.g. us-central)
export TAG_ENGINE_SUB_REGION=[ZONE] (e.g. us-central1)
export BQ_PROJECT=[PROJECT_ID]
export TAG_ENGINE_SA=${TAG_ENGINE_PROJECT}@appspot.gserviceaccount.com
```


#### 3. Create a `variables.tfvars` file for Terraform (if you don't have one already defined):

```
cat > deploy/variables.tfvars << EOL
tag_engine_project="${TAG_ENGINE_PROJECT}"
bigquery_project="${BQ_PROJECT}"
app_engine_region="${TAG_ENGINE_REGION}"
app_engine_subregion="${TAG_ENGINE_SUB_REGION}"
EOL
```

#### 4. Delete the Firestore indexes:

* Open the Firestore console in your Tag Engine project and manually delete all the Firestore indexes. 

* Note: if you don't do this step, you will see "index already exists" errors when running the Terraform script. <br><br>



#### 5. Delete the Firestore collections:

* Open the Firestore console in your Tag Engine project and manually delete all the Firestore collections. 

* Note: this will remove all your existing Tag Engine configurations. If you want to recreate them, you'll need to record the details before deleting the collections. This step does <b>not</b> delete any of your Data Catalog tags or entries, only your configurations in Tag Engine.  <br><br>



#### 6. Run the Terraform deployment scripts:<br>

```
cd deploy
terraform init
terraform apply -var-file variables.tfvars
cd ..
```

#### 7. Deploy the Tag Engine application:<br>

```
gcloud app deploy
```

