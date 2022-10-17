### Tag Engine Upgrade Guide

Context: In October 2022, a number of schema changes were made to Tag Engine, which are not backwards compatible. These changes were needed in order to support new configuration types. 

The following instructions are meant for users who are running on a code base of Tag Engine prior to Oct 2022 and who want to upgrade to the most recent code base. Note that this upgrade destroys your existing Tag Engine configurations, so make note of those details if you need them. You can recreate them through the Tag Engine UI and/or API once the upgrade is complete. 

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

* Note: this is to avoid any "index already exists" errors when running Terraform. <br><br>



#### 5. Delete Firestore collections:

* Open the Firestore console in your Tag Engine project and manually delete all the Firestore collections. 

* Note: this will remove all your existing static and dynamic tag configurations. If you want to recreate them, you'll need to record the details before deleting the collections. This will <b>not</b> delete your Data Catalog tags, only your tag configurations in Tag Engine.  <br><br>



#### 6. Run Terraform:<br>

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

