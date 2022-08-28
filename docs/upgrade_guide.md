### Tag Engine Upgrade Guide

Context: In August 2022, a number of changes were made to Tag Engine's data model, which are not backwards compatible. These changes were needed in order to add support for additional tagging configuration types (e.g. restore, import), which have different properties from the earlier types (e.g. static, dynamic, etc.).  

The following instructions are meant for users who are running on an older version of Tag Engine (prior to Aug 2022) and who want to upgrade to the latest code base. Note that this upgrade destroys your existing static and dynamic tag configurations. If you want to have them after the upgrade, you will need to recreate them through the Tag Engine UI and API. 

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

