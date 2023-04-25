export TAG_ENGINE_URL="https://tag-engine-eshsagj3ta-uc.a.run.app" # Service URL from Cloud Run
#export TAG_ENGINE_URL="http://127.0.0.1:5000"

# Bearer token
export IAM_TOKEN=$(gcloud auth print-identity-token)

# OAuth TOKEN
gcloud auth application-default login
export OAUTH_TOKEN=$(gcloud auth application-default print-access-token)

####### tag template section #######
# These tag templates are referenced by the unit tests. 
# Create the tag templates before running the examples. 

export TEMPLATE_PROJECT="tag-engine-run"
export TEMPLATE_REGION="us-central1"

cd datacatalog-templates/
pip install -r requirements.txt

python create_template.py $TEMPLATE_PROJECT $TEMPLATE_REGION data_governance.yaml
python create_template.py $TEMPLATE_PROJECT $TEMPLATE_REGION data_sensitivity.yaml
python create_template.py $TEMPLATE_PROJECT $TEMPLATE_REGION enterprise_glossary.yaml
python create_template.py $TEMPLATE_PROJECT $TEMPLATE_REGION data_discovery.yaml
python create_template.py $TEMPLATE_PROJECT $TEMPLATE_REGION compliance_template.yaml

####### tag history #######

cd datacatalog-tag-engine/

curl -X POST $TAG_ENGINE_URL/configure_tag_history \
	-d '{"bigquery_region":"us-central1", "bigquery_project":"tag-engine-run", "bigquery_dataset":"tag_history", "enabled":true}' \
	-H "Authorization: Bearer $IAM_TOKEN"

####### static tags #######

# create config 
curl -X POST $TAG_ENGINE_URL/create_static_asset_config -d @tests/configs/static_asset/static_asset_auto_bq.json \
	-H "Authorization: Bearer $IAM_TOKEN" \
	-H "oauth_token: $OAUTH_TOKEN"

# trigger job
curl -i -X POST $TAG_ENGINE_URL/trigger_job \
  -d '{"config_type":"STATIC_TAG_ASSET","config_uuid":"e885499ed64d11ed91ef3b0868acbb65"}' \
  -H "Authorization: Bearer $IAM_TOKEN" \
  -H "oauth_token: $OAUTH_TOKEN"

####### dynamic tags #######

# create config
curl -X POST $TAG_ENGINE_URL/create_dynamic_table_config -d @tests/configs/dynamic_table/dynamic_table_ondemand.json \
	-H "Authorization: Bearer $IAM_TOKEN" \
	-H "oauth_token: $OAUTH_TOKEN"

# trigger job
curl -i -X POST $TAG_ENGINE_URL/trigger_job \
  -d '{"config_type":"DYNAMIC_TAG_TABLE","config_uuid":"c255f764d56711edb96eb170f969c0af"}' \
  -H "Authorization: Bearer $IAM_TOKEN" \
  -H "oauth_token: $OAUTH_TOKEN"

# create config
curl -X POST $TAG_ENGINE_URL/create_dynamic_column_config -d @tests/configs/dynamic_column/dynamic_column_ondemand.json \
  -H "Authorization: Bearer $IAM_TOKEN" \
  -H "oauth_token: $OAUTH_TOKEN"

# trigger job
curl -i -X POST $TAG_ENGINE_URL/trigger_job \
  -d '{"config_type":"DYNAMIC_TAG_COLUMN","config_uuid":"18e06b5ad64e11edb9fdf1930a40c33e"}' \
  -H "Authorization: Bearer $IAM_TOKEN" \
  -H "oauth_token: $OAUTH_TOKEN"

####### sensitive tags #######

# create config 
curl -X POST $TAG_ENGINE_URL/create_sensitive_column_config \
	-d @tests/configs/sensitive_column/sensitive_column_auto.json \
	-H "Authorization: Bearer $IAM_TOKEN" \
	-H "oauth_token: $OAUTH_TOKEN"

# trigger job
curl -i -X POST $TAG_ENGINE_URL/trigger_job \
  -d '{"config_type":"SENSITIVE_TAG_COLUMN","config_uuid":"96cb3764d5ab11ed936ef9fa48b6860b"}' \
  -H "Authorization: Bearer $IAM_TOKEN" \
  -H "oauth_token: $OAUTH_TOKEN"

####### glossary tags #######

# create config 
curl -X POST $TAG_ENGINE_URL/create_glossary_asset_config -d @tests/configs/glossary_asset/glossary_asset_ondemand_bq.json \
	-H "Authorization: Bearer $IAM_TOKEN" \
	-H "oauth_token: $OAUTH_TOKEN"

# trigger job
curl -i -X POST $TAG_ENGINE_URL/trigger_job \
  -d '{"config_type":"GLOSSARY_TAG_ASSET","config_uuid":"13bea024d56811ed95362762b95fd865"}' \
  -H "Authorization: Bearer $IAM_TOKEN" \
  -H "oauth_token: $OAUTH_TOKEN"

####### export tags to BQ #######

# create config
curl -X POST $TAG_ENGINE_URL/create_export_config -d @tests/configs/export/export_by_project.json \
	-H "Authorization: Bearer $IAM_TOKEN" \
	-H "oauth_token: $OAUTH_TOKEN"

# trigger job
curl -i -X POST $TAG_ENGINE_URL/trigger_job \
  -d '{"config_type":"TAG_EXPORT","config_uuid":"0ceb28d4d64f11edb9fdf1930a40c33e"}' \
  -H "Authorization: Bearer $IAM_TOKEN" \
  -H "oauth_token: $OAUTH_TOKEN"

# create config
curl -X POST $TAG_ENGINE_URL/create_export_config -d @tests/configs/export/export_by_folder.json \
	-H "Authorization: Bearer $IAM_TOKEN" \
	-H "oauth_token: $OAUTH_TOKEN"
	
# trigger job
curl -i -X POST $TAG_ENGINE_URL/trigger_job \
  -d '{"config_type":"TAG_EXPORT","config_uuid":"13bea024d56811ed95362762b95fd865"}' \
  -H "Authorization: Bearer $IAM_TOKEN" \
  -H "oauth_token: $OAUTH_TOKEN"

####### Import tags from CSV #######

# create the config
curl -X POST $TAG_ENGINE_URL/create_import_config -d @tests/configs/import/import_table_tags.json \
	-H "Authorization: Bearer $IAM_TOKEN" \
	-H "oauth_token: $OAUTH_TOKEN"

# trigger job
curl -i -X POST $TAG_ENGINE_URL/trigger_job \
  -d '{"config_type":"TAG_IMPORT","config_uuid":"4ac0fb9ee09411edb235eba06dc7096f"}' \
  -H "Authorization: Bearer $IAM_TOKEN" \
  -H "oauth_token: $OAUTH_TOKEN"

curl -X POST $TAG_ENGINE_URL/get_job_status -d '{"job_uuid":"6acb30b2e09411ed80416b1ac85cbc14"}' \
	-H "Authorization: Bearer $IAM_TOKEN" \
	-H "oauth_token: $OAUTH_TOKEN"

# create the config
curl -X POST $TAG_ENGINE_URL/create_import_config -d @tests/configs/import/import_column_tags.json \
	-H "Authorization: Bearer $IAM_TOKEN" \
	-H "oauth_token: $OAUTH_TOKEN"

# trigger job
curl -i -X POST $TAG_ENGINE_URL/trigger_job \
  -d '{"config_type":"TAG_IMPORT","config_uuid":"004d023ce0a911edac6f9d2f379f9994"}' \
  -H "Authorization: Bearer $IAM_TOKEN" \
  -H "oauth_token: $OAUTH_TOKEN"

####### Restore tags from metadata export #######

# export the metadata
curl --request POST 'https://datacatalog.googleapis.com/v1/projects/tag-engine-run/locations/us-central1:exportMetadata' \
	--header "X-Goog-User-Project: tag-engine-run" \
	--header "Authorization: Bearer $(gcloud auth print-access-token)" \
	--header 'Accept: application/json' \
	--header 'Content-Type: application/json' \
	--data '{"bucket":"catalog_metadata_exports","notifyTopic":"projects/tag-engine-run/topics/catalog_metadata_exports"}' \
	--compressed

# create the config
curl -X POST $TAG_ENGINE_URL/create_restore_config -d @tests/configs/restore/restore_table_tags.json \
	-H "Authorization: Bearer $IAM_TOKEN" \
	-H "oauth_token: $OAUTH_TOKEN"

# trigger job
curl -i -X POST $TAG_ENGINE_URL/trigger_job \
  -d '{"config_type":"RESTORE_TAG","config_uuid":"13bea024d56811ed95362762b95fd865"}' \
  -H "Authorization: Bearer $IAM_TOKEN" \
  -H "oauth_token: $OAUTH_TOKEN"

# create the config
curl -X POST $TAG_ENGINE_URL/create_restore_config -d @tests/configs/restore/restore_column_tags.json \
	-H "Authorization: Bearer $IAM_TOKEN" \
	-H "oauth_token: $OAUTH_TOKEN"

# trigger job 
curl -i -X POST $TAG_ENGINE_URL/trigger_job \
  -d '{"config_type":"RESTORE_TAG","config_uuid":"13bea024d56811ed95362762b95fd865"}' \
  -H "Authorization: Bearer $IAM_TOKEN" \
  -H "oauth_token: $OAUTH_TOKEN"


####### Trigger job by uris #######

curl -i -X POST $TAG_ENGINE_URL/trigger_job \
  -d '{"config_type":"DYNAMIC_TAG_TABLE","included_tables_uris":"bigquery/project/tag-engine-run/dataset/GCP_Mockup/*","template_id":"data_governance","template_project":"tag-engine-run","template_region":"us-central1"}' \
  -H "Authorization: Bearer $IAM_TOKEN" \
  -H "oauth_token: $OAUTH_TOKEN"


####### Job status #######

curl -X POST $TAG_ENGINE_URL/get_job_status -d '{"job_uuid":"0d5cee42e0a911edb2ec3becb01f33e6"}' \
	-H "Authorization: Bearer $IAM_TOKEN" \
	-H "oauth_token: $OAUTH_TOKEN"


####### Scheduled auto updates #######

curl -i -X POST $TAG_ENGINE_URL/scheduled_auto_updates \
  -H "Authorization: Bearer $IAM_TOKEN" \
  -H "oauth_token: $OAUTH_TOKEN"


####### List configs #######
curl -i -X POST $TAG_ENGINE_URL/list_configs \
  -d '{"config_type":"ALL"}' \
  -H "Authorization: Bearer $IAM_TOKEN" \
  -H "oauth_token: $OAUTH_TOKEN"


####### Read config #######
curl -i -X POST $TAG_ENGINE_URL/get_config \
  -d '{"config_type":"SENSITIVE_TAG_COLUMN", "config_uuid": "96cb3764d5ab11ed936ef9fa48b6860b"}' \
  -H "Authorization: Bearer $IAM_TOKEN" \
  -H "oauth_token: $OAUTH_TOKEN"


####### Delete config #######
curl -i -X POST $TAG_ENGINE_URL/delete_config \
  -d '{"config_type":"DYNAMIC_TAG_COLUMN", "config_uuid": "13bea024d56811ed95362762b95fd865"}' \
  -H "Authorization: Bearer $IAM_TOKEN" \
  -H "oauth_token: $OAUTH_TOKEN"

####### Purge inactive configs #######
curl -i -X POST $TAG_ENGINE_URL/purge_inactive_configs \
  -d '{"config_type":"ALL"}' \
  -H "Authorization: Bearer $IAM_TOKEN" \
  -H "oauth_token: $OAUTH_TOKEN"

####### Testing with dummy account #######

export IAM_TOKEN=$(gcloud auth print-identity-token)

# authenticate with gmail account
unset GOOGLE_APPLICATION_CREDENTIALS
gcloud auth application-default login 
export OAUTH_TOKEN=$(gcloud auth application-default print-access-token)

# create config
curl -X POST $TAG_ENGINE_URL/create_dynamic_table_config -d @tests/configs/dynamic_table/dynamic_dataset_ondemand.json \
	-H "Authorization: Bearer $IAM_TOKEN" \
	-H "oauth_token: $OAUTH_TOKEN"

# trigger job
curl -i -X POST $TAG_ENGINE_URL/trigger_job \
  -d '{"config_type":"DYNAMIC_TAG_TABLE","config_uuid":"c255f764d56711edb96eb170f969c0af"}' \
  -H "Authorization: Bearer $IAM_TOKEN" \
  -H "oauth_token: $OAUTH_TOKEN"

####### Testing with service account override #######

export IAM_TOKEN=$(gcloud auth print-identity-token)

# authenticate with gmail account
export GOOGLE_APPLICATION_CREDENTIALS="/Users/scohen/keys/tag-engine-client.json"
export OAUTH_TOKEN=$(gcloud auth application-default print-access-token)

# create config
curl -X POST $TAG_ENGINE_URL/create_dynamic_table_config -d @tests/configs/dynamic_table/dynamic_dataset_non_default_service_account.json \
	-H "Authorization: Bearer $IAM_TOKEN" \
	-H "oauth_token: $OAUTH_TOKEN"

# trigger job
curl -i -X POST $TAG_ENGINE_URL/trigger_job \
  -d '{"config_type":"DYNAMIC_TAG_TABLE","config_uuid":"8c1fe554e39c11ed9129acde48001122"}' \
  -H "Authorization: Bearer $IAM_TOKEN" \
  -H "oauth_token: $OAUTH_TOKEN"

curl -X POST $TAG_ENGINE_URL/get_job_status -d '{"job_uuid":"accbe460e39c11ed9129acde48001122"}' \
	-H "Authorization: Bearer $IAM_TOKEN" \
	-H "oauth_token: $OAUTH_TOKEN"
