export TAG_ENGINE_URL=https://tag-engine-develop.uc.r.appspot.com

####### BQ section #######

# static asset tags on BQ tables

curl -X POST $TAG_ENGINE_URL/static_asset_tags -d @examples/static_asset_configs/static_asset_create_auto_bq.json

# dynamic table tags

curl -X POST $TAG_ENGINE_URL/dynamic_table_tags -d @examples/dynamic_table_configs/dynamic_table_create_auto.json

# dynamic column tags

curl -X POST $TAG_ENGINE_URL/dynamic_column_tags -d @examples/dynamic_column_configs/dynamic_column_create_auto.json

# sensitive column tags on BQ tables

curl -X POST $TAG_ENGINE_URL/sensitive_column_tags -d @examples/sensitive_column_configs/sensitive_column_create_auto.json

# glossary asset tags on BQ tables

curl -X POST $TAG_ENGINE_URL/glossary_asset_tags -d @examples/glossary_asset_configs/glossary_asset_create_ondemand_bq.json

# get job status

curl -X POST $TAG_ENGINE_URL/get_job_status -d '{"job_uuid":"1404a2b04a6011ed9082eb6a49899340"}'

####### GCS section #######

# data catalog entries 

curl -X POST $TAG_ENGINE_URL/entries -d @examples/entry_configs/entry_create_auto.json

# static asset tags on GCS files

curl -X POST $TAG_ENGINE_URL/static_asset_tags -d @examples/static_asset_configs/static_asset_create_auto_gcs.json

# glossary asset tags on GCS files

curl -X POST $TAG_ENGINE_URL/glossary_asset_tags -d @examples/glossary_asset_configs/glossary_asset_create_auto_gcs.json

####### restore section #######

curl --request POST 'https://datacatalog.googleapis.com/v1/projects/tag-engine-develop/locations/us-central1:exportMetadata' \
--header "X-Goog-User-Project: tag-engine-develop" \
--header "Authorization: Bearer $(gcloud auth print-access-token)" \
--header 'Accept: application/json' \
--header 'Content-Type: application/json' \
--data '{"bucket":"catalog_metadata_exports","notifyTopic":"projects/tag-engine-develop/topics/catalog_metadata_exports"}' \
--compressed

curl -X POST $TAG_ENGINE_URL/restore_tags -d @examples/restore_configs/restore_table_tags.json

curl --request POST 'https://datacatalog.googleapis.com/v1/projects/tag-engine-develop/locations/us-central1:exportMetadata' \
--header "X-Goog-User-Project: tag-engine-develop" \
--header "Authorization: Bearer $(gcloud auth print-access-token)" \
--header 'Accept: application/json' \
--header 'Content-Type: application/json' \
--data '{"bucket":"catalog_metadata_exports","notifyTopic":"projects/tag-engine-develop/topics/catalog_metadata_exports"}' \
--compressed

curl -X POST $TAG_ENGINE_URL/restore_tags -d @examples/restore_configs/restore_column_tags.json

####### import section #######

# create the tag templates
python create_template.py tag-engine-develop us-central1 data_discovery.yaml
python create_template.py tag-engine-develop us-central1 compliance_template.yaml

curl -X POST $TAG_ENGINE_URL/import_tags -d @examples/import_configs/import_table_tags.json

curl -X POST $TAG_ENGINE_URL/import_tags -d @examples/import_configs/import_column_tags.json
