### Tag Engine API Reference

This page lists the API methods for Tag Engine. 

#### Methods

* `static_asset_tags`: creates static tags on BigQuery or Google Cloud Storage assets
* `dynamic_table_tags`: creates dynamic tags on BigQuery tables, views and datasets
* `dynamic_column_tags`: creates dynamic tags on BigQuery columns
* `glossary_asset_tags`: creates glossary tags on BigQuery or Google Cloud Storage assets
* `sensitive_column_tags`: creates sensitive tags on BigQuery tables
* `entries`: creates Data Catalog entries on Google Cloud Storage assets
* `import_tags`: imports tags from a CSV file to either BigQuery tables, views and datasets or columns
* `export_tags`: exports tags to BigQuery
* `restore_tags`: restores tags from a Data Catalog metadata export file
* `get_job_status`: gets the status of a job
* `ondemand_updates`: updates the tags associated with a configuration that is set to on-demand refresh


#### static_asset_tags

creates static tags on BigQuery or Google Cloud Storage assets. 

```
POST [TAG_ENGINE_URL]/static_asset_tags -d [@input.json](https://github.com/GoogleCloudPlatform/datacatalog-tag-engine/blob/main/examples/static_asset_configs/static_asset_create_auto_bq.json)
```

#### dynamic_table_tags

creates dynamic tags on BigQuery tables, views and datasets.

```
POST [TAG_ENGINE_URL]/dynamic_table_tags -d [@input.json](https://github.com/GoogleCloudPlatform/datacatalog-tag-engine/blob/main/examples/dynamic_table_configs/dynamic_table_create_auto.json)
``` 

#### dynamic_column_tags

creates dynamic tags on BigQuery columns.

```
POST [TAG_ENGINE_URL]/dynamic_column_tags -d [@input.json](https://github.com/GoogleCloudPlatform/datacatalog-tag-engine/blob/main/examples/dynamic_column_configs/dynamic_column_create_auto.json)
``` 

#### glossary_asset_tags

creates glossary tags on BigQuery or Google Cloud Storage assets. Requires a column to glossary attribute mapping table in BigQuery. 

```
POST [TAG_ENGINE_URL]/glossary_asset_tags -d [@input.json](https://github.com/GoogleCloudPlatform/datacatalog-tag-engine/blob/main/examples/glossary_asset_configs/glossary_asset_create_ondemand_bq.json)
``` 

#### sensitive_column_tags

creates sensitive tags on BQ tables. Requires Data Loss Prevention inspection job findings in BigQuery. 

```
POST [TAG_ENGINE_URL]/sensitive_column_tags -d [@input.json](https://github.com/GoogleCloudPlatform/datacatalog-tag-engine/blob/main/examples/sensitive_column_configs/sensitive_column_create_auto.json)
``` 

#### import_tags

imports tags into Data Catalog from one or more CSV files. Tags can be attached to columns or tables and views. 

Example 1:
```
POST [TAG_ENGINE_URL]/import_tags -d [@input.json](https://github.com/GoogleCloudPlatform/datacatalog-tag-engine/blob/main/examples/import_configs/import_table_tags.json)
``` 

Example 2:
```
POST [TAG_ENGINE_URL]/import_tags -d [@input.json](https://github.com/GoogleCloudPlatform/datacatalog-tag-engine/blob/main/examples/import_configs/import_column_tags.json)
``` 

#### export_tags

exports tags from Data Catalog into BigQuery tables. Tags are written into one of three reporting tables: tags attached to datasets are written into the dataset reporting table, tags attached to tables or views are written to the table reporting table, and tags attached to fields are written to the column reporting table.  

Example 1:
```
POST [TAG_ENGINE_URL]/export_tags -d [@input.json](https://github.com/GoogleCloudPlatform/datacatalog-tag-engine/blob/main/examples/export_configs/export_tags_by_project.json)
``` 

Example 2:
```
POST [TAG_ENGINE_URL]/export_tags -d [@input.json](https://github.com/GoogleCloudPlatform/datacatalog-tag-engine/blob/main/examples/export_configs/export_tags_by_folder.json)
``` 


#### restore_tags

restores tags from a Data Catalog metadata export file. Requires the Data Catalog export metadata feature. 

```
POST [TAG_ENGINE_URL]/restore_tags -d [@input.json](https://github.com/GoogleCloudPlatform/datacatalog-tag-engine/blob/main/examples/restore_configs/restore_table_tags.json)
``` 

#### get_job_status

gets the status of a Tag Engine job. 

```
POST [TAG_ENGINE_URL]/get_job_status -d '{"job_uuid":"47aa9460fbac11ecb1a0190a014149c1"}'
``` 

#### ondemand_updates

updates the tags associated with a configuration whose `refresh_mode` is set to 'ON_DEMAND'. 

```
POST [TAG_ENGINE_URL]/ondemand_updates -d [@input.json](https://github.com/GoogleCloudPlatform/datacatalog-tag-engine/blob/main/examples/dynamic_table_configs/dynamic_table_update_ondemand.json)
``` 