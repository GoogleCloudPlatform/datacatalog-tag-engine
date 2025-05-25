### Tag Engine API Reference

This page lists the API methods for Tag Engine. 

#### Methods

* `dynamic_table_tags`: creates dynamic tags on BigQuery tables, views and datasets
* `dynamic_column_tags`: creates dynamic tags on BigQuery columns
* `import_tags`: imports tags from a CSV file to either BigQuery tables, views and datasets or columns
* `export_tags`: exports tags from Data Catalog to BigQuery
* `restore_tags`: restores tags from a Data Catalog metadata export file
* `get_job_status`: gets the status of a job
* `scheduled_auto_updates`: updates all the tags whose configurations set to refresh_mode = 'AUTO'


#### dynamic_table_tags

creates dynamic tags on BigQuery tables, views and datasets.

```
POST [TAG_ENGINE_URL]/dynamic_table_tags -d datacatalog-tag-engine/examples/dynamic_table_configs/dynamic_table_create_auto.json
``` 

#### dynamic_column_tags

creates dynamic tags on BigQuery columns.

```
POST [TAG_ENGINE_URL]/dynamic_column_tags -d datacatalog-tag-engine/examples/dynamic_column_configs/dynamic_column_create_auto.json
``` 

#### import_tags

imports tags into Data Catalog from one or more CSV files. Tags can be attached to columns or tables and views. 

Example 1:
```
POST [TAG_ENGINE_URL]/import_tags -d datacatalog-tag-engine/examples/import_configs/import_table_tags.json
``` 

Example 2:
```
POST [TAG_ENGINE_URL]/import_tags -d datacatalog-tag-engine/examples/import_configs/import_column_tags.json
``` 

#### export_tags

exports tags from Data Catalog into BigQuery tables. Tags are written into one of three reporting tables: tags attached to datasets are written into the dataset reporting table, tags attached to tables or views are written to the table reporting table, and tags attached to fields are written to the column reporting table.  

Example 1:
```
POST [TAG_ENGINE_URL]/export_tags -d datacatalog-tag-engine/examples/export_configs/export_tags_by_project.json
``` 

Example 2:
```
POST [TAG_ENGINE_URL]/export_tags -d datacatalog-tag-engine/examples/export_configs/export_tags_by_folder.json
``` 


#### restore_tags

restores tags from a Data Catalog metadata export file. Requires the Data Catalog export metadata feature. 

```
POST [TAG_ENGINE_URL]/restore_tags -d datacatalog-tag-engine/examples/restore_configs/restore_table_tags.json
``` 

#### get_job_status

gets the status of a Tag Engine job. 

```
POST [TAG_ENGINE_URL]/get_job_status -d '{"job_uuid":"47aa9460fbac11ecb1a0190a014149c1"}'
``` 

#### scheduled_auto_updates

updates the tags associated with a configuration whose `refresh_mode` field == 'AUTO', `config_status` == 'ACTIVE', and `scheduling_status` == 'READY'. 

```
POST [TAG_ENGINE_URL]/scheduled_auto_updates
```
Example:
```
$ curl -X POST $TAG_ENGINE_URL/scheduled_auto_updates -H "Authorization: Bearer $IAM_TOKEN"
{"job_ids":"[\"20380bf6340d11ef8ea942004e494300\"]","success":true}
```
