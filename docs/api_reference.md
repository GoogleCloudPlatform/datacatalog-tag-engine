### Tag Engine API Reference

#### Overview

* `static_create`: create a static tag configuration
* `dynamic_create`: create a dynamic tag configuration
* `entry_create`: create a Data Catalog entry configuration for GCS objects
* `glossary_create`: create a glossary tag configuration
* `sensitive_create`: create a sensitive tag configuration
* `restore_create`: create a restore tag configuration
* `import_create`: create an import tag configuration
* `scheduled_auto_updates`: run a tag configuration that is set to auto refresh 
* `ondemand_updates`: run a tag configuration that is set to on-demand refresh 
* `get_job_status`: get the status of a job

#### static_create

Create a static tag configuration, which in turn creates one or more Data Catalog tags. 

##### Syntax:

```
curl -X POST [TAG_ENGINE_URL]/static_create -d @input.json
```

##### Example `input.json`:

```
{
    "template_id": "data_product",
    "project_id": "tag-engine-develop",
    "region": "us-central1",
    "fields": [
        {
            "field_id": "data_domain",
            "field_value": "Finance"
        },
        {
            "field_id": "data_subdomain",
            "field_value": "Financial_Insights"
        },
        {
            "field_id": "data_product_name",
            "field_value": "Finwire archives"
        },
        {
            "field_id": "data_product_description",
            "field_value": "This dataset contains financial information about companies and securities obtained from a financial newswire service that has been archived over an extended period of time."
        },
        {
            "field_id": "data_confidentiality",
            "field_value": "Public"
        },
        {
            "field_id": "business_criticality",
            "field_value": "Medium"
        },
        {
            "field_id": "business_owner",
            "field_value": "John Williams"
        },
        {
            "field_id": "technical_owner",
            "field_value": "Emily Doe"
        },
        {
            "field_id": "number_data_resources",
            "field_value": "409"
        },
        {
            "field_id": "storage_location",
            "field_value": "us-central1"
        },
        {
            "field_id": "data_retention_period",
            "field_value": "7_years"
        },
        {
            "field_id": "data_latency_slo",
            "field_value": "quarterly"
        },
        {
            "field_id": "documentation_link",
            "field_value": "https://www.tpc.org/tpc_documents_current_versions/pdf/tpc-di_v1.1.0.pdf "
        },
        {
            "field_id": "access_request_link",
            "field_value": "go/sphinx/finwire-archive"
        },
	    {
	        "field_id": "data_product_status",
            "field_value": "RELEASED"
        },
	    {
	        "field_id": "last_modified_date",
            "field_value": "2022-08-21"
        }
    ],
    "included_uris": "bigquery/project/tag-engine-develop/dataset/finwire",
    "excluded_uris": "",
    "refresh_mode": "ON_DEMAND",
    "tag_history": true,
    "tag_stream": false   
}
```

##### Additional notes:
* The `included_uris` value may be a valid BQ path or a GCS path. 
* The `refresh_mode` value must be set to either `AUTO` or `ON-DEMAND`. If `AUTO`, the `refresh_frequency` and `refresh_unit` fields must also be set.  
* The `refresh_unit` value must be set to either `minutes`,  `hours` or `days`. 


#### dynamic_create

Create a dynamic tag configuration, which in turn creates one or more Data Catalog tags. 

##### Syntax:

```
curl -X POST [TAG_ENGINE_URL]/dynamic_create -d @input.json
```

##### Example `input.json`:

```
{
    "template_id": "data_resource",
    "project_id": "tag-engine-develop",
    "region": "us-central1",
    "fields": [
        {
            "field_id": "data_sensitivity",
            "query_expression": "select sensitive_type from tag-engine-develop.tag_history.data_sensitivity ds join tag-engine-develop.reference.SensitiveCategory sc on ds.sensitive_type = sc.category where starts_with(asset_name, '$project/dataset/$dataset/table/$table/') order by rank desc limit 1"
        },
        {
            "field_id": "num_fields",
            "query_expression": "select count(*) from $project.$dataset.INFORMATION_SCHEMA.COLUMNS where table_name = '$table'"
        },
		{
            "field_id": "num_records",
            "query_expression": "select row_count from `$project.$dataset.__TABLES__` where table_id = '$table'"
        },
        {
            "field_id": "size",
            "query_expression": "select round(size_bytes/1048576, 2) from `$project.$dataset.__TABLES__` where table_id = '$table'"
        },
        {
            "field_id": "recent_data_update",
            "query_expression": "select cast(timestamp_millis(last_modified_time) as datetime) from `$project.$dataset.__TABLES__` where table_id = '$table'"
        },
        {
            "field_id": "actual_data_latency",
            "query_expression": "select timestamp_diff(max(start_time), min(start_time), second) / (count(distinct(start_time)) - 1) from `$project`.`region-us-central1`.INFORMATION_SCHEMA.JOBS j1, unnest(referenced_tables) as r where statement_type in ('INSERT', 'UPDATE', 'DELETE') and r.project_id = '$project' and r.dataset_id = '$dataset' and r.table_id = '$table'"
        }, 
        {
            "field_id": "global_id_customer",
            "query_expression": "select False"
        },
        {
            "field_id": "global_id_account",
            "query_expression": "select False"
        },
        {
            "field_id": "global_id_location",
            "query_expression": "select False"
        },
        {
            "field_id": "global_id_product",
            "query_expression": "select False"
        }
    ],
    "included_uris": "bigquery/project/tag-engine-develop/dataset/finwire/*",
    "excluded_uris": "",
    "refresh_mode": "AUTO",
    "refresh_frequency": 24,
    "refresh_unit": "hours",
    "tag_history": true,
    "tag_stream": false  
}
```

##### Additional notes:
* The  `query_expression` must be a valid BQ query and can reference the special variables $project, $dataset, $table and $column. 
* The `included_uris` value may be a valid BQ path or a GCS path. 
* The `refresh_mode` value must be set to either `AUTO` or `ON-DEMAND`. If `AUTO`, the `refresh_frequency` and `refresh_unit` fields must also be set.  
* The `refresh_unit` value must be set to either `minutes`,  `hours` or `days`. 


#### entry_create

Create an entry configuration, which in turn creates one or more Data Catalog entries that represent physical file objects on GCS. 

##### Syntax:

```
curl -X POST [TAG_ENGINE_URL]/entry_create -d @input.json
```

##### Example `input.json`:

```
{
    "template_id": "file_metadata",
    "project_id": "tag-engine-develop",
    "region": "us-central1",
    "fields": [
        {
            "field_id": "name"
        },
        {
            "field_id": "bucket"
        },
        {
            "field_id": "path"
        },
        {
            "field_id": "type"
        },
        {
            "field_id": "size"
        },
        {
            "field_id": "num_rows"
        },
        {
            "field_id": "created_time"
        },
        {
            "field_id": "updated_time"
        },
        {
            "field_id": "storage_class"
        },
        {
            "field_id": "owner"
        },
        {
            "field_id": "content_encoding"
        },
        {
            "field_id": "content_language"
        },
        {
            "field_id": "media_link"
        }
    ],
    "included_uris": "gs://discovery-area/cities_311/*",
    "excluded_uris": "gs://discovery-area/cities_311/austin_311_service_requests.parquet",
    "refresh_mode": "AUTO",
    "refresh_frequency": 30,
    "refresh_unit": "minutes",
    "tag_history": true,
    "tag_stream": false   
}
```

##### Additional notes:
* The `included_uris` value may be a valid GCS path. It can contain a wildcard for the file name. 
* The  `exclued_uris` field is optional. If set, it must be a valid GCS path. 
* The `refresh_mode` value must be set to either `AUTO` or `ON-DEMAND`. If `AUTO`, the `refresh_frequency` and `refresh_unit` fields must also be set.  
* The `refresh_unit` value must be set to either `minutes`,  `hours` or `days`. 


#### glossary_create

Create a glossary configuration, which in turn creates one or more glossary tags in Data Catalog. A glossary tag maps element names from a GCS file or BQ table to canonical names, allowing users to search for the assets by their canonical names. 

##### Syntax:

```
curl -X POST [TAG_ENGINE_URL]/glossary_create -d @input.json
```

##### Example `input.json`:

```
{
    "template_id": "enterprise_glossary",
    "project_id": "tag-engine-develop",
    "region": "us-central1",
    "fields": [
        {
            "field_id": "source_supplier_unique_id"
        },
        {
            "field_id": "primary_business_name"
        },
        {
            "field_id": "organization_address_street_address_line1"
        },
        {
            "field_id": "organization_address_city"
        },
        {
            "field_id": "organization_address_state"
        },
        {
            "field_id": "organization_address_postal_code"
        },
        {
            "field_id": "organization_address_country"
        },
        {
            "field_id": "telephone_number"
        },
        {
            "field_id": "chief_executive_officer_first_name"
        },
        {
            "field_id": "chief_executive_officer_last_name"
        },
        {
            "field_id": "chief_executive_officer_name_suffix"
        },
        {
            "field_id": "sic_code_1"
        },
        {
            "field_id": "chief_executive_officer_title"
        },
        {
            "field_id": "sic_code_1_base_6"
        },
        {
            "field_id": "organization_address_county"
        },
        {
            "field_id": "organization_msa_name"
        },
        {
            "field_id": "sic_code_1_base_2"
        },
        {
            "field_id": "sic_code_1_base_4"
        },
        {
            "field_id": "sic_code_1_base_4_description"
        },
        {
            "field_id": "year_started"
        },
        {
            "field_id": "organization_address_postal_code_4_extension"
        },
        {
            "field_id": "company_url"
        }
    ],
    "mapping_table": "bigquery/project/tag-engine-develop/dataset/enterprise_glossary/mapping",
    "included_uris": "gs://discovery-area/sample_data/*",
    "refresh_mode": "AUTO",
    "refresh_frequency": 24,
    "refresh_unit": "hours",
    "tag_history": true,
    "tag_stream": false   
}
```

##### Additional notes:
* The `mapping_table` value must be a BQ table that maps every file element name to a glossary name. The mapping table must have the fields `source_name` and `canonical_name`. The values in the `canonical_name` must be equal to the tag template fields. 
* The `included_uris` value may be a BQ path or a GCS path. 
* The `refresh_mode` value must be either `AUTO` or `ON-DEMAND`. If set to `AUTO`, the `refresh_frequency` and `refresh_unit` fields must also be set.  

#### sensitive_create

Create a sensitive data configuration, which in turn creates one or more sensitive data tags in Data Catalog. Note: These are field-level tags unlike the above mentioned tags. 

##### Syntax:

```
curl -X POST [TAG_ENGINE_URL]/sensitive_create -d @input.json
```

##### Example `input.json`:

```
{
    "template_id": "data_sensitivity",
    "project_id": "tag-engine-develop",
    "region": "us-central1",
    "fields": [
        {
            "field_id": "sensitive_field"
        },
        {
            "field_id": "sensitive_type"
        }
    ],
    "dlp_dataset": "bigquery/project/tag-engine-develop/dataset/finwire_dlp",
    "mapping_table": "bigquery/project/tag-engine-develop/dataset/reference/SensitiveCategory",
    "included_uris": "bigquery/project/tag-engine-develop/dataset/finwire/*",
    "create_policy_tags": true,
    "taxonomy_id": "projects/tag-engine-develop/locations/us-central1/taxonomies/317244085807144487",
    "refresh_mode": "ON_DEMAND",
    "tag_history": true,
    "tag_stream": false   
}
```

##### Additional notes:
* The `dlp_dataset` value must be a BQ dataset that contains the DLP inspection result tables. 
* The `mapping_table` value must be a BQ table that maps every DLP infotype name to a sensitive category. The mapping table must have the fields `source_name` and `canonical_name`. The values in the `canonical_name` must be equal to the tag template fields. 
* If the `create_policy_tags` field is `true`, then the `taxonomy_id` field is required. 
* The `taxonomy_id` value is the fully-qualified policy tag taxonomy id in Data Catalog. 


#### restore_create

Create a restore tag configuration, which restores table-level or field-level tags from a metadata export file into Data Catalog. The export file must be generated using Data Catalog's export feature.  

##### Syntax:

```
curl -X POST [TAG_ENGINE_URL]/restore_create -d @input.json
```

##### Example `input.json`:

```
{
    "source_template_id": "data_resource",
    "source_template_project": "data-mesh-344315",
    "source_template_region": "us-central1",
    "target_template_id": "data_resource_v2",
    "target_template_project": "tag-engine-develop",
    "target_template_region": "us-central1",
    "metadata_export_location": "gs://catalog_metadata_exports/Exported_Metadata_Project_tag-engine-develop_2022-08-04T15-23-28Z_UTC.jsonl",
    "tag_history": true, 
    "tag_stream": false   
}
```

##### Additional notes:
* The `source_template_id` is the tag template whose tags you want to restore from the metadata export file. 
* The `target_template_id` is the tag template whose tags you want to populate. 
* The `metadata_export_location` is the path to the export files on GCS. The export files must be generated from Data Catalog's export function. 


#### import_create

Create an import tag configuration, which imports tags from one or more CSV files into Data Catalog. The CSV file must have a specific format.  

##### Syntax:

```
curl -X POST [TAG_ENGINE_URL]/import_create -d @input.json
```

##### Example `input.json`:

```
{
    "template_id": "data_resource",
    "template_project": "tag-engine-develop",
    "template_region": "us-central1",
    "metadata_import_location": "gs://catalog_metadata_imports/finwire_table_tags.csv",
    "tag_history": true, 
    "tag_stream": false   
}
```

##### Additional notes:
* The `template_id` is the tag template you want to create the tags with. 
* The `metadata_import_location` is the path to the CSV files on GCS. 
* The CSV files must contain the following fields: `project`, `dataset`, `table` if you are creating table-level tags and `project`, `dataset`, `table, `column` if you are creating column-level tags. 
* The CSV files must also contain a field for each tag field that you want to populate. 

