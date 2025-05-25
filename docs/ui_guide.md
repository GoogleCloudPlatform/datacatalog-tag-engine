### Tag Engine UI Guide

This is a user guide for the Tag Engine UI. 

#### Table of Contents
*  Getting started: [link](#get_started)
*  General navigation [link](#navigation)
*  Dynamic tag table [link](#dynamic_table)
*  Dynamic tag column [link](#dynamic_column)
*  Import tag [link](#import_tags) 
*  Export tag [link](#export_tags) 
*  Restore tag [link](#restore_tags) 

#### <a name="get_started"></a> Getting started

The first time you bring up the Tag Engine UI, you should see a login page.

Enter your tag template details into the three fields shown. The `template_id` is the tag template identifier, the `template_project` is the tag template's GCP project id, and the `template_region` is the tag template's region. You must already have a Data Catalog tag template to continue. Once you have entered those details, you can click the `Search Template` button to start creating Data Catalog tags. 

<img src="https://github.com/GoogleCloudPlatform/datacatalog-tag-engine/blob/cloud-run/static/homepage.png" alt="static" width="550"/>


On the home page, you'll also see links for setting a default tag template, turning on/off tag history as well as turning on/off tag stream. Setting a default tag template saves you from having to type the details into the fields each time you use the Tag Engine UI. Tag history lets you save a change history of all your tags into BigQuery and is a popular option. Tag stream lets you do something similar with Pub/Sub in that Tag Engine will publish to a pub/sub topic a message for every tag creation or update request.    


#### <a name="navigation"></a> General navigation

On the next page, you'll see the field details of your tag template. Below, you'll also see a number of actions. 

<img src="https://github.com/GoogleCloudPlatform/datacatalog-tag-engine/blob/cloud-run/static/configuration-types.png" alt="static" width="650"/>

Each action type will be covered below. 


#### <a name="dynamic_table"></a> Dynamic Tag Table

This config type creates Data Catalog tags on BQ tables and views. The tags contain the results of SQL queries. Each SQL query is associated with a tag template field of the tag. 

The SQL queries can reference these variables: 
* $project = the BQ project of the table being tagged
* $dataset = the dataset of the table being tagged
* $table = the table being tagged

The `included_tables_URIs` field must be set to a BQ path. 

The `refresh_mode` field is either `AUTO` or `ON-DEMAND`. `AUTO` means that any new tables that match the `included_tables_URIs` value will be auto-tagged on a schedule. `ON-DEMAND` means that Tag Engine does not schedule any tag updates, you will call the `ondemand_updates` method to trigger an update of the tags.  

Upon clicking the submit button, you will be directed to a confirmation page. You can click on the `here` link to see status of your request. 

<img src="https://github.com/GoogleCloudPlatform/datacatalog-tag-engine/blob/cloud-run/static/dynamic-table-config-1.png" alt="static" width="900"/>
<img src="https://github.com/GoogleCloudPlatform/datacatalog-tag-engine/blob/cloud-run/static/dynamic-table-config-2.png" alt="static" width="800"/>
<img src="https://github.com/GoogleCloudPlatform/datacatalog-tag-engine/blob/cloud-run/static/dynamic-table-config-3.png" alt="static" width="450"/>

#### <a name="dynamic_column"></a> Dynamic Tag Column

This config type creates Data Catalog tags on BQ columns. The tags contain the results of SQL queries. Each SQL query is associated with a tag template field of the tag. 

The SQL queries can reference these variables: 
* $project = the BQ project of the table being tagged
* $dataset = the dataset of the table being tagged
* $table = the table being tagged

The `included_tables_URIs` field must be set to a BQ path. 

The `refresh_mode` field is either `AUTO` or `ON-DEMAND`. `AUTO` means that any new tables that match the `included_tables_URIs` value will be auto-tagged on a schedule. `ON-DEMAND` means that Tag Engine does not schedule any tag updates, you will call the `ondemand_updates` method to trigger an update of the tags.  

Upon clicking the submit button, you will be directed to a confirmation page. You can click on the `here` link to see status of your request. 

<img src="https://github.com/GoogleCloudPlatform/datacatalog-tag-engine/blob/cloud-run/static/dynamic-column-config-1.png" alt="static" width="800"/>
<img src="https://github.com/GoogleCloudPlatform/datacatalog-tag-engine/blob/cloud-run/static/dynamic-column-config-2.png" alt="static" width="800"/>
<img src="https://github.com/GoogleCloudPlatform/datacatalog-tag-engine/blob/cloud-run/static/dynamic-column-config-3.png" alt="static" width="500"/>


#### <a name="import_tags"></a> Import Tag

This config type creates Data Catalog tags from a CSV file. The tags are created either on BQ tables and views or BQ columns. The config takes as input a CSV file located on GCS. The CSV file which must conform to the [CSV template specification](https://github.com/GoogleCloudPlatform/datacatalog-tag-engine/blob/cloud-run/examples/import_configs/CSV-template-for-bulk-tagging.xlsx).

<img src="https://github.com/GoogleCloudPlatform/datacatalog-tag-engine/blob/cloud-run/static/import-config-1.png" alt="static" width="700"/>
<img src="https://github.com/GoogleCloudPlatform/datacatalog-tag-engine/blob/cloud-run/static/import-config-2.png" alt="static" width="400"/>


#### <a name="export_tags"></a> Export tags configuration

This config type lets you export your Data Catalog tags into BigQuery. It generates three output tables in BigQuery,<br> 
one for dataset-level tags, another for table and view level tags, and a third for column-level tags. <br>
These tables can be used to source curation boards and other business intelligence reports.

<img src="https://github.com/GoogleCloudPlatform/datacatalog-tag-engine/blob/cloud-run/static/export-config-1.png" alt="static" width="600"/>
<img src="https://github.com/GoogleCloudPlatform/datacatalog-tag-engine/blob/cloud-run/static/export-config-2.png" alt="static" width="400"/>


#### <a name="restore_tags"></a> Restore tags configuration

This config type re-creates Data Catalog tags from metadata export files. It takes as input a metadata export file stored on GCS. The export file must be generated from the Data Catalog export API. 

<img src="https://github.com/GoogleCloudPlatform/datacatalog-tag-engine/blob/cloud-run/static/restore-config-1.png" alt="static" width="700"/>
<img src="https://github.com/GoogleCloudPlatform/datacatalog-tag-engine/blob/cloud-run/static/restore-config-2.png" alt="static" width="400"/>
