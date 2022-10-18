### Tag Engine UI Guide

This is a user guide for the Tag Engine UI. 

#### Table of Contents
*  Getting started: [link](#get_started)
*  General navigation [link](#navigation)
*  Static asset configuration [link](#static_asset)
*  Dynamic table configuration [link](#dynamic_table)
*  Dynamic column configuration [link](#dynamic_column)
*  Sensitive column configuration [link](#sensitive_column) 
*  Glossary asset configuration [link](#glossary_asset) 
*  Entry configuration [link](#entry)
*  Restore tag configuration [link](#restore_tags) 
*  Import tag configuration [link](#import_tags) 

#### <a name="get_started"></a> Getting started

The first time you bring up the Tag Engine UI, you should see a login page.

Enter your tag template details into the three fields shown. The `template_id` is the tag template identifier, the `template_project` is the tag template's GCP project id, and the `template_region` is the tag template's region. You must already have a Data Catalog tag template to continue. Once you have entered those details, you can click the `Search Template` button to start creating Data Catalog tags. 

<img src="https://github.com/GoogleCloudPlatform/datacatalog-tag-engine/blob/main/static/login.png" alt="static" width="350"/>


On the home page, you'll also see links for setting a default tag template, turning on/off tag history as well as turning on/off tag stream. Setting a default tag template saves you from having to type the details into the fields each time you use the Tag Engine UI. Tag history lets you save a change history of all your tags into BigQuery and is a popular option. Tag stream lets you do something similar with Pub/Sub in that Tag Engine will publish to a pub/sub topic a message for every tag creation or update request.    


#### <a name="navigation"></a> General navigation

On the next page, you'll see the field details of your tag template. Below, you'll also see a number of actions. 

<img src="https://github.com/GoogleCloudPlatform/datacatalog-tag-engine/blob/main/static/configuration-types.png" alt="static"/>

Each action type will be covered below. 


#### <a name="static_asset"></a> Static asset configuration

A static asset configuration creates Data Catalog tags on BigQuery assets (datasets, tables, views) or Google Cloud Storage assets (buckets, folders, files) that match a URI. 

<img src="https://github.com/GoogleCloudPlatform/datacatalog-tag-engine/blob/main/static/static-asset-config-1.png" alt="static" width="500"/>
<img src="https://github.com/GoogleCloudPlatform/datacatalog-tag-engine/blob/main/static/static-asset-config-2.png" alt="static" width="500"/>
<img src="https://github.com/GoogleCloudPlatform/datacatalog-tag-engine/blob/main/static/static-asset-config-3.png" alt="static" width="500"/>

The `included_assets_URIs` field can be set to either one or more BQ paths or one or more GCS path. If you want to tag assets in BQ and GCS, you need to create a separate configuration for each. 

The `refresh_mode` field is either `AUTO` or `ON-DEMAND`. `AUTO` means that any new assets that match the `included_assets_URIs` field will be auto-tagged on a schedule. `ON-DEMAND` means that Tag Engine does not schedule any tag updates, you will need to call the `ondemand_updates` method to trigger an update of the tags.  

Upon clicking the submit button, you will be directed to a confirmation page. You can click on the `here` link to see status of your request. 


#### <a name="dynamic_table"></a> Dynamic table configuration

A dynamic table configuration creates Data Catalog tags on BQ tables and views. The tags contain the results of SQL queries. Each SQL query is associated with a tag template field of the tag. 

The SQL queries can reference these variables: 
* $project = the BQ project of the table being tagged
* $dataset = the dataset of the table being tagged
* $table = the table being tagged

The `included_tables_URIs` field must be set to a BQ path. 

The `refresh_mode` field is either `AUTO` or `ON-DEMAND`. `AUTO` means that any new tables that match the `included_tables_URIs` value will be auto-tagged on a schedule. `ON-DEMAND` means that Tag Engine does not schedule any tag updates, you will call the `ondemand_updates` method to trigger an update of the tags.  

Upon clicking the submit button, you will be directed to a confirmation page. You can click on the `here` link to see status of your request. 

<img src="https://github.com/GoogleCloudPlatform/datacatalog-tag-engine/blob/main/static/dynamic-table-config-1.png" alt="static" width="700"/>
<img src="https://github.com/GoogleCloudPlatform/datacatalog-tag-engine/blob/main/static/dynamic-table-config-2.png" alt="static" width="600"/>
<img src="https://github.com/GoogleCloudPlatform/datacatalog-tag-engine/blob/main/static/dynamic-table-config-3.png" alt="static" width="300"/>

#### <a name="dynamic_column"></a> Dynamic column configuration

A dynamic column configuration creates Data Catalog tags on BQ columns. The tags contain the results of SQL queries. Each SQL query is associated with a tag template field of the tag. 

The SQL queries can reference these variables: 
* $project = the BQ project of the table being tagged
* $dataset = the dataset of the table being tagged
* $table = the table being tagged

The `included_tables_URIs` field must be set to a BQ path. 

The `refresh_mode` field is either `AUTO` or `ON-DEMAND`. `AUTO` means that any new tables that match the `included_tables_URIs` value will be auto-tagged on a schedule. `ON-DEMAND` means that Tag Engine does not schedule any tag updates, you will call the `ondemand_updates` method to trigger an update of the tags.  

Upon clicking the submit button, you will be directed to a confirmation page. You can click on the `here` link to see status of your request. 

<img src="https://github.com/GoogleCloudPlatform/datacatalog-tag-engine/blob/main/static/dynamic-column-config-1.png" alt="static" width="700"/>
<img src="https://github.com/GoogleCloudPlatform/datacatalog-tag-engine/blob/main/static/dynamic-column-config-2.png" alt="static" width="700"/>
<img src="https://github.com/GoogleCloudPlatform/datacatalog-tag-engine/blob/main/static/dynamic-column-config-3.png" alt="static" width="400"/>


#### <a name="sensitive_column"></a> Sensitive column configuration

The sensitive column configuration creates Data Catalog tags on columns in BQ. This configuration classifies the sensitivity of the data in those columns and tags the classification results. The classification requires the [sensitive data template](https://github.com/GoogleCloudPlatform/datacatalog-templates/blob/master/data_sensitivity.yaml) and Data Loss Prevention. It uses as input the info types found in the DLP inspection jobs. It then maps the info types to a custom data classification defined in a mapping table in BQ. It can optionally create policy tags on the sensitive columns so that access to the sensitive data is restricted. 

<img src="https://github.com/GoogleCloudPlatform/datacatalog-tag-engine/blob/main/static/sensitive-column-config-1.png" alt="static" width="800"/>
<img src="https://github.com/GoogleCloudPlatform/datacatalog-tag-engine/blob/main/static/sensitive-column-config-2.png" alt="static" width="750"/>
<img src="https://github.com/GoogleCloudPlatform/datacatalog-tag-engine/blob/main/static/sensitive-column-config-3.png" alt="static" width="500"/>

##### Sample mapping table in BQ (input to the sensitive column configuration)

<img src="https://github.com/GoogleCloudPlatform/datacatalog-tag-engine/blob/main/static/sensitive-column-mapping-table.png" alt="static" width="400"/>

##### Sample policy tag taxonomy in Data Catalog (input to the sensitive column configuration)

<img src="https://github.com/GoogleCloudPlatform/datacatalog-tag-engine/blob/main/static/policy-tag-taxonomy.png" alt="static" width="400"/>

###### Sample sensitive column tag created by Tag Engine

Below are sensitive tags (metadata and policy) produced by a sensitive tag configuration in Tag Engine: 

<img src="https://github.com/GoogleCloudPlatform/datacatalog-tag-engine/blob/main/static/sensitive-tags.png" alt="static" width="500"/>

#### <a name="glossary_asset"></a> Glossary asset configuration

The glossary asset config creates Data Catalog tags on either tables and views in BQ or files in GCS. The fields in the tag contain canonical field names which are mapped from the schema columns of the asset. This configuration type requires a mapping table in BQ, specified below. It also requires every canonical element name to be present in the tag template as a boolean field. When Tag Engine finds a mapping, it sets the tag template field to true, thus enabling users to search the catalog by canonical name. 

<img src="https://github.com/GoogleCloudPlatform/datacatalog-tag-engine/blob/main/static/glossary-asset-config-1.png" alt="static" width="500"/>
<img src="https://github.com/GoogleCloudPlatform/datacatalog-tag-engine/blob/main/static/glossary-asset-config-2.png" alt="static" width="750"/>
<img src="https://github.com/GoogleCloudPlatform/datacatalog-tag-engine/blob/main/static/glossary-asset-config-3.png" alt="static" width="500"/>

##### Sample mapping table in BQ (input to the glossary asset configuration)

<img src="https://github.com/GoogleCloudPlatform/datacatalog-tag-engine/blob/main/static/glossary-asset-mapping-table.png" alt="static" width="400"/>


#### <a name="entry"></a> Entry configuration

The entry configuration creates Data Catalog entries that represents parquet files stored in GCS. Each entry represents a different file and is tagged with the [file metadata template](https://github.com/GoogleCloudPlatform/datacatalog-templates/blob/master/file_template.yaml). This template includes various file metadata attributes such as file size, creation time, and number of rows. The majority of these attributes are harvested from the Cloud Storage API.  

<img src="https://github.com/GoogleCloudPlatform/datacatalog-tag-engine/blob/main/static/entry-config-1.png" alt="static" width="500"/>
<img src="https://github.com/GoogleCloudPlatform/datacatalog-tag-engine/blob/main/static/entry-config-2.png" alt="static" width="750"/>
<img src="https://github.com/GoogleCloudPlatform/datacatalog-tag-engine/blob/main/static/entry-config-3.png" alt="static" width="500"/>


###### Sample entry and file metadata tag in Data Catalog

Below is a sample entry and tag produced by an entry configuration in Tag Engine. 

<img src="https://github.com/GoogleCloudPlatform/datacatalog-tag-engine/blob/main/docs/entry-and-tag.png" alt="static" width="650"/>

#### <a name="restore_tags"></a> Restore tags configuration

The restore tags configuration re-creates Data Catalog tags from a metadata export file. It takes as input a metadata export file stored on GCS. The export file must be generated from the Data Catalog export API. 

<img src="https://github.com/GoogleCloudPlatform/datacatalog-tag-engine/blob/main/static/restore-config-1.png" alt="static" width="600"/>
<img src="https://github.com/GoogleCloudPlatform/datacatalog-tag-engine/blob/main/static/restore-config-2.png" alt="static" width="300"/>

#### <a name="import_tags"></a> Import tags configuration

The import tags configuration creates Data Catalog tags from a CSV file. The tags are created either on BQ tables and views or BQ columns. The config takes as input a CSV file located on GCS. The CSV file which must conform to the [CSV template specification](https://github.com/GoogleCloudPlatform/datacatalog-tag-engine/blob/main/examples/import_configs/CSV-template-for-bulk-tagging.xlsx).

<img src="https://github.com/GoogleCloudPlatform/datacatalog-tag-engine/blob/main/static/import-config-1.png" alt="static" width="900"/>
<img src="https://github.com/GoogleCloudPlatform/datacatalog-tag-engine/blob/main/static/import-config-2.png" alt="static" width="400"/>
