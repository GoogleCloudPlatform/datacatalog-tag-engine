### Tag Engine UI Guide

This is a user guide for the Tag Engine UI. 

#### Table of Contents
*  Getting started: [link](#get_started)
*  General navigation [link](#navigation)
*  Static tag configuration [link](#static)
*  Dynamic tag configuration [link](#dynamic)
*  Entry configuration [link](#entry)
*  Sensitive tag configuration [link](#sensitive)
*  Restore tag configuration
*  Import tag configuration

#### <a name="get_started"></a> Getting started

The first time you bring up the Tag Engine UI, you should see this page: 

<img src="https://github.com/GoogleCloudPlatform/datacatalog-tag-engine/blob/main/docs/home.png" alt="home" width="300"/>

Enter your tag template details into the three fields shown. The `template_id` is the tag template identifier, the `project_id` is the tag template's GCP project id, and the `region` is the tag template's region. If you are not sure about those details, go to the Data Catalog UI and look up your tag template information before proceeding. Once you have entered those details, you can click the `Search Template` button to start tagging. 

On the home page, you'll also see links for setting a default tag template, turning on/off tag history as well as turning on/off tag stream. Setting a default tag template saves you from having to type the details into the fields each time you use the Tag Engine UI. Tag history lets you save a change history of all your tags into BigQuery and is a popular option. Tag stream lets you do something similar with Pub/Sub in that Tag Engine will publish to a pub/sub topic a message for every tag creation or update request.    


#### <a name="navigation"></a> General navigation

On the next page, you'll see the field details of your tag template. Below, you'll also see a number of actions. 

<img src="https://github.com/GoogleCloudPlatform/datacatalog-tag-engine/blob/main/docs/navigation.png" alt="navigation" width="750"/>

Each action type will be covered below. 


#### <a name="static"></a> Static tag configuration

A static tag configuration creates one or more tags in Data Catalog based on the static values that you pass in. 

<img src="https://github.com/GoogleCloudPlatform/datacatalog-tag-engine/blob/main/docs/static_1.png" alt="static" width="350"/>
<img src="https://github.com/GoogleCloudPlatform/datacatalog-tag-engine/blob/main/docs/static_2.png" alt="static" width="600"/>
<img src="https://github.com/GoogleCloudPlatform/datacatalog-tag-engine/blob/main/docs/static_3.png" alt="static" width="600"/>

The `included URIs` field can be set to either a BQ path or a GCS path, depending on which type of asset you want to tag. In BQ, you can tag columns, tables, views and datasets. In GCS, you can tag buckets, folders and files.

The `refresh_mode` field is either set to `AUTO` or `ON-DEMAND`. `AUTO` refresh means that any new assets that match the `included_URIs` field will get auto-tagged on a schedule. Note that in the context of a static tag configuration, `AUTO` does not update any existing tags. Assets that have already been tagged do not get retagged. `ON-DEMAND` means that Tag Engine does not schedule any tag updates, you will call the `ondemand_updates` method to trigger an update.  

Upon clicking the submit button, you will be directed to a confirmation page. You can click on the `here` link to see status of your bulk tagging request. 


#### <a name="dynamic"></a> Dynamic tag configuration

A dynamic tag configuration creates one or more tags in Data Catalog based on the SQL queries that you pass in. 

<img src="https://github.com/GoogleCloudPlatform/datacatalog-tag-engine/blob/main/docs/dynamic_1.png" alt="static" width="600"/>
<img src="https://github.com/GoogleCloudPlatform/datacatalog-tag-engine/blob/main/docs/dynamic_2.png" alt="static" width="600"/>

In the SQL expression fields, you have access to a number of special variables: 
* $project = the project id of the asset being tagged
* $dataset = the dataset name of the asset being tagged
* $table = the table name of the asset being tagged
* $column = the field name of the asset being tagged. This variable is only available when you are creating field-level tags. 

The `included URIs` field must be set to a BQ path. You can tag columns, tables, views and datasets. 

The `refresh_mode` field is either set to `AUTO` or `ON-DEMAND`. `AUTO` refresh means that any new assets that match the `included_URIs` field will get auto-tagged on a schedule. Note that in the context of a static tag configuration, `AUTO` does not update any existing tags. Assets that have already been tagged do not get retagged. `ON-DEMAND` means that Tag Engine does not schedule any tag updates, you will call the `ondemand_updates` method to trigger an update.  

Upon clicking the submit button, you will be directed to a confirmation page. You can click on the `here` link to see status of your bulk tagging request. 


#### <a name="entry"></a> Entry configuration

An entry configuration creates one or more FILESET entries in Data Catalog, each of which represents a parquet file in GCS. Each entry is tagged with the [file metadata template](https://github.com/GoogleCloudPlatform/datacatalog-templates/blob/master/file_template.yaml) that includes the file size, the file creation time, and the number of rows in the file. This configuration can be extended to other file types. 

<img src="https://github.com/GoogleCloudPlatform/datacatalog-tag-engine/blob/main/docs/entry_1.png" alt="static" width="700"/><br>

###### Sample entry and file metadata tag in Data Catalog

Below is a sample entry and tag produced by an entry configuration in Tag Engine. 

<img src="https://github.com/GoogleCloudPlatform/datacatalog-tag-engine/blob/main/docs/entry_2.png" alt="static" width="650"/>


#### <a name="sensitive"></a> Sensitive tag configuration

The sensitive tag configuration annotates sensitive tables columns in BQ with the [sensitive data template](https://github.com/GoogleCloudPlatform/datacatalog-templates/blob/master/data_sensitivity.yaml). It can also create policy tags on the sensitive fields to restrict access to the raw data. This configuration uses as input Data Loss Prevention (DLP) tables computed from DLP inspection jobs along with a mapping table in BQ that maps a DLP infotype to a classification label (e.g. `PERSON_NAME` -> `Personal_Identifiable_Information`, etc.). 

<img src="https://github.com/GoogleCloudPlatform/datacatalog-tag-engine/blob/main/docs/data_sensitivity_1.png" alt="static" width="750"/>
<img src="https://github.com/GoogleCloudPlatform/datacatalog-tag-engine/blob/main/docs/data_sensitivity_2.png" alt="static" width="750"/>

###### Sample mapping table in BQ (input to the sensitive tag configuration)

<img src="https://github.com/GoogleCloudPlatform/datacatalog-tag-engine/blob/main/docs/mapping_table.png" alt="static" width="500"/>

###### Sample policy tag taxonomy in Data Catalog (input to the sensitive tag configuration)

<img src="https://github.com/GoogleCloudPlatform/datacatalog-tag-engine/blob/main/docs/policy_tag_taxonomy.png" alt="static" width="400"/>

###### Sample sensitive tags in Data Catalog

Below are sensitive tags (metadata and policy) produced by a sensitive tag configuration in Tag Engine: 

<img src="https://github.com/GoogleCloudPlatform/datacatalog-tag-engine/blob/main/docs/sensitive_tags.png" alt="static" width="500"/>
