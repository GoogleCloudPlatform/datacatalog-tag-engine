## Product Registration Pipeline
This folder contains a workflow that creates the data discovery and data protection tags for a data product. The workflow is triggered via a Data Catalog tag update event in which the status field of a data product tag is updated to "Pending". The workflow runs a Cloud Function, which in turn calls Tag Engine to generate the required tags on the data assets of the product. Once all the tags have been generated, the workflow updates the status of the data product tag to "Review". This signals to the Data Steward to review the accuracy of the generated tags before granting users access to the metadata via the BQ Metadata Viewer role.  

### Dependencies

In order to implement this workflow, you must have a running instance of Tag Engine. Moreover, Tag Engine must be running on version 1.0.5 or later. 

If you are new to Tag Engine, start with [this tutorial](https://cloud.google.com/architecture/tag-engine-and-data-catalog). You can follow [this README](https://github.com/GoogleCloudPlatform/datacatalog-tag-engine/blob/main/README.md) to deploy Tag Engine on Google Cloud Platform. 


### Deployment Procedure

The following procedure assumes that you have a running instance of Tag Engine. 


#### Step 1: Create the tag templates

This workflow makes use of four tag templates:
* [data product](https://github.com/GoogleCloudPlatform/datacatalog-templates/blob/master/data_product.yaml)
* [data resource](https://github.com/GoogleCloudPlatform/datacatalog-templates/blob/master/data_resource.yaml)
* [data sensitivity](https://github.com/GoogleCloudPlatform/datacatalog-templates/blob/master/data_sensitivity.yaml)
* [data standardization](https://github.com/GoogleCloudPlatform/datacatalog-templates/blob/master/data_standardization.yaml)

To create them, clone the [tag template repository](https://github.com/GoogleCloudPlatform/datacatalog-templates.git) and run the [create_template.py](https://github.com/GoogleCloudPlatform/datacatalog-templates/blob/master/create_template.py) script as follows:

```
git clone https://github.com/GoogleCloudPlatform/datacatalog-templates.git

export PROJECT='my-project-id'
export REGION='us-central1'

python create_template.py $PROJECT $REGION data_product.yaml
python create_template.py $PROJECT $REGION data_resource.yaml
python create_template.py $PROJECT $REGION data_sensitivity.yaml
python create_template.py $PROJECT $REGION data_standardization.yaml
```

#### Step 2: Update Tag Engine configs and upload them to Cloud Storage

The subfolders _CRM_, _Customer_Churn_, Trade_Transactions, and _Finwire_Archives_ represent sample data products and each one contains the configuration files that are used by Tag Engine to create and populate the metadata and policy tags. 

The configuration files are:
* _data_product.yaml_
* _data_resource.yaml_
* _data_sensitivity.yaml_
* _data_standardization.yaml_

These example products are based on the [TPCDI dataset](https://www.tpc.org/tpc_documents_current_versions/pdf/tpc-di_v1.1.0.pdf). You can either download the TPCDI data from [TPC.org](https://www.tpc.org/) and load it into BigQuery or you can use your own dataset. 

Either way, you'll want to update the references to the GCP project, BQ dataset, Policy Tag taxonomy in the configuration files. If you're not using the TPCDI dataset, you'll also want to update the values of certain fields so that they make sense for your data. For example, _data_product.yaml_ contains fields like _data_domain_ and _data_subdomain_. And _data_standardization.yaml_ contains _query_expression and _included_columns_query_ which need to be updated to reflect your table structure. 

Once you have made the changes to the config files, upload them into a GCS bucket. 


#### Step 3: Create the policy tag taxonomy and policy tag labels

```
export TAXONOMY_NAME='sensitive_categories'
export POLICY_TAG_LABELS='Personal_Information,Personal_Identifiable_Information,Sensitive_Personal_Information,Sensitive_Personal_Identifiable_Information'
python datacatalog-tag-engine/helper_functions/create_policy_tag_taxonomy.py $PROJECT $REGION $TAXONOMY_NAME $POLICY_TAG_LABELS
```

#### Step 4: Create the Infotype mapping tables

Create two infotype mapping tables in BigQuery. The first one maps a grouping of infotypes associated with a table column to a notable infotype. The second one maps the set of notable infotypes to a data classification category (e.g. PII, SPII, etc.).  

```
bq mk --project_id=$PROJECT –-location=$REGION reference
cat datacatalog-tag-engine/helper_functions/create_classification_tables.sql | bq query --use_legacy_sql=false 
```

#### Step 5: Scan your data assets with DLP (Data Loss Prevention)

```
export REGION='us-central1'
export INSPECT_PROJECT='my-project-id'
export INSPECT_DATASETS='finwire,oltp,crm,sales,hr'
export RESULT_PROJECT='my-project-id'
export RESULT_DATASETS='dlp_finwire,dlp_oltp,dlp_crm,dlp_sales,dlp_hr'
python datacatalog-tag-engine/helper_functions/inspect_datasets.py $REGION $INSPECT_PROJECT $INSPECT_DATASETS $RESULT_PROJECT $RESULT_DATASETS 
```

Note: you'll need to enable the DLP API on your project before running [inspect_datasets.py](https://github.com/GoogleCloudPlatform/datacatalog-tag-engine/blob/main/helper_functions/inspect_datasets.py). 


#### Step 6: Create a log router sink

The trigger event that we are interested in is `UpdateTag` and it is part of the Data Catalog admin activity log, as described in [the product documentation](https://cloud.google.com/data-catalog/docs/how-to/audit-logging).  

Create a [log router sink](https://pantheon.corp.google.com/logs/router/sink) for the `UpdateTag` event as follows:

* Sink destination service: Pub/Sub topic
* Create a Pub/Sub topic and name it _pending_status_ (it must be in the same project as where your data assets reside)
* Use the inclusion filter: 

```
resource.type="audited_resource" protoPayload.serviceName="datacatalog.googleapis.com" resource.labels.method="google.cloud.datacatalog.v1.DataCatalog.UpdateTag"
protoPayload.request.tag.fields.data_product_status.enumValue.displayName="PENDING"
```

#### Step 7: Deploy the Cloud Function

The file [main.py](https://github.com/GoogleCloudPlatform/datacatalog-tag-engine/blob/main/examples/product_registration_pipeline/main.py) contains the source for the Cloud Function. The file [requirements.txt](https://github.com/GoogleCloudPlatform/datacatalog-tag-engine/blob/main/examples/product_registration_pipeline/requirements.txt) contains the dependencies to run the function. 

Before deploying, update the _BUCKET_ and _TAG_ENGINE_URL_ variables in main.py on lines 18 and 19, respectively. The _BUCKET_ variable should point to the GCS bucket where you uploaded the config files in Step 2. 

Create the Cloud Function as follows:

* Environment: 1st gen
* Trigger type: Cloud Pub/Sub and choose _pending_status_
* Authentication: require authentication
* Entry point: _process_pending_status_event_

 
#### Step 9: Create the data product tags

```
curl -X POST $TAG_ENGINE_URL/static_asset_tags -d @examples/product_registration_pipeline/Finance/Trade_Transactions/data_product.json
curl -X POST $TAG_ENGINE_URL/static_asset_tags -d @examples/product_registration_pipeline/Customer/CRM/data_product.json
curl -X POST $TAG_ENGINE_URL/static_asset_tags -d @examples/product_registration_pipeline/Finance/Finwire_Archive/data_product.json
curl -X POST $TAG_ENGINE_URL/static_asset_tags -d @examples/product_registration_pipeline/Customer/Customer_Churn/data_product.json
```

Remember to update the metadata attribute values in the json files to reflect the data products that you are actually tagging. These ones are intended for describing products built around the TPCDI data. 


#### Step 10: Manually update the status field of the data product tags
 
Go to the Data Catalog UI and find the Data Product Template from the Tag Template menu. 
Select <b>Search for entries using this template</b> and open one of the entries from the results. 
From the entry's page, click on <b>Edit tag</b> to edit the data product tag attached to the
entry. Set the data product status field to _PENDING_ and to save the tag to trigger the automation workflow.   

Next, go to the Cloud Function logs to confirm that the _process_pending_status_event_ function has started. 

After a few minutes, reload the data product tag and you should see that the data product status field 
has been updated to _REVIEW_ and the last modified date has been updated to the current timestamp. 

click on the Entry List tab to look at the data assets associated with the product. 
Open one of the entries from the list. If the workflow succeeded, you should see a data resource tag 
attached at the table-level, a data sensitivity tag attached at the attribute level, 
and a data standardization tag attached at the attribute level. 
If you chose to generate policy tags, you should also see those appear next to each 
sensitive data field. 


### Troubleshooting:

* Consult the Cloud Function logs for details if you do not see the expected output.<br> 
* Consult the App Engine logs if you encounter any errors while running Tag Engine:<br>
`gcloud app logs tail -s default`

