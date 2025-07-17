#### Migrating to Dataplex Aspects

Through Tag Engine v3, you can clone your existing tags into aspects without the need to make changes to your tag configurations in Tag Engine. You can also retire your tags once you no longer have a need for them. This is done by preparing a `mappings.yaml` and setting the parameters `clone_tags` and `retire_tags` in your `tagengine.ini`. 

What does `cloning` mean?

When you run a Tag Engine job against a Data Catalog config and `clone_tags` is on, Tag Engine creates an equivalent config for aspects from the existing tag config. This works for dynamic table, dynamic column, and import configs.

What does `retiring` mean?

In addition to cloning, there is a second parameter `retire_tags` that lets you stop creating Data Catalog tags when running a Tag Engine job. When set to True, this parameter creates only the aspects, not the tags in Data Catalog. This means that you can migrate to using aspects without changing your Tag Engine scripts.   

Follow the steps below to put this into effect in your Tag Engine v3 deployment. 

1. Create an aspect type in Dataplex for each tag template that you wish to clone. Make sure that the fields in your aspect type match the ones in your tag template. They don't need to be called the same, but they should of the same type. For example, a string field in a tag template should map to a string field in an aspect type. 

2. Open the default `mappings.yaml` in the current folder (`migrate`) and edit the contents of this file based on your own tag template mappings. The examples that came with the repo are only meant to serve as a template so that you will know the desired structure of the mappings. For convenience, I have copied the sample mappings below:

```
mappings:
  - template_id: data_governance
    template_project: tag-engine-develop
    template_region: us-central1
    aspect_type_id: data-governance
    aspect_type_project: tag-engine-develop
    aspect_type_region: us-central1

  - template_id: data_sensitivity
    template_project: tag-engine-develop
    template_region: us-central1
    aspect_type_id: data-sensitivity
    aspect_type_project: tag-engine-develop
    aspect_type_region: us-central1
```

You can see that we have two samples mapping entries defined. The `data_governance` tag template should be mapped to the `data-governance` aspect type. The `data_sentivity` tag template should be mapped to the `data-sensitivity` aspect type. 

3. If you want to enable cloning at the system level, please add the parameter `CLONE_TAGS` to your `tagengine.ini`. This will turn on cloning on all of your jobs by default. Similarly, if you want to turn on retiring at the system level, add `RETIRE_TAGS` to the same file. 
  
For example:

<pre><code>
[DEFAULT]
TAG_ENGINE_SA = tag-engine@tag-engine-develop.iam.gserviceaccount.com
TAG_CREATOR_SA = tag-creator@tag-engine-develop.iam.gserviceaccount.com 
TAG_ENGINE_PROJECT = tag-engine-develop
TAG_ENGINE_REGION = us-central1
FIRESTORE_PROJECT = tag-engine-develop
FIRESTORE_DB = (default)
INJECTOR_QUEUE = tag-engine-injector-queue
WORK_QUEUE = tag-engine-work-queue
BIGQUERY_REGION = us-central1
FILESET_REGION = us-central1
SPANNER_REGION = us-central1
CLOUDSQL_REGION = us-central1
ENABLE_AUTH = False
OAUTH_CLIENT_CREDENTIALS = te_client_secret.json
ENABLE_TAG_HISTORY = True
TAG_HISTORY_PROJECT = tag-engine-develop
TAG_HISTORY_DATASET = tag_history
ENABLE_JOB_METADATA = True
JOB_METADATA_PROJECT = tag-engine-develop
JOB_METADATA_DATASET = job_metadata
<b>CLONE_TAGS = True</b>
<b>RETIRE_TAGS = False</b>
</code></pre>

4. If you want to turn on cloning and retiring on a case-by-case basis, you don't add them to the `tagengine.ini`. You place them instead in your Tag Engine config files. Here is an example of what that looks like:

<pre><code>
{
    "template_id": "data_governance",
    "template_project": "tag-engine-develop",
    "template_region": "us-central1",
    "fields": [
        {
            "field_id": "data_domain",
            "query_expression": "select 'LOGISTICS'"
        },
        {
            "field_id": "broad_data_category",
            "query_expression": "select 'CONTENT'"
        }
    ],
    "included_tables_uris": "bigquery/project/tag-engine-develop/dataset/crm/*",
    "excluded_tables_uris": "",
    "refresh_mode": "ON_DEMAND",
    <b>"clone_tags": true,</b>
    <b>"retire_tags": false</b>   
}
</code></pre>

5. Once you have created the mappings and made the changes to `tagengine.ini` (if application), you need to redeploy the Tag Engine service for the changes to go into effect:

```
gcloud run deploy tag-engine-api \
--source . \
--platform managed \
--project $TAG_ENGINE_PROJECT \
--region $TAG_ENGINE_REGION \
--no-allow-unauthenticated \
--ingress=all \
--memory=4G \
--timeout=60m \
--service-account=$TAG_ENGINE_SA
```

6. To test the cloning, re-run your Tag Engine job as you normally would. If only cloning is enabled, the job should create twice the number of tasks and you should see the same number of resulting aspects as tags. If retiring is enabled, you should see the normal number of tasks and aspects. 
