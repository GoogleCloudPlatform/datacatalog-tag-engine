#### Migrating to Dataplex Aspects

Tag Engine simplifies your migration from Data Catalog tags to Dataplex aspects. Through Tag Engine v3, you can clone your existing tags into aspects without the need to make changes to your tag configurations. You can also retire your tags once you no longer have a need for them. This is done by preparing a `mappings.yaml` and setting parameters the `clone_tags` and `retire_tags`. 

Follow the steps below to put this into effect in your Tag Engine deployment. 

1. Create an aspect type in Dataplex for each tag template that you want to clone. Make sure that the fields in your aspect type match the ones in your tag template. 

2. Open the existing `mappings.yaml` and edit the contents of this file based on your mappings. The default `mappings.yaml`, which is located in the `migrate` directory, is meant for you to modify. The contents of the file show you how to specify the mappings. For convenience, they are copied below:

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

This examples shows you two samples mapping entries. It says that for the `data_governance` tag template, Tag Engine should use the `data-governance` aspect type when cloning the tags (and similarly for `data_sensitivity`, use `data-sensitivity`). The `project_id` in each entry refers to the GCP project id of the tag template and aspect type, while the `region` refers to the GCP location of the tag template and aspect type. 

3. If you would like to enable cloning at the system level, add the parameter `CLONE_TAGS` to your `tagengine.ini`. This parameter turns on cloning on all of your jobs by default. When you run a Tag Engine job against a Data Catalog config and `CLONE_TAGS` is one, Tag Engine creates an equivalent config for aspects from the existing tag config (as long as you have the mappings defined in `mappings.yaml`). Tag Engine will then create the tags and aspects as part of the same job execution.  

`CLONE_TAGS = True` 

4. In addition to cloning, there is a second parameter `RETIRE_TAGS` that lets you stop creating Data Catalog tags when running a Tag Engine job. When set to True, `RETIRE_TAGS` will create only the aspects not the tags from the existing Data Catalog config. 


5. If you don't want to turn on `CLONE_TAGS` or `RETIRE_TAGS` at the system level, you can turn them on on individual configs by adding the parameters to your config files. Here is an example of what that looks like:

```
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
    "clone_tags": true,
    "retire_tags": false   
}
```

6. Once you have created the mappings and made the changes to `tagengine.ini` (if application), you need to redeploy the Tag Engine service for the changes to go into effect:

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

7. If you enabled cloning at the system level, you now try to run one of your Tag Engine jobs as you normally would. The job should have 2x the number of tasks (if you are not retiring the tags) and you should see the equivalent aspects populated when the job finishes. 

8. If you did not enable cloning at the system level, you should have added those parameters to one or more of your config files. You should then recreate the config as you normally would and run the job against the updated config. There are no changes to the create config and run job commands.  