variable "required_apis" {
	type = list
	description = "list of required GCP services"
	default = ["cloudresourcemanager.googleapis.com", "iam.googleapis.com", "cloudresourcemanager.googleapis.com", "cloudbuild.googleapis.com", "artifactregistry.googleapis.com", "run.googleapis.com", "vpcaccess.googleapis.com", "cloudtasks.googleapis.com", "firestore.googleapis.com", "datacatalog.googleapis.com", "iap.googleapis.com"] 
}

variable "tag_engine_project" {
    type = string
	description = "project id in which to deploy tag engine"
	default = "tag-engine-develop"
}

variable "tag_engine_region" {
    type = string
	description = "region in which to deploy tag engine"
	default = "us-central1"
}

variable "firestore_region" {
    type = string
	description = "region in which to deploy firestore as it may not be available in your chosen tag engine region. See https://cloud.google.com/firestore/docs/locations"
	default = "us-east1"
}

variable "bigquery_project" {
     type = string
	 description = "project id in which your bigquery data assets reside"
	 default = "tag-engine-develop"
}

variable "bigquery_region" {
     type = string
	 description = "region in which your bigquery data assets reside"
	 default = "us-central1"
}

variable "tag_engine_sa" {
     type = string
	 description = "service account for running the tag engine cloud run service"
	 default = "tag-engine@tag-engine-develop.iam.gserviceaccount.com"
}

variable "tag_creator_sa" {
     type = string
	 description = "service account used by tag engine to create the tags in data catalog"
	 default = "tag-creator@tag-engine-develop.iam.gserviceaccount.com"
}

variable "injector_queue" {
     type = string
	 description = "name of the task queue used for tracking job requests. Make sure that it matches the same variable in tagengine.ini"
	 default = "tag-engine-injector-queue"
}

variable "work_queue" {
     type = string
	 description = "name of the task queue used for tracking individual work items. Make sure that it matches the same variable in tagengine.ini"
	 default = "tag-engine-work-queue"
}

variable "csv_bucket" {
     type = string
	 description = "name of the gcs bucket which will be used to store the cvs data for tagging"
	 default = "tag-imports"
}

// iap
variable "use_iap" {
     type = bool
	 description = "whether or not to deploy IAP as part of this deployment. Defaults to true"
	 default = true
}

variable "domain_name" {
     type = string
	 description = "In order to deploy the Tag Engine UI, you need to have a registered domain name. You can register a domain from Cloud Domains on GCP. You should get a domain before running the Terraform scripts."
	 default = "tagengine.dev"
}

variable "dns_zone" {
     type = string
	 description = "The name of your DNS zone in Cloud DNS. If you purchased a domain from Cloud Domains, you should already have a DNS zone created for it. If not, you'll need to create one yourself before running the Terraform scripts."
	 default = "tagengine-dev"
}

variable "oauth_client_id" {
  type = string
  description = "The IAP-tag-engine-backend OAuth Client ID"
}

variable "oauth_client_secret" {
  type = string
  description = "The IAP-tag-engine-backend OAuth Client Secret"
}
	
variable "authorized_user_accounts" {
  type = list(string)
  description = "The list of users you want to authorize to use the Tag Engine UI. Provide the email address for each user, which must be a google identity." 
}
