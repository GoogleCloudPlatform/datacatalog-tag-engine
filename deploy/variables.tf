variable "tag_engine_required_apis" {
	type = list
	description = "list of required GCP services for the tag engine project"
	default = ["cloudresourcemanager.googleapis.com", "iam.googleapis.com", "cloudbuild.googleapis.com", "artifactregistry.googleapis.com", "datacatalog.googleapis.com", "dataplex.googleapis.com", "run.googleapis.com", "cloudtasks.googleapis.com"]
}

variable "firestore_required_apis" {
	type = list
	description = "list of required GCP services for the firestore project"
	default = ["cloudresourcemanager.googleapis.com", "iam.googleapis.com", "firestore.googleapis.com"] 
}

variable "bigquery_required_apis" {
	type = list
	description = "list of required GCP services for the bigquery project"
	default = ["cloudresourcemanager.googleapis.com"]
}

variable "data_catalog_required_apis" {
	type = list
	description = "list of required GCP services for the data catalog project"
	default = ["cloudresourcemanager.googleapis.com", "datacatalog.googleapis.com", "dataplex.googleapis.com"]
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

variable "tag_engine_project" {
    type = string
	description = "project id in which to create the cloud run services and cloud tasks, typically the data governance project. Make sure that it matches the same variable that you put in tagengine.ini."
	default = "tag-engine-run"
}

variable "tag_engine_region" {
    type = string
	description = "region in which to create the cloud run services and cloud tasks. Make sure that it matches the same variable that you put in tagengine.ini."
	default = "us-central1"
}

variable "firestore_project" {
    type = string
	description = "project id in which to create your firestore database. Make sure that it matches the same variable that you put in tagengine.ini."
	default = "tag-engine-run"
}

variable "firestore_region" {
    type = string
	description = "region where to deploy your firestore database as it may not be available in your chosen tag engine region. See https://cloud.google.com/firestore/docs/locations. Make sure that it matches the same variable that you put in tagengine.ini."
	default = "us-central1"
}

variable "firestore_database" {
    type = string
	description = "name of your firestore database. Make sure that it matches the same variable that you put in tagengine.ini."
	default = "(default)"
}

variable "data_catalog_project" {
    type = string
	description = "project id in which you have created your tag templates."
	default = "tag-engine-run"
}

variable "data_catalog_region" {
    type = string
	description = "region in which you have created your tag templates."
	default = "us-central1"
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

variable "injector_queue" {
     type = string
	 description = "name of the task queue used for tracking job requests. Make sure that it matches the same variable in tagengine.ini"
	 default = "tag-engine-injector-queue"
}

variable "work_queue" {
     type = string
	 description = "name of the task queue used for tracking individual work items. Make sure that it matches the same variable that you put in tagengine.ini."
	 default = "tag-engine-work-queue"
}

variable "csv_bucket" {
     type = string
	 description = "name of the gcs bucket which will be used to store the cvs data for tagging"
	 default = "tag-imports"
}
