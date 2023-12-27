variable "required_apis" {
	type = list
	description = "list of required GCP services"
	default = ["cloudresourcemanager.googleapis.com", "iam.googleapis.com", "cloudresourcemanager.googleapis.com", "cloudbuild.googleapis.com", "artifactregistry.googleapis.com", "cloudtasks.googleapis.com", "firestore.googleapis.com", "datacatalog.googleapis.com", "run.googleapis.com"] 
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
