variable "tag_engine_project" {
    type = string
}

variable "bigquery_project" {
     type = string
}

variable "app_engine_region" {
     type = string
     default = "us-central"
}

variable "app_engine_subregion" {
     type = string
     default = "us-central1"
}

variable "gcp_services_tag_engine" {
	type = list
	default = ["cloudresourcemanager.googleapis.com", "firestore.googleapis.com", "cloudtasks.googleapis.com", "cloudscheduler.googleapis.com", "datacatalog.googleapis.com"]
}
	
variable "gcp_services_bigquery" {
	type = list
	default = ["iam.googleapis.com", "cloudresourcemanager.googleapis.com"]
}


