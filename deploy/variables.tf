variable "tag_engine_project" {
    type = string
    default = "tag-engine-vanilla-337221"
}

variable "bigquery_project" {
     type = string
     default = "warehouse-337221"
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
	default = ["cloudresourcemanager.googleapis.com", "firestore.googleapis.com", "cloudtasks.googleapis.com", "cloudscheduler.googleapis.com"]
}
	
variable "gcp_services_bigquery" {
	type = list
	default = ["iam.googleapis.com", "cloudresourcemanager.googleapis.com"]
}


