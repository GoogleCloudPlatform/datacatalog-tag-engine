terraform {
  required_providers {
    google = {
      source = "hashicorp/google"
      version = "5.37.0"
    }
  }
}

# enable the required APIs
resource "google_project_service" "tag_engine_project" {
  count   = length(var.tag_engine_required_apis)
  project = var.tag_engine_project
  service = var.tag_engine_required_apis[count.index]
  disable_on_destroy = false
  disable_dependent_services = true
}

resource "google_project_service" "firestore_project" {
  count   = length(var.firestore_required_apis)
  project = var.firestore_project
  service = var.firestore_required_apis[count.index]
  disable_on_destroy = false
  disable_dependent_services = true
}

resource "google_project_service" "bigquery_project" {
  count   = length(var.bigquery_required_apis)
  project = var.bigquery_project
  service = var.bigquery_required_apis[count.index]
  disable_on_destroy = false
  disable_dependent_services = true
}

resource "google_project_service" "data_catalog_project" {
  count   = length(var.data_catalog_required_apis)
  project = var.data_catalog_project
  service = var.data_catalog_required_apis[count.index]
  disable_on_destroy = false
  disable_dependent_services = true
}
