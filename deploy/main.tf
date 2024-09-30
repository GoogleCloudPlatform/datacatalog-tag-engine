terraform {
  required_providers {
    google = {
      source = "hashicorp/google"
      version = "5.37.0"
    }
  }
  provider_meta "google" {
      module_name = "cloud-solutions/datacatalog-tag-engine-v2"
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

resource "local_file" "tagengineini" {
  filename = "${path.module}/../tagengine.ini"
  content = templatefile("${path.module}/tagengine.ini.tpl", {
    tag_engine_sa = var.tag_engine_sa
    tag_creator_sa = var.tag_creator_sa
    project_id = var.tag_engine_project
    region = var.tag_engine_region
    firestore_project = google_firestore_database.create.project
    firestore_db = google_firestore_database.create.name
    injector_queue = google_cloud_tasks_queue.injector_queue.name
    work_queue = google_cloud_tasks_queue.work_queue.name
    bq_region = var.bigquery_region
    fileset_region = var.tag_engine_region
    spanner_region = var.tag_engine_region
  })
}