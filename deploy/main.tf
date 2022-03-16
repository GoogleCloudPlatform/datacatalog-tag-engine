terraform {
  required_providers {
    google = {
      source = "hashicorp/google"
      version = "3.65.0"
    }
  }
}

resource "google_project_service" "tag_engine_project" {
  count   = length(var.gcp_services_tag_engine)
  project = var.tag_engine_project
  service = var.gcp_services_tag_engine[count.index]

  disable_dependent_services = true
}

resource "google_project_service" "bigquery_project" {
  count   = length(var.gcp_services_bigquery)
  project = var.bigquery_project
  service = var.gcp_services_bigquery[count.index]

  disable_dependent_services = true
}

# IAM permissions
resource "google_project_iam_member" "bigquery_data_viewer_binding" {
  project = var.bigquery_project
  role    = "roles/bigquery.dataViewer"
  member  = "serviceAccount:${var.tag_engine_project}@appspot.gserviceaccount.com"
  
  depends_on = [google_project_service.bigquery_project]
}

resource "google_project_iam_member" "bigquery_job_user_binding" {
  project = var.bigquery_project
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${var.tag_engine_project}@appspot.gserviceaccount.com"
  
  depends_on = [google_project_service.bigquery_project]
}

resource "google_project_iam_member" "bigquery_metadata_viewer_binding" {
  project = var.bigquery_project
  role    = "roles/bigquery.metadataViewer"
  member  = "serviceAccount:${var.tag_engine_project}@appspot.gserviceaccount.com"
  
  depends_on = [google_project_service.bigquery_project]
}

resource "google_project_iam_member" "data_catalog_tag_editor_binding" {
  project = var.bigquery_project
  role    = "roles/datacatalog.tagEditor"
  member  = "serviceAccount:${var.tag_engine_project}@appspot.gserviceaccount.com"
  
  depends_on = [google_project_service.bigquery_project]
}

resource "google_project_iam_member" "data_catalog_viewer_binding" {
  project = var.bigquery_project
  role    = "roles/datacatalog.viewer"
  member  = "serviceAccount:${var.tag_engine_project}@appspot.gserviceaccount.com"
  
  depends_on = [google_project_service.bigquery_project]
}

# Cloud task queues
resource "google_cloud_tasks_queue" "injector_queue" {
  name = "tag-engine-injector-queue"
  location = var.app_engine_subregion
  project = var.tag_engine_project

  stackdriver_logging_config {
    sampling_ratio = 0.9
  }
  
  depends_on = [google_project_service.tag_engine_project]
}

resource "google_cloud_tasks_queue" "work_queue" {
  name = "tag-engine-work-queue"
  location = var.app_engine_subregion
  project = var.tag_engine_project

  stackdriver_logging_config {
    sampling_ratio = 0.9
  }
  
  depends_on = [google_project_service.tag_engine_project]
}


# Cloud scheduler entries
resource "google_cloud_scheduler_job" "dynamic_auto_update" {
  name             = "dynamic_auto_update"
  schedule         = "*/30 * * * *"
  description      = "update tags produced by dynamic configs which are set to auto update"
  time_zone        = "CST"
  attempt_deadline = "320s"
  project 	    = var.tag_engine_project
  region 		    = var.app_engine_subregion

  retry_config {
    min_backoff_duration = "5s"
    max_retry_duration = "0s"
    max_doublings = 16
    retry_count = 0
  }

  app_engine_http_target {
    #http_method = "POST"

    #app_engine_routing {
      #service  = "default"
      #instance = "tag-engine-vanilla-335716.uc.r.appspot.com"
    #}

    relative_uri = "/dynamic_auto_update"
  }
  
  depends_on = [google_project_service.tag_engine_project]
}
