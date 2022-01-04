provider "google" {
  version = "~> 3.65"
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

# app engine
#resource "google_app_engine_application" "app" {
  #project = var.tag_engine_project
  #location_id = var.app_engine_region
  #database_type = "CLOUD_FIRESTORE"
  #}

#resource "google_project_service" "firestore" {
  #service = "firestore.googleapis.com"
  #project = var.tag_engine_project
  #disable_dependent_services = true
  #}

# iam service account grants
#data "google_app_engine_default_service_account" "default" { 
  #project = var.tag_engine_project
  #}

#output "app_engine_service_account" {
  #value = data.google_app_engine_default_service_account.default.email
  #}

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

# cloud task
resource "google_cloud_tasks_queue" "tag_engine_queue" {
  name = "tag-engine-queue"
  location = var.task_queue_region
  project = var.app_engine_subregion

  stackdriver_logging_config {
    sampling_ratio = 0.9
  }
  
  depends_on = [google_project_service.tag_engine_project]
}


# cloud scheduler

resource "google_cloud_scheduler_job" "ready-jobs" {
  name             = "ready-jobs"
  schedule         = "*/30 * * * *"
  description      = "dynamic tag updates"
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

    relative_uri = "/run_ready_jobs"
  }
  
  depends_on = [google_project_service.tag_engine_project]
}

resource "google_cloud_scheduler_job" "clear-stale-jobs" {
  name             = "clear-stale-jobs"
  schedule         = "*/15 * * * *"
  description      = "dynamic tag updates"
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
    http_method = "POST"

    #app_engine_routing {
      #service  = "default"
      #instance = "tag-engine-vanilla-335716.uc.r.appspot.com"
    #}

    relative_uri = "/clear_stale_jobs"
  }
  
  depends_on = [google_project_service.tag_engine_project]
}