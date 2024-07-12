# ************************************************** #
# create the two task queues (injector and work)
# ************************************************** #

resource "google_cloud_tasks_queue" "injector_queue" {
  name = var.injector_queue
  location = var.tag_engine_region
  project = var.tag_engine_project

  rate_limits {
      max_concurrent_dispatches = 100
    }

    retry_config {
      max_attempts = 1
    }

  stackdriver_logging_config {
    sampling_ratio = 0.9
  }
  
  depends_on = [google_project_service.tag_engine_project, google_cloud_run_v2_service.api_service, google_cloud_run_v2_service.ui_service]
}


resource "google_cloud_tasks_queue" "work_queue" {
  name = var.work_queue
  location = var.tag_engine_region
  project = var.tag_engine_project
  
  rate_limits {
      max_concurrent_dispatches = 100
    }

    retry_config {
      max_attempts = 2
    }

  stackdriver_logging_config {
    sampling_ratio = 0.9
  }
  
  depends_on = [google_project_service.tag_engine_project, google_cloud_run_v2_service.api_service, google_cloud_run_v2_service.ui_service]
}
