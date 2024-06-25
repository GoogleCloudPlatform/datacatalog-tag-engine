terraform {
  required_providers {
    google = {
      source = "hashicorp/google"
      version = "4.80.0"
    }
  }
}

# enable the required APIs
resource "google_project_service" "tag_engine_project" {
  count   = length(var.required_apis)
  project = var.tag_engine_project
  service = var.tag_engine_required_apis[count.index]

  disable_dependent_services = true
}

resource "google_project_service" "firestore_project" {
  count   = length(var.required_apis)
  project = var.firestore_project
  service = var.firestore_required_apis[count.index]

  disable_dependent_services = true
}

resource "google_project_service" "queue_project" {
  count   = length(var.required_apis)
  project = var.queue_project
  service = var.queue_required_apis[count.index]

  disable_dependent_services = true
}
