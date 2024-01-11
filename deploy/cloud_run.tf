resource "google_artifact_registry_repository" "image_registry" {
  format        = "DOCKER"
  repository_id = "cloud-run-source-deploy"
  project       = var.tag_engine_project
  location      = var.tag_engine_region
  depends_on = [google_project_service.tag_engine_project]
}

# ************************************************************ #
# Create the Cloud Run API service
# ************************************************************ #

resource "null_resource" "build_api_image" {
  
  triggers = {
    project_id = var.tag_engine_project
    region = var.tag_engine_region
    full_image_path = "${var.tag_engine_region}-docker.pkg.dev/${var.tag_engine_project}/cloud-run-source-deploy/tag-engine-api"
  }

  provisioner "local-exec" {
    when    = create
    command = <<EOF
gcloud builds submit .. --git-source-dir=. \
--config=../cloudbuild.yaml \
--project ${var.tag_engine_project} \
--region ${var.tag_engine_region} \
--machine-type=e2-highcpu-8 \
--substitutions=_PROJECT=${var.tag_engine_project},_REGION=${var.tag_engine_region},_TAG_ENGINE_SA=${var.tag_engine_sa},_IMAGE=tag-engine-api
EOF
  }

  provisioner "local-exec" {
    when    = destroy
    command = <<EOF
gcloud artifacts docker images delete \
${self.triggers.full_image_path} \
--quiet
EOF
  }
  depends_on = [google_artifact_registry_repository.image_registry, google_project_service.tag_engine_project, google_project_iam_binding.storage_object_get, google_project_iam_binding.log_writer, google_project_iam_binding.repo_admin]
}

resource "google_cloud_run_v2_service" "api_service" {
  location   = var.tag_engine_region
  name       = "tag-engine-api"
  project    = var.tag_engine_project
  ingress    = "INGRESS_TRAFFIC_INTERNAL_ONLY" 
  
  template {
    service_account = var.tag_engine_sa
	
	scaling {
	    min_instance_count = 0
		max_instance_count = 5
	}

    containers {
        image = null_resource.build_api_image.triggers.full_image_path
	  
	    resources {
	        limits = {
	           memory = "1024Mi"
		    }
	        cpu_idle = true
	    }
     }
  }
  depends_on = [google_project_service.tag_engine_project, null_resource.build_api_image]
}

output "api_service_uri" {
  value = google_cloud_run_v2_service.api_service.uri
}

# ************************************************************ #
# Create the Cloud Run UI service
# ************************************************************ #

resource "null_resource" "build_ui_image" {
  
  triggers = {
    project_id = var.tag_engine_project
    region = var.tag_engine_region
    full_image_path = "${var.tag_engine_region}-docker.pkg.dev/${var.tag_engine_project}/cloud-run-source-deploy/tag-engine-ui"
  }

  provisioner "local-exec" {
    when    = create
    command = <<EOF
gcloud builds submit .. --git-source-dir=. \
--config=../cloudbuild.yaml \
--project ${var.tag_engine_project} \
--region ${var.tag_engine_region} \
--machine-type=e2-highcpu-8 \
--substitutions=_PROJECT=${var.tag_engine_project},_REGION=${var.tag_engine_region},_TAG_ENGINE_SA=${var.tag_engine_sa},_IMAGE=tag-engine-ui
EOF
  }

  provisioner "local-exec" {
    when    = destroy
    command = <<EOF
gcloud artifacts docker images delete \
${self.triggers.full_image_path} \
--quiet
EOF
  }
  
  depends_on = [google_artifact_registry_repository.image_registry, google_project_service.tag_engine_project, google_project_iam_binding.storage_object_get, google_project_iam_binding.log_writer, google_project_iam_binding.repo_admin]
}

resource "google_cloud_run_v2_service" "ui_service" {
  location   = var.tag_engine_region
  name       = "tag-engine-ui"
  project    = var.tag_engine_project
  ingress    = "INGRESS_TRAFFIC_ALL"  
  
  template {
    service_account = var.tag_engine_sa
	
	scaling {
	    min_instance_count = 0
		max_instance_count = 5
	}

    containers {
        image = null_resource.build_ui_image.triggers.full_image_path
	  
	    resources {
	        limits = {
	           memory = "1024Mi"
		    }
	        cpu_idle = true
	    }
     }
  }
  depends_on = [google_project_service.tag_engine_project, null_resource.build_ui_image]
}

output "ui_service_uri" {
  value = google_cloud_run_v2_service.ui_service.uri
}


data "google_iam_policy" "auth" {
  binding {
    role = "roles/run.invoker"
    members = [
      "allUsers",  
    ]
  }
}

resource "google_cloud_run_service_iam_policy" "auth" {
  location    = google_cloud_run_v2_service.ui_service.location
  project     = google_cloud_run_v2_service.ui_service.project
  service     = google_cloud_run_v2_service.ui_service.name

  policy_data = data.google_iam_policy.auth.policy_data
}

# ************************************************************ #
# Set environment variables on Cloud Run services
# ************************************************************ #
resource "null_resource" "set_env_var" {

 provisioner "local-exec" {
    command = "/bin/bash set_service_url.sh"
  }
  
 triggers = {
    always_run = timestamp()
  }
  
  depends_on = [google_cloud_run_v2_service.api_service, google_cloud_run_v2_service.ui_service]
}
