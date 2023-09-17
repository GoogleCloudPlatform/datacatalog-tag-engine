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
  service = var.required_apis[count.index]

  disable_dependent_services = true
}

# run instructions: 
# export GOOGLE_APPLICATION_CREDENTIALS="/Users/scohen/keys/tag-engine-develop-dev.json"
# gcloud auth application-default login
# terraform init -input=false 
# terraform plan -out=tfplan -input=false
# terraform apply -input=false tfplan
