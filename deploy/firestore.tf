# ************************************************************ #
# Create the firestore database
# ************************************************************ #

resource "google_firestore_database" "create" {
  project     = var.firestore_project
  name        = var.firestore_database
  location_id = var.firestore_region
  type        = "FIRESTORE_NATIVE"

  depends_on = [google_project_service.firestore_project]
}


# ************************************************************ #
# Install python packages
# ************************************************************ #
resource "null_resource" "install_packages" {

provisioner "local-exec" {
  command = "/bin/bash install_packages.sh"
}

triggers = {
  always_run = timestamp()
}

depends_on = [google_cloud_run_v2_service.api_service, google_cloud_run_v2_service.ui_service]
}
  
# ************************************************************ #
# Create the firestore indexes
# ************************************************************ #

resource "null_resource" "firestore_indexes" {
  
  provisioner "local-exec" {
    command = "python create_indexes.py ${var.tag_engine_project}"
  }
  
  depends_on = [google_firestore_database.create, null_resource.install_packages]
}
   
