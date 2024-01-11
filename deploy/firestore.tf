# ************************************************************ #
# Create the firestore database
# Note: no need to create the default database, it gets 
# created automatically when the API is enabled. 
# ************************************************************ #

resource "google_firestore_database" "create" {
  project     = var.tag_engine_project
  name        = "(default)"
  location_id = var.firestore_region
  type        = "FIRESTORE_NATIVE"

  depends_on = [google_project_service.tag_engine_project]
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
   
