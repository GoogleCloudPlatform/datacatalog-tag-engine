# ************************************************************ #
# Create the firestore database
# ************************************************************ #

resource "google_firestore_database" "create" {
  project                   = var.firestore_project
  name                      = var.firestore_database
  location_id               = var.firestore_region
  type                      = "FIRESTORE_NATIVE"
  deletion_policy           = "DELETE"
  delete_protection_state   = "DELETE_PROTECTION_DISABLED"
  depends_on                = [google_project_service.firestore_project]
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
  triggers = {
    firestore_project = var.firestore_project,
    firestore_database = var.firestore_database
    firestore_db = google_firestore_database.create.id
  }
  provisioner "local-exec" {
    command = "python3 create_indexes.py create ${var.firestore_project} ${var.firestore_database}"
  }

  provisioner "local-exec" {
    when = destroy
    command = "python3 create_indexes.py destroy ${self.triggers.firestore_project} ${self.triggers.firestore_database}"
  }
  
  depends_on = [google_firestore_database.create, null_resource.install_packages]
}
   
