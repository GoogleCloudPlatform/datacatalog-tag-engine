# ************************************************************ #
# Create the firestore database
# ************************************************************ #

resource "google_firestore_database" "create" {
  project     = var.tag_engine_project
  name        = "(default)"
  location_id = var.firestore_region
  type        = "FIRESTORE_NATIVE"

  depends_on = [google_project_service.tag_engine_project]
}


# ************************************************************ #
# Create the firestore indexes
# ************************************************************ #

resource "null_resource" "firestore_indexes" {
  
  provisioner "local-exec" {
    command = "python create_indexes.py ${var.tag_engine_project}"
  }
  
  depends_on = [google_firestore_database.create]
}
   
