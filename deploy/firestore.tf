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
# Create the firestore indexes
# ************************************************************ #


locals {
  config = yamldecode(file("firestore.yaml"))
  indexes = local.config["indexes"]
}

resource "google_firestore_index" "indices" {
  # generate a map from each index object defined in the yaml file, where the key is just all the fields and collection
  # put together and the value is the index object (with collection, fields and an optional order fields).
  for_each = {for index in local.indexes: "${index["collection"]}--${join("-", index["fields"][*]["field"])}" => index}

  project                       = google_firestore_database.create.project
  database                      = google_firestore_database.create.name
  collection                    = each.value["collection"]

  dynamic "fields" {
    for_each = [for f in each.value["fields"] : {
      field = f["field"]
      order = lookup(f, "order", "ASCENDING")
    }]
    content {
      field_path = fields.value.field
      order = fields.value.order
    }
  }
}
