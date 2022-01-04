resource "google_firestore_index" "index-1" {
  project = var.tag_engine_project

  collection = "tag_config"

  fields {
    field_path = "template_uuid"
    order      = "ASCENDING"
  }

  fields {
    field_path = "config_status"
    order      = "ASCENDING"
  }

}

resource "google_firestore_index" "index-2" {
  project = var.tag_engine_project

  collection = "logs"

  fields {
    field_path = "rs"
    order      = "ASCENDING"
  }

  fields {
    field_path = "template_uuid"
    order      = "ASCENDING"
  }
  
  fields {
    field_path = "ts"
    order      = "DESCENDING"
  }

}

resource "google_firestore_index" "index-3" {
  project = var.tag_engine_project

  collection = "logs"

  fields {
    field_path = "rs"
    order      = "ASCENDING"
  }

  fields {
    field_path = "template_uuid"
    order      = "ASCENDING"
  }
  
  fields {
    field_path = "ts"
    order      = "DESCENDING"
  }

}