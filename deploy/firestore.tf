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

  depends_on = [google_project_service.tag_engine_project]
}

resource "google_firestore_index" "index-2" {
  project = var.tag_engine_project

  collection = "tag_config"

  fields {
    field_path = "config_type"
    order      = "ASCENDING"
  }

  fields {
    field_path = "included_uris_hash"
    order      = "ASCENDING"
  }

  fields {
    field_path = "template_uuid"
    order      = "ASCENDING"
  }

  fields {
    field_path = "config_status"
    order      = "ASCENDING"
  }

  depends_on = [google_firestore_index.index-1]
}

resource "google_firestore_index" "index-3" {
    project = var.tag_engine_project

    collection = "tag_config"

    fields {
      field_path = "config_status"
      order      = "ASCENDING"
    }

    fields {
      field_path = "refresh_mode"
      order      = "ASCENDING"
    }

    fields {
      field_path = "scheduling_status"
      order      = "ASCENDING"
    }

    fields {
      field_path = "next_run"
      order      = "ASCENDING"
    }

    depends_on = [google_firestore_index.index-2]
}

resource "google_firestore_index" "index-4" {
    project = var.tag_engine_project

    collection = "logs"

    fields {
      field_path = "config_type"
      order      = "ASCENDING"
    }

    fields {
      field_path = "res"
      order      = "ASCENDING"
    }

    fields {
      field_path = "ts"
      order      = "DESCENDING"
    }

    depends_on = [google_firestore_index.index-3]
}

resource "google_firestore_index" "index-5" {
    project = var.tag_engine_project

    collection = "tasks"

    fields {
      field_path = "job_uuid"
      order      = "ASCENDING"
    }

    fields {
      field_path = "tag_uuid"
      order      = "ASCENDING"
    }

    fields {
      field_path = "uri"
      order      = "ASCENDING"
    }

  depends_on = [google_firestore_index.index-4]
}