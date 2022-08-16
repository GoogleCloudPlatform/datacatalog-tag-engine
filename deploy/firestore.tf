resource "google_firestore_index" "index-1" {
  project = var.tag_engine_project

  collection = "static_configs"

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

  depends_on = [google_project_service.tag_engine_project]
}

resource "google_firestore_index" "index-2" {
  project = var.tag_engine_project

  collection = "dynamic_configs"

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

  collection = "entry_configs"

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

  depends_on = [google_firestore_index.index-2]
}

resource "google_firestore_index" "index-4" {
  project = var.tag_engine_project

  collection = "glossary_configs"

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

  depends_on = [google_firestore_index.index-3]
}

resource "google_firestore_index" "index-5" {
  project = var.tag_engine_project

  collection = "sensitive_configs"

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

  depends_on = [google_firestore_index.index-4]
}

resource "google_firestore_index" "index-6" {
    project = var.tag_engine_project

    collection = "dynamic_configs"

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

    depends_on = [google_firestore_index.index-5]
}

resource "google_firestore_index" "index-7" {
    project = var.tag_engine_project

    collection = "static_configs"

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

    depends_on = [google_firestore_index.index-6]
}

resource "google_firestore_index" "index-8" {
    project = var.tag_engine_project

    collection = "entry_configs"

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

    depends_on = [google_firestore_index.index-7]
}

resource "google_firestore_index" "index-9" {
    project = var.tag_engine_project

    collection = "glossary_configs"

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

    depends_on = [google_firestore_index.index-8]
}


resource "google_firestore_index" "index-10" {
    project = var.tag_engine_project

    collection = "sensitive_configs"

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

    depends_on = [google_firestore_index.index-9]
}

resource "google_firestore_index" "index-11" {
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

    depends_on = [google_firestore_index.index-10]
}

resource "google_firestore_index" "index-12" {
    project = var.tag_engine_project

    collection = "tasks"

    fields {
      field_path = "job_uuid"
      order      = "ASCENDING"
    }

    fields {
      field_path = "config_uuid"
      order      = "ASCENDING"
    }

    fields {
      field_path = "uri"
      order      = "ASCENDING"
    }

  depends_on = [google_firestore_index.index-11]
}

resource "google_firestore_index" "index-13" {
    project = var.tag_engine_project

    collection = "restore_configs"

    fields {
      field_path = "source_template_uuid"
      order      = "ASCENDING"
    }

    fields {
      field_path = "target_template_uuid"
      order      = "ASCENDING"
    }

    fields {
      field_path = "config_status"
      order      = "ASCENDING"
    }

  depends_on = [google_firestore_index.index-12]
}

resource "google_firestore_index" "index-14" {
    project = var.tag_engine_project

    collection = "dynamic_configs"

    fields {
      field_path = "template_uuid"
      order      = "ASCENDING"
    }

    fields {
      field_path = "config_status"
      order      = "ASCENDING"
    }

  depends_on = [google_firestore_index.index-13]
}

resource "google_firestore_index" "index-15" {
    project = var.tag_engine_project

    collection = "static_configs"

    fields {
      field_path = "template_uuid"
      order      = "ASCENDING"
    }

    fields {
      field_path = "config_status"
      order      = "ASCENDING"
    }

  depends_on = [google_firestore_index.index-14]
}

resource "google_firestore_index" "index-16" {
    project = var.tag_engine_project

    collection = "entry_configs"

    fields {
      field_path = "template_uuid"
      order      = "ASCENDING"
    }

    fields {
      field_path = "config_status"
      order      = "ASCENDING"
    }

  depends_on = [google_firestore_index.index-15]
}

resource "google_firestore_index" "index-17" {
    project = var.tag_engine_project

    collection = "glossary_configs"

    fields {
      field_path = "template_uuid"
      order      = "ASCENDING"
    }

    fields {
      field_path = "config_status"
      order      = "ASCENDING"
    }

  depends_on = [google_firestore_index.index-16]
}

resource "google_firestore_index" "index-18" {
    project = var.tag_engine_project

    collection = "sensitive_configs"

    fields {
      field_path = "template_uuid"
      order      = "ASCENDING"
    }

    fields {
      field_path = "config_status"
      order      = "ASCENDING"
    }

  depends_on = [google_firestore_index.index-17]
}

resource "google_firestore_index" "index-19" {
    project = var.tag_engine_project

    collection = "restore_configs"

    fields {
      field_path = "target_template_uuid"
      order      = "ASCENDING"
    }

    fields {
      field_path = "config_status"
      order      = "ASCENDING"
    }

  depends_on = [google_firestore_index.index-18]
}