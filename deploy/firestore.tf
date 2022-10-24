# logs

resource "google_firestore_index" "index-0" {
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

    depends_on = [google_project_service.tag_engine_project]
}

# tasks

resource "google_firestore_index" "index-1" {
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

  depends_on = [google_project_service.tag_engine_project]
}

# static asset configs

resource "google_firestore_index" "index-2" {
  project = var.tag_engine_project

  collection = "static_asset_configs"

  fields {
    field_path = "config_type"
    order      = "ASCENDING"
  }

  fields {
    field_path = "included_assets_uris_hash"
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

resource "google_firestore_index" "index-3" {
    project = var.tag_engine_project

    collection = "static_asset_configs"

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

    depends_on = [google_project_service.tag_engine_project]
}

resource "google_firestore_index" "index-4" {
    project = var.tag_engine_project

    collection = "static_asset_configs"

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


# dynamic table configs

resource "google_firestore_index" "index-5" {
    project = var.tag_engine_project

    collection = "dynamic_table_configs"

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

resource "google_firestore_index" "index-6" {
  project = var.tag_engine_project

  collection = "dynamic_table_configs"

  fields {
    field_path = "config_type"
    order      = "ASCENDING"
  }

  fields {
    field_path = "included_tables_uris_hash"
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


resource "google_firestore_index" "index-7" {
    project = var.tag_engine_project

    collection = "dynamic_table_configs"

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

    depends_on = [google_project_service.tag_engine_project]
}

# dynamic column configs

resource "google_firestore_index" "index-8" {
    project = var.tag_engine_project

    collection = "dynamic_column_configs"

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

resource "google_firestore_index" "index-9" {
  project = var.tag_engine_project

  collection = "dynamic_column_configs"

  fields {
    field_path = "config_type"
    order      = "ASCENDING"
  }

  fields {
    field_path = "included_tables_uris_hash"
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

resource "google_firestore_index" "index-10" {
    project = var.tag_engine_project

    collection = "dynamic_column_configs"

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

    depends_on = [google_project_service.tag_engine_project]
}


# entry configs

resource "google_firestore_index" "index-11" {
  project = var.tag_engine_project

  collection = "entry_configs"

  fields {
    field_path = "config_type"
    order      = "ASCENDING"
  }

  fields {
    field_path = "included_assets_hash"
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

resource "google_firestore_index" "index-12" {
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

  depends_on = [google_project_service.tag_engine_project]
}

resource "google_firestore_index" "index-13" {
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

    depends_on = [google_project_service.tag_engine_project]
}


# glossary asset configs
resource "google_firestore_index" "index-14" {
  project = var.tag_engine_project

  collection = "glossary_asset_configs"

  fields {
    field_path = "config_type"
    order      = "ASCENDING"
  }

  fields {
    field_path = "included_assets_hash"
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

resource "google_firestore_index" "index-15" {
    project = var.tag_engine_project

    collection = "glossary_asset_configs"

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

    depends_on = [google_project_service.tag_engine_project]
}

resource "google_firestore_index" "index-16" {
    project = var.tag_engine_project

    collection = "glossary_asset_configs"

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

# sensitive column configs

resource "google_firestore_index" "index-17" {
  project = var.tag_engine_project

  collection = "sensitive_column_configs"

  fields {
    field_path = "config_type"
    order      = "ASCENDING"
  }

  fields {
    field_path = "included_table_hash"
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

resource "google_firestore_index" "index-18" {
    project = var.tag_engine_project

    collection = "sensitive_column_configs"

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


resource "google_firestore_index" "index-19" {
    project = var.tag_engine_project

    collection = "sensitive_column_configs"

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

    depends_on = [google_project_service.tag_engine_project]
}

# restore configs

resource "google_firestore_index" "index-20" {
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

  depends_on = [google_project_service.tag_engine_project]
}


resource "google_firestore_index" "index-21" {
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

  depends_on = [google_project_service.tag_engine_project]
}


# import configs

resource "google_firestore_index" "index-22" {
    project = var.tag_engine_project

    collection = "import_configs"

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

resource "google_firestore_index" "index-23" {
    project = var.tag_engine_project

    collection = "import_configs"

    fields {
      field_path = "metadata_import_location"
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
