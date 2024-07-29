# ************************************************** #
# Create the two custom IAM roles (needed by the SENSITIVE_COLUMN_CONFIG type)
# ************************************************** #

resource "google_project_iam_custom_role" "bigquery_schema_update" {
  project     = var.bigquery_project
  role_id     = "BigQuerySchemaUpdate"
  title       = "BigQuery Schema Update"
  description = "Custom role for updating the schema of a BigQuery table with policy tags"
  permissions = ["bigquery.tables.setCategory"]
  depends_on  = [google_project_service.tag_engine_project]
}

resource "google_project_iam_custom_role" "policy_tag_reader" {
  project     = var.data_catalog_project
  role_id     = "PolicyTagReader"
  title       = "BigQuery Policy Tag Reader"
  description = "Read Policy Tag Taxonomy"
  permissions = ["datacatalog.taxonomies.get", "datacatalog.taxonomies.list"]
  depends_on  = [google_project_service.tag_engine_project]
}

# ************************************************** #
# Create the project level policy bindings for tag_engine_sa
# ************************************************** #

resource "google_project_iam_member" "enqueuer" {
  project    = var.tag_engine_project
  role       = "roles/cloudtasks.enqueuer"
  member     = "serviceAccount:${var.tag_engine_sa}"
  depends_on = [google_project_service.tag_engine_project]
}

resource "google_project_iam_member" "taskRunner" {
  project    = var.tag_engine_project
  role       = "roles/cloudtasks.taskRunner"
  member     = "serviceAccount:${var.tag_engine_sa}"
  depends_on = [google_project_service.tag_engine_project]
}

resource "google_project_iam_member" "user" {
  project    = var.firestore_project
  role       = "roles/datastore.user"
  member     = "serviceAccount:${var.tag_engine_sa}"
  depends_on = [google_project_service.firestore_project]
}

resource "google_project_iam_member" "indexAdmin" {
  project    = var.firestore_project
  role       = "roles/datastore.indexAdmin"
  member     = "serviceAccount:${var.tag_engine_sa}"
  depends_on = [google_project_service.firestore_project]
}

resource "google_project_iam_member" "invoker" {
  project    = var.tag_engine_project
  role       = "roles/run.invoker"
  member     = "serviceAccount:${var.tag_engine_sa}"
  depends_on = [google_project_service.tag_engine_project]
}

resource "google_project_iam_member" "storage_object_get" {
  project    = var.tag_engine_project
  role       = "roles/storage.objectViewer"
  member     = "serviceAccount:${var.tag_engine_sa}"
  depends_on = [google_project_service.tag_engine_project]
}

resource "google_project_iam_member" "log_writer" {
  project    = var.tag_engine_project
  role       = "roles/logging.logWriter"
  member     = "serviceAccount:${var.tag_engine_sa}"
  depends_on = [google_project_service.tag_engine_project]
}

resource "google_project_iam_member" "repo_admin" {
  project    = var.tag_engine_project
  role       = "roles/artifactregistry.repoAdmin"
  member     = "serviceAccount:${var.tag_engine_sa}"
  depends_on = [google_project_service.tag_engine_project]
}

# ************************************************************ #
# Create the project level policy bindings for tag_creator_sa
# ************************************************************ #

resource "google_project_iam_member" "tagEditor" {
  project    = var.tag_engine_project
  role       = "roles/datacatalog.tagEditor"
  member     = "serviceAccount:${var.tag_creator_sa}"
  depends_on = [google_project_service.tag_engine_project]
}

resource "google_project_iam_member" "tagTemplateUser" {
  project    = var.tag_engine_project
  role       = "roles/datacatalog.tagTemplateUser"
  member     = "serviceAccount:${var.tag_creator_sa}"
  depends_on = [google_project_service.tag_engine_project]
}

resource "google_project_iam_member" "tagTemplateViewer" {
  project    = var.tag_engine_project
  role       = "roles/datacatalog.tagTemplateViewer"
  member     = "serviceAccount:${var.tag_creator_sa}"
  depends_on = [google_project_service.tag_engine_project]
}

resource "google_project_iam_member" "viewer" {
  project    = var.data_catalog_project
  role       = "roles/datacatalog.viewer"
  member     = "serviceAccount:${var.tag_creator_sa}"
  depends_on = [google_project_service.tag_engine_project]
}

resource "google_project_iam_member" "dataEditor" {
  project    = var.bigquery_project
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${var.tag_creator_sa}"
  depends_on = [google_project_service.bigquery_project]
}

resource "google_project_iam_member" "jobUser" {
  project    = var.tag_engine_project
  role       = "roles/bigquery.jobUser"
  member     = "serviceAccount:${var.tag_creator_sa}"
  depends_on = [google_project_service.tag_engine_project]
}

resource "google_project_iam_member" "metadataViewer" {
  project    = var.bigquery_project
  role       = "roles/bigquery.metadataViewer"
  member     = "serviceAccount:${var.tag_creator_sa}"
  depends_on = [google_project_service.bigquery_project]
}

resource "google_project_iam_member" "loggingViewer" {
  project    = var.tag_engine_project
  role       = "roles/logging.viewer"
  member     = "serviceAccount:${var.tag_creator_sa}"
  depends_on = [google_project_service.tag_engine_project]
}

resource "google_project_iam_member" "BigQuerySchemaUpdate" {
  project    = var.bigquery_project
  role       = "projects/${var.bigquery_project}/roles/BigQuerySchemaUpdate"
  member     = "serviceAccount:${var.tag_creator_sa}"
  depends_on = [google_project_iam_custom_role.bigquery_schema_update]
}

resource "google_project_iam_member" "PolicyTagReader" {
  project    = var.data_catalog_project
  role       = "projects/${var.data_catalog_project}/roles/PolicyTagReader"
  member     = "serviceAccount:${var.tag_creator_sa}"
  depends_on = [google_project_iam_custom_role.policy_tag_reader]
}

# ************************************************************ #
# Create the service account policy bindings for tag_engine_sa
# ************************************************************ #

resource "google_service_account_iam_member" "serviceAccountUser_tag_engine_sa" {
  service_account_id = "projects/${var.tag_engine_project}/serviceAccounts/${var.tag_engine_sa}"
  role               = "roles/iam.serviceAccountUser"
  member             = "serviceAccount:${var.tag_engine_sa}"
  depends_on         = [google_project_service.tag_engine_project]
}

resource "google_service_account_iam_member" "serviceAccountUser_tag_creator_sa" {
  service_account_id = "projects/${var.tag_engine_project}/serviceAccounts/${var.tag_creator_sa}"
  role               = "roles/iam.serviceAccountUser"
  member             = "serviceAccount:${var.tag_engine_sa}"
  depends_on         = [google_project_service.tag_engine_project]
}

resource "google_service_account_iam_member" "serviceAccountViewer_tag_creator_sa" {
  service_account_id = "projects/${var.tag_engine_project}/serviceAccounts/${var.tag_creator_sa}"
  role               = "roles/iam.serviceAccountViewer"
  member             = "serviceAccount:${var.tag_engine_sa}"
  depends_on         = [google_project_service.tag_engine_project]
}

resource "google_service_account_iam_member" "serviceAccountTokenCreator_tag_creator_sa" {
  service_account_id = "projects/${var.tag_engine_project}/serviceAccounts/${var.tag_creator_sa}"
  role               = "roles/iam.serviceAccountTokenCreator"
  member             = "serviceAccount:${var.tag_engine_sa}"
  depends_on         = [google_project_service.tag_engine_project]
}

# ************************************************************ #
# Create the storage bucket policy binding for tag_creator_sa
# ************************************************************ #

module "storage_bucket-iam-bindings" {
  source          = "terraform-google-modules/iam/google//modules/storage_buckets_iam"
  storage_buckets = ["${var.csv_bucket}"]
  mode            = "additive"

  bindings = {
    "roles/storage.legacyBucketReader" = [
      "serviceAccount:${var.tag_creator_sa}",
    ]
  }
  depends_on = [google_project_service.data_catalog_project]
}
