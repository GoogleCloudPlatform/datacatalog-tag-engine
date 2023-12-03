# ************************************************** #
# Create the two custom IAM roles (needed by the SENSITIVE_COLUMN_CONFIG type)
# ************************************************** #

resource "google_project_iam_custom_role" "bigquery_schema_update" {  
  project            	= var.bigquery_project
  role_id              = "BigQuerySchemaUpdate"
  title                = "BigQuery Schema Update"
  description          = "Custom role for updating the schema of a BigQuery table with policy tags"
  permissions          = ["bigquery.tables.setCategory"]
  depends_on = [google_project_service.tag_engine_project]
}

resource "google_project_iam_custom_role" "policy_tag_reader" {  
  project              = var.tag_engine_project
  role_id              = "PolicyTagReader"
  title                = "BigQuery Policy Tag Reader"
  description          = "Read Policy Tag Taxonomy"
  permissions          = ["datacatalog.taxonomies.get","datacatalog.taxonomies.list"]
  depends_on = [google_project_service.tag_engine_project]
}

# ************************************************** #
# Create the project level policy bindings for tag_engine_sa
# ************************************************** #

resource "google_project_iam_binding" "enqueuer" {
  project = var.tag_engine_project
  role    = "roles/cloudtasks.enqueuer"
  members = ["serviceAccount:${var.tag_engine_sa}"]
  depends_on = [google_project_service.tag_engine_project]
}

resource "google_project_iam_binding" "taskRunner" {
  project = var.tag_engine_project
  role    = "roles/cloudtasks.taskRunner"
  members = ["serviceAccount:${var.tag_engine_sa}"]
  depends_on = [google_project_service.tag_engine_project]
}
	 
resource "google_project_iam_binding" "user" {
  project = var.tag_engine_project
  role    = "roles/datastore.user"
  members = ["serviceAccount:${var.tag_engine_sa}"]
  depends_on = [google_project_service.tag_engine_project]
}

resource "google_project_iam_binding" "indexAdmin" {
  project = var.tag_engine_project
  role    = "roles/datastore.indexAdmin"
  members = ["serviceAccount:${var.tag_engine_sa}"]
  depends_on = [google_project_service.tag_engine_project]
}

resource "google_project_iam_binding" "invoker" {
  project = var.tag_engine_project
  role    = "roles/run.invoker"
  members = ["serviceAccount:${var.tag_engine_sa}"]
  depends_on = [google_project_service.tag_engine_project]
}

resource "google_project_iam_binding" "storage_object_get" {
  project = var.tag_engine_project
  role    = "roles/storage.objectViewer"
  members = ["serviceAccount:${var.tag_engine_sa}"]
  depends_on = [google_project_service.tag_engine_project]
}

resource "google_project_iam_binding" "log_writer" {
  project = var.tag_engine_project
  role    = "roles/logging.logWriter"
  members = ["serviceAccount:${var.tag_engine_sa}"]
  depends_on = [google_project_service.tag_engine_project]
}

resource "google_project_iam_binding" "repo_admin" {
  project = var.tag_engine_project
  role    = "roles/artifactregistry.repoAdmin"
  members = ["serviceAccount:${var.tag_engine_sa}"]
  depends_on = [google_project_service.tag_engine_project]
}
 
# ************************************************************ #
# Create the project level policy bindings for tag_creator_sa
# ************************************************************ #

resource "google_project_iam_binding" "tagEditor" {
  project = var.tag_engine_project
  role    = "roles/datacatalog.tagEditor"
  members = ["serviceAccount:${var.tag_creator_sa}"]
  depends_on = [google_project_service.tag_engine_project]
}
	
resource "google_project_iam_binding" "tagTemplateUser" {
  project = var.tag_engine_project
  role    = "roles/datacatalog.tagTemplateUser"
  members = ["serviceAccount:${var.tag_creator_sa}"]
  depends_on = [google_project_service.tag_engine_project]
}

resource "google_project_iam_binding" "tagTemplateViewer" {
  project = var.tag_engine_project
  role    = "roles/datacatalog.tagTemplateViewer"
  members = ["serviceAccount:${var.tag_creator_sa}"]
  depends_on = [google_project_service.tag_engine_project]
}

resource "google_project_iam_binding" "viewer" {
  project = var.tag_engine_project
  role    = "roles/datacatalog.viewer"
  members = ["serviceAccount:${var.tag_creator_sa}"]
  depends_on = [google_project_service.tag_engine_project]
}

resource "google_project_iam_binding" "dataEditor" {
  project = var.tag_engine_project
  role    = "roles/bigquery.dataEditor"
  members = ["serviceAccount:${var.tag_creator_sa}"]
  depends_on = [google_project_service.tag_engine_project]
}

resource "google_project_iam_binding" "jobUser" {
  project = var.tag_engine_project
  role    = "roles/bigquery.jobUser"
  members = ["serviceAccount:${var.tag_creator_sa}"]
  depends_on = [google_project_service.tag_engine_project]
}
	
resource "google_project_iam_binding" "metadataViewer" {
  project = var.tag_engine_project
  role    = "roles/bigquery.metadataViewer"
  members = ["serviceAccount:${var.tag_creator_sa}"]
  depends_on = [google_project_service.tag_engine_project]
}

resource "google_project_iam_binding" "loggingViewer" {
  project = var.tag_engine_project
  role    = "roles/logging.viewer"
  members = ["serviceAccount:${var.tag_creator_sa}"]
  depends_on = [google_project_service.tag_engine_project]
}

resource "google_project_iam_binding" "BigQuerySchemaUpdate" {
  project = var.bigquery_project
  role    = "projects/${var.bigquery_project}/roles/BigQuerySchemaUpdate"
  members = ["serviceAccount:${var.tag_creator_sa}"]
  depends_on = [google_project_iam_custom_role.bigquery_schema_update]
}

resource "google_project_iam_binding" "PolicyTagReader" {
  project = var.tag_engine_project
  role    = "projects/${var.tag_engine_project}/roles/PolicyTagReader"
  members = ["serviceAccount:${var.tag_creator_sa}"]
  depends_on = [google_project_iam_custom_role.policy_tag_reader]
}

# ************************************************************ #
# Create the service account policy bindings for tag_engine_sa
# ************************************************************ #

resource "google_service_account_iam_binding" "serviceAccountUser_tag_engine_sa" {
  service_account_id = "projects/${var.tag_engine_project}/serviceAccounts/${var.tag_engine_sa}"
  role = "roles/iam.serviceAccountUser"
  members = [
    "serviceAccount:${var.tag_engine_sa}",
  ]
  depends_on = [google_project_service.tag_engine_project]
}

resource "google_service_account_iam_binding" "serviceAccountUser_tag_creator_sa" {
  service_account_id = "projects/${var.tag_engine_project}/serviceAccounts/${var.tag_creator_sa}"
  role = "roles/iam.serviceAccountUser"
  members = [
    "serviceAccount:${var.tag_engine_sa}",
  ]
  depends_on = [google_project_service.tag_engine_project]
}

resource "google_service_account_iam_binding" "serviceAccountViewer_tag_creator_sa" {
  service_account_id = "projects/${var.tag_engine_project}/serviceAccounts/${var.tag_creator_sa}"
  role = "roles/iam.serviceAccountViewer"
  members = [
    "serviceAccount:${var.tag_engine_sa}",
  ]
  depends_on = [google_project_service.tag_engine_project]
}

resource "google_service_account_iam_binding" "serviceAccountTokenCreator_tag_creator_sa" {
  service_account_id = "projects/${var.tag_engine_project}/serviceAccounts/${var.tag_creator_sa}"
  role = "roles/iam.serviceAccountTokenCreator"
  members = [
    "serviceAccount:${var.tag_engine_sa}",
  ]
  depends_on = [google_project_service.tag_engine_project]
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
  depends_on = [google_project_service.tag_engine_project]
}
