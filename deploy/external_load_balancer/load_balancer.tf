resource "google_compute_region_network_endpoint_group" "default" {
  provider              = google-beta
  project               = var.tag_engine_project
  name                  = "tag-engine-neg"
  network_endpoint_type = "SERVERLESS"
  region                = var.tag_engine_region
  cloud_run {
    service = google_cloud_run_v2_service.ui_service.name
  }
  
  depends_on = [google_project_service.tag_engine_project]
}

module "lb-http" {
  source            = "GoogleCloudPlatform/lb-http/google//modules/serverless_negs"
  version           = "~> 9.0"

  project           = var.tag_engine_project
  name              = "tag-engine-lb"

  ssl                             = true
  managed_ssl_certificate_domains = ["${var.domain_name}"]
  https_redirect                  = true
  backends = {
    default = {
      description                     = null
      protocol                        = "HTTPS"
      #port_name                       = "https"
      enable_cdn                      = false
      custom_request_headers          = null
      custom_response_headers         = null
      security_policy                 = null
      compression_mode                = null


      log_config = {
        enable = true
        sample_rate = 1.0
      }

      groups = [
        {
          # Your serverless service should have a NEG created that's referenced here.
          group = google_compute_region_network_endpoint_group.default.id
        }
      ]

      iap_config = {
        enable               = var.use_iap
        oauth2_client_id     = var.oauth_client_id
        oauth2_client_secret = var.oauth_client_secret
      }
    }
  }
}

output "external_ip" {
  value = module.lb-http.external_ip
}


# ************************************************************ #
# Post-processing: update DNS A record and create IAP SA
# ************************************************************ #

resource "null_resource" "update_dns" {

 provisioner "local-exec" {

    command = "/bin/bash update_dns_record.sh ${var.domain_name} ${module.lb-http.external_ip} ${var.dns_zone}"
  }
  
  depends_on = [module.lb-http.external_ip]
}


resource "null_resource" "create_iap_sa" {

 provisioner "local-exec" {

    command = "/bin/bash create_iap_sa.sh ${var.tag_engine_project}"
  }
  
  depends_on = [module.lb-http.external_ip]
}

resource "google_project_iam_member" "member-role" {
  for_each = toset(var.authorized_user_accounts)
  role = "roles/iap.httpsResourceAccessor"
  member = "user:${each.value}"
  project = var.tag_engine_project
  depends_on = [null_resource.create_iap_sa]
}
