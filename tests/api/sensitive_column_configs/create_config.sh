export TAG_ENGINE_URL="https://tag-engine-eshsagj3ta-uc.a.run.app"
export ID_TOKEN=$(gcloud auth print-identity-token)
curl -X POST $TAG_ENGINE_URL/create_sensitive_column_config -d sensitive_column_auto.json -H "Authorization: Bearer $ID_TOKEN"