export TAG_ENGINE_URL="tag-engine-develop.uc.r.appspot.com"
curl -X POST $TAG_ENGINE_URL/sensitive_column_tags -d @examples/sensitive_column_configs/sensitive_column_create_auto.json