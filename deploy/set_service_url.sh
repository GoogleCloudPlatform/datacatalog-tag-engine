#!/bin/bash

export API_SERVICE_URL=`gcloud run services describe tag-engine-api --format="value(status.url)"`
gcloud run services update tag-engine-api --set-env-vars SERVICE_URL=$1

export UI_SERVICE_URL=`gcloud run services describe tag-engine-ui --format="value(status.url)"`
gcloud run services update tag-engine-ui --set-env-vars SERVICE_URL=$2
