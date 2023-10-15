#!/bin/bash

gcloud beta services identity create --service=iap.googleapis.com --project=$1 &> iap.tmp
cat iap.tmp
SA=$(cat iap.tmp | cut -d':' -f 2 | xargs)
echo $SA

gcloud projects add-iam-policy-binding $1 \
	--member=serviceAccount:$SA \
	--role=roles/run.invoker

rm iap.tmp