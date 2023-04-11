#!/bin/bash
source /Users/scohen/tag_eng_run/bin/activate

export PROJECT_ID="tag-engine-run"
export GOOGLE_APPLICATION_CREDENTIALS=/Users/scohen/keys/python-client-tag-engine-run.json
export TAG_ENGINE_URL="https://tag-engine-eshsagj3ta-uc.a.run.app"

gcloud config set project $PROJECT_ID
