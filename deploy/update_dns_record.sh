#!/bin/bash

gcloud dns record-sets update $1. --rrdatas=$2 --type=A --ttl=60 --zone=$3