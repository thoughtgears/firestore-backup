# Firestore backup and restore tool

This is a tool to backup and restore a Firestore database. It is written in Go and will run as a function on GCP. The 
tool can be run stand alone or with a scheduler function that backs up the database on a regular basis. The tool can 
also restore a database from a backup file. The tool is designed to be run on a GCP project that has a Firestore database
and a Cloud Storage bucket. The tool will create a folder in the bucket to store the backup files. 

## Prerequisites

* A GCP project with a Firestore database and a Cloud Storage bucket, ensure that the Firestore database is in Native mode.
* A GCP service account with the following roles:
  * Cloud Functions Developer
  * Cloud Functions Invoker
  * Cloud Scheduler Admin
  * Cloud Storage Admin
  * Firestore Admin
  * Service Account User
* Permissions to deploy Cloud Functions and Cloud Scheduler jobs

## Installation

### Deploying the backup function

```shell
gcloud functions deploy firestore-backup \
  --runtime go116 \
  --trigger-http \
  --service-account [service-account]
  
gcloud functions add-iam-policy-binding firestore-backup \
  --member=serviceAccount:[service-account] \
  --role=roles/functions.invoker
  
gcloud scheduler jobs create http firestore-backup \
  --schedule="0 0 * * *" \
  --uri=[function-url] \
  --oidc-service-account-email=[service-account] \
  --oidc-token-audience=[function-url]
  --body='{"project_id":"[project-id]","bucket_name":"[bucket-name]", "action":"backup"}'
```

### Calling the function to restore the database

```shell
curl -X POST -H "Content-Type: application/json" -d '{"project_id":"[project-id]","bucket_name":"[bucket-name]", "action":"restore"}' [function-url]
```
