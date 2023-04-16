package firestore_backup

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	apiv1 "cloud.google.com/go/firestore/apiv1/admin"
	"github.com/GoogleCloudPlatform/functions-framework-go/functions"
	"google.golang.org/genproto/googleapis/firestore/admin/v1"
)

func init() {
	functions.HTTP("Function", function)
}

type backupRequest struct {
	Action      string   `json:"action"`
	Collections []string `json:"collections"`
	ProjectId   string   `json:"project_id"`
	Bucket      string   `json:"bucket"`
}

// function is the entry point for the cloudfunction that will either backup a firestore database
// or restores a firestore database from a backup. The function is triggered by a cloud scheduler
// job. You can invoke the function manually to restore collections by calling the function with the following
// body: {"action": "restore", "collections": ["collection1", "collection2"], "project": "my-project"}
func function(w http.ResponseWriter, r *http.Request) {
	var req backupRequest

	body, err := io.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}

	if err := json.Unmarshal(body, &req); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}

	if err := req.validate(); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(err.Error()))
		return
	}

	client, err := apiv1.NewFirestoreAdminClient(r.Context())
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}

	switch req.Action {
	case "back":
		if err := req.backup(r.Context(), client); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error()))
			return
		}
	case "restore":
		if err := req.restore(r.Context(), client); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error()))
			return
		}
	}
}

// backup will backup all collections or a subset of collections in a firestore database to a bucket.
// This is called when the action is set to "backup" in the request body.
// Name: The database resource name. For example: projects/{project_id}/databases/{database_id}
// CollectionIds: The collection IDs to export. Unspecified means all collections.
// OutputUriPrefix: Supports Google Cloud Storage URIs of the form: gs://BUCKET_NAME[/NAMESPACE_PATH]
// By default all collections will be exported to the bucket with a namespace of firestore-backup
func (r *backupRequest) backup(ctx context.Context, client *apiv1.FirestoreAdminClient) error {
	op, err := client.ExportDocuments(ctx, &admin.ExportDocumentsRequest{
		Name:            fmt.Sprintf("projects/%s/databases/(default)", r.ProjectId),
		CollectionIds:   r.Collections,
		OutputUriPrefix: fmt.Sprintf("gs://%s/firestore-backup", r.Bucket),
	}, nil)
	if err != nil {
		return fmt.Errorf("error backing up firestore database: %v", err)
	}

	if _, err := op.Wait(ctx); err != nil {
		return fmt.Errorf("error backing up firestore database: %v", err)
	}

	return nil
}

// restore will restore all collections or a subset of collections in a firestore database from a bucket.
// This is called when the action is set to "restore" in the request body.
// Name: The database resource name. For example: projects/{project_id}/databases/{database_id}
// CollectionIds: The collection IDs to export. Unspecified means all collections.
// InputUriPrefix: Supports Google Cloud Storage URIs of the form: gs://BUCKET_NAME[/NAMESPACE_PATH]
func (r *backupRequest) restore(ctx context.Context, client *apiv1.FirestoreAdminClient) error {
	op, err := client.ImportDocuments(ctx, &admin.ImportDocumentsRequest{
		Name:           fmt.Sprintf("projects/%s/databases/(default)", r.ProjectId),
		CollectionIds:  r.Collections,
		InputUriPrefix: fmt.Sprintf("gs://%s/firestore-backup", r.Bucket),
	}, nil)
	if err != nil {
		return fmt.Errorf("error restoring firestore database: %v", err)
	}

	if err := op.Wait(ctx); err != nil {
		return fmt.Errorf("error backing up firestore database: %v", err)
	}

	return nil
}

// validate will validate the request body to ensure that the required fields are present.
func (r *backupRequest) validate() error {
	if r.ProjectId == "" {
		return fmt.Errorf("project is required")
	}

	if r.Bucket == "" {
		return fmt.Errorf("bucket is required")
	}

	if r.Action == "" {
		return fmt.Errorf("action is required")
	}

	if r.Action != "backup" && r.Action != "restore" {
		return fmt.Errorf("action must be either backup or restore")
	}

	if r.Action == "backup" && r.Bucket == "" {
		return fmt.Errorf("bucket is required when action is backup")
	}

	return nil
}
