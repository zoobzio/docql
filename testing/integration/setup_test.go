// Package integration provides integration tests for docql using real databases.
package integration

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/mongodb"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// Shared containers - lazily initialized.
var (
	sharedMongoContainer   *MongoContainer
	sharedCouchDBContainer *CouchDBContainer

	mongoOnce   sync.Once
	couchDBOnce sync.Once

	// Track which containers were started for cleanup.
	containersStarted = struct {
		mongo   bool
		couchdb bool
	}{}
)

// TestMain sets up shared containers for all integration tests.
func TestMain(m *testing.M) {
	// Run tests
	code := m.Run()

	// Cleanup any containers that were started
	ctx := context.Background()

	if containersStarted.mongo && sharedMongoContainer != nil {
		if sharedMongoContainer.client != nil {
			_ = sharedMongoContainer.client.Disconnect(ctx)
		}
		if sharedMongoContainer.container != nil {
			_ = sharedMongoContainer.container.Terminate(ctx)
		}
	}

	if containersStarted.couchdb && sharedCouchDBContainer != nil {
		if sharedCouchDBContainer.container != nil {
			_ = sharedCouchDBContainer.container.Terminate(ctx)
		}
	}

	os.Exit(code)
}

// MongoContainer wraps a testcontainers MongoDB instance.
type MongoContainer struct {
	container *mongodb.MongoDBContainer
	client    *mongo.Client
	connStr   string
}

// CouchDBContainer wraps a testcontainers CouchDB instance.
type CouchDBContainer struct {
	container testcontainers.Container
	url       string
	username  string
	password  string
}

// getMongoContainer returns the shared MongoDB container, starting it if needed.
func getMongoContainer(t *testing.T) *MongoContainer {
	t.Helper()

	mongoOnce.Do(func() {
		ctx := context.Background()

		container, err := mongodb.Run(ctx,
			"docker.io/mongo:7",
			testcontainers.WithWaitStrategy(
				wait.ForLog("Waiting for connections").
					WithStartupTimeout(60*time.Second),
			),
		)
		if err != nil {
			log.Fatalf("Failed to start mongodb container: %v", err)
		}

		connStr, err := container.ConnectionString(ctx)
		if err != nil {
			log.Fatalf("Failed to get connection string: %v", err)
		}

		client, err := mongo.Connect(options.Client().ApplyURI(connStr))
		if err != nil {
			log.Fatalf("Failed to connect to mongodb: %v", err)
		}

		// Verify connection
		if err := client.Ping(ctx, nil); err != nil {
			log.Fatalf("Failed to ping mongodb: %v", err)
		}

		sharedMongoContainer = &MongoContainer{
			container: container,
			client:    client,
			connStr:   connStr,
		}
		containersStarted.mongo = true
	})

	return sharedMongoContainer
}

// getCouchDBContainer returns the shared CouchDB container, starting it if needed.
func getCouchDBContainer(t *testing.T) *CouchDBContainer {
	t.Helper()

	couchDBOnce.Do(func() {
		ctx := context.Background()

		req := testcontainers.ContainerRequest{
			Image:        "docker.io/couchdb:3",
			ExposedPorts: []string{"5984/tcp"},
			Env: map[string]string{
				"COUCHDB_USER":     "admin",
				"COUCHDB_PASSWORD": "password",
			},
			WaitingFor: wait.ForHTTP("/_up").
				WithPort("5984/tcp").
				WithStartupTimeout(60 * time.Second),
		}

		container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
			ContainerRequest: req,
			Started:          true,
		})
		if err != nil {
			log.Fatalf("Failed to start couchdb container: %v", err)
		}

		host, err := container.Host(ctx)
		if err != nil {
			log.Fatalf("Failed to get couchdb host: %v", err)
		}

		port, err := container.MappedPort(ctx, "5984")
		if err != nil {
			log.Fatalf("Failed to get couchdb port: %v", err)
		}

		url := "http://" + host + ":" + port.Port()

		// Finish single-node cluster setup for CouchDB 3.x
		if err := finishCouchDBSetup(url, "admin", "password"); err != nil {
			log.Fatalf("Failed to finish CouchDB setup: %v", err)
		}

		sharedCouchDBContainer = &CouchDBContainer{
			container: container,
			url:       url,
			username:  "admin",
			password:  "password",
		}
		containersStarted.couchdb = true
	})

	return sharedCouchDBContainer
}

// finishCouchDBSetup completes the single-node cluster setup for CouchDB 3.x.
func finishCouchDBSetup(url, username, password string) error {
	client := &http.Client{Timeout: 10 * time.Second}

	// Wait for authentication to be ready (retry a few times)
	var lastErr error
	for i := 0; i < 10; i++ {
		req, err := http.NewRequest("GET", url+"/_session", nil)
		if err != nil {
			return err
		}
		req.SetBasicAuth(username, password)

		resp, err := client.Do(req)
		if err != nil {
			lastErr = err
			time.Sleep(500 * time.Millisecond)
			continue
		}

		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		if resp.StatusCode == 200 {
			// Check if we're authenticated as admin
			if bytes.Contains(body, []byte(`"roles":["_admin"]`)) {
				// Enable single-node mode
				return enableSingleNode(client, url, username, password)
			}
		}

		lastErr = fmt.Errorf("auth check returned status %d: %s", resp.StatusCode, string(body))
		time.Sleep(500 * time.Millisecond)
	}

	return fmt.Errorf("authentication not ready after retries: %w", lastErr)
}

func enableSingleNode(client *http.Client, url, username, password string) error {
	payload := []byte(`{"action": "enable_single_node", "username": "` + username + `", "password": "` + password + `", "bind_address": "0.0.0.0", "port": 5984}`)
	req, err := http.NewRequest("POST", url+"/_cluster_setup", bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.SetBasicAuth(username, password)
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// 200, 201, or 400 (already configured) are all acceptable
	if resp.StatusCode != 200 && resp.StatusCode != 201 && resp.StatusCode != 400 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("cluster setup failed with status %d: %s", resp.StatusCode, string(body))
	}

	return nil
}

// skipIfNoDynamoDB skips the test if DynamoDB Local is not available.
// DynamoDB Local can be run as a Docker container but requires table setup.
// These tests validate query rendering without requiring a live connection.
func skipIfNoDynamoDB(t *testing.T) {
	t.Helper()

	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// DynamoDB Local requires explicit setup
	// If DYNAMODB_ENDPOINT is not set, run as render-only test
	if os.Getenv("DYNAMODB_ENDPOINT") == "" {
		// Still run the test but without actual API calls
		// These tests validate query rendering
		return
	}
}

// skipIfNoFirestore skips the test if Firestore Emulator is not available.
// Firestore Emulator requires gcloud SDK and explicit setup.
// These tests validate query rendering without requiring a live connection.
func skipIfNoFirestore(t *testing.T) {
	t.Helper()

	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Firestore Emulator requires explicit setup
	// If FIRESTORE_EMULATOR_HOST is not set, run as render-only test
	if os.Getenv("FIRESTORE_EMULATOR_HOST") == "" {
		// Still run the test but without actual API calls
		// These tests validate query rendering
		return
	}
}
