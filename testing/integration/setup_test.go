// Package integration provides integration tests for docql using real databases.
package integration

import (
	"context"
	"log"
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

		sharedCouchDBContainer = &CouchDBContainer{
			container: container,
			url:       "http://" + host + ":" + port.Port(),
			username:  "admin",
			password:  "password",
		}
		containersStarted.couchdb = true
	})

	return sharedCouchDBContainer
}
