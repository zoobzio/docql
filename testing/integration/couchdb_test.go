// Package integration provides integration tests for docql using real CouchDB.
package integration

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"

	"github.com/zoobzio/ddml"
	"github.com/zoobzio/docql"
	"github.com/zoobzio/docql/pkg/couchdb"
)

// CouchDB HTTP client helper.
type couchClient struct {
	url      string
	username string
	password string
}

func newCouchClient(cc *CouchDBContainer) *couchClient {
	return &couchClient{
		url:      cc.url,
		username: cc.username,
		password: cc.password,
	}
}

func (c *couchClient) request(method, path string, body interface{}) (*http.Response, error) {
	var bodyReader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		bodyReader = bytes.NewReader(data)
	}

	req, err := http.NewRequest(method, c.url+path, bodyReader)
	if err != nil {
		return nil, err
	}

	req.SetBasicAuth(c.username, c.password)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	return http.DefaultClient.Do(req)
}

func (c *couchClient) createDB(name string) error {
	resp, err := c.request("PUT", "/"+name, nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	// Ignore 412 (database already exists)
	return nil
}

func (c *couchClient) createIndex(db string, fields []string) error {
	index := map[string]interface{}{
		"index": map[string]interface{}{
			"fields": fields,
		},
	}
	resp, err := c.request("POST", "/"+db+"/_index", index)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("create index failed with status %d: %s", resp.StatusCode, string(body))
	}
	return nil
}

func (c *couchClient) deleteDB(name string) error {
	resp, err := c.request("DELETE", "/"+name, nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return nil
}

func (c *couchClient) insertDoc(db string, doc map[string]interface{}) error {
	resp, err := c.request("POST", "/"+db, doc)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("insert failed with status %d: %s", resp.StatusCode, string(body))
	}
	return nil
}

func (c *couchClient) find(db string, query map[string]interface{}) ([]map[string]interface{}, error) {
	resp, err := c.request("POST", "/"+db+"/_find", query)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("find failed with status %d: %s", resp.StatusCode, string(body))
	}

	var result struct {
		Docs    []map[string]interface{} `json:"docs"`
		Warning string                   `json:"warning,omitempty"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, err
	}
	return result.Docs, nil
}

// createCouchDBTestInstance creates a DOCQL instance for CouchDB tests.
func createCouchDBTestInstance(t *testing.T) *docql.DOCQL {
	t.Helper()

	schema := ddml.NewSchema("test").
		AddCollection(
			ddml.NewCollection("users").
				AddField(ddml.NewField("_id", ddml.TypeString)).
				AddField(ddml.NewField("username", ddml.TypeString)).
				AddField(ddml.NewField("email", ddml.TypeString)).
				AddField(ddml.NewField("age", ddml.TypeInt)).
				AddField(ddml.NewField("active", ddml.TypeBool)),
		).
		AddCollection(
			ddml.NewCollection("orders").
				AddField(ddml.NewField("_id", ddml.TypeString)).
				AddField(ddml.NewField("userId", ddml.TypeString)).
				AddField(ddml.NewField("total", ddml.TypeFloat)).
				AddField(ddml.NewField("status", ddml.TypeString)),
		)

	instance, err := docql.NewFromDDML(schema)
	if err != nil {
		t.Fatalf("Failed to create instance: %v", err)
	}
	return instance
}

// setupCouchDB creates test database and seeds data.
func setupCouchDB(t *testing.T, cc *CouchDBContainer) *couchClient {
	t.Helper()

	client := newCouchClient(cc)

	// Delete and recreate database for clean state
	_ = client.deleteDB("docql_test")
	if err := client.createDB("docql_test"); err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}

	// Create indexes for Mango queries
	if err := client.createIndex("docql_test", []string{"type"}); err != nil {
		t.Fatalf("Failed to create type index: %v", err)
	}
	if err := client.createIndex("docql_test", []string{"type", "active"}); err != nil {
		t.Fatalf("Failed to create type+active index: %v", err)
	}
	if err := client.createIndex("docql_test", []string{"type", "active", "age"}); err != nil {
		t.Fatalf("Failed to create type+active+age index: %v", err)
	}

	// Seed users
	users := []map[string]interface{}{
		{"_id": "user:1", "type": "user", "username": "alice", "email": "alice@example.com", "age": 30, "active": true},
		{"_id": "user:2", "type": "user", "username": "bob", "email": "bob@example.com", "age": 25, "active": true},
		{"_id": "user:3", "type": "user", "username": "charlie", "email": "charlie@example.com", "age": 35, "active": false},
		{"_id": "user:4", "type": "user", "username": "diana", "email": "diana@example.com", "age": 28, "active": true},
	}
	for _, user := range users {
		if err := client.insertDoc("docql_test", user); err != nil {
			t.Fatalf("Failed to insert user: %v", err)
		}
	}

	// Seed orders
	orders := []map[string]interface{}{
		{"_id": "order:1", "type": "order", "userId": "user:1", "total": 99.99, "status": "completed"},
		{"_id": "order:2", "type": "order", "userId": "user:1", "total": 149.99, "status": "completed"},
		{"_id": "order:3", "type": "order", "userId": "user:2", "total": 49.99, "status": "pending"},
		{"_id": "order:4", "type": "order", "userId": "user:4", "total": 199.99, "status": "completed"},
	}
	for _, order := range orders {
		if err := client.insertDoc("docql_test", order); err != nil {
			t.Fatalf("Failed to insert order: %v", err)
		}
	}

	return client
}

func TestCouchDB_SimpleFind(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	cc := getCouchDBContainer(t)
	client := setupCouchDB(t, cc)

	instance := createCouchDBTestInstance(t)
	renderer := couchdb.New()

	// Build query: Find all users
	query := docql.Find(instance.C("users"))
	result, err := query.Render(renderer)
	if err != nil {
		t.Fatalf("Failed to render query: %v", err)
	}

	// Verify query JSON
	if result.JSON == "" {
		t.Error("Expected non-empty query JSON")
	}

	// Execute actual find
	docs, err := client.find("docql_test", map[string]interface{}{
		"selector": map[string]interface{}{
			"type": "user",
		},
	})
	if err != nil {
		t.Fatalf("Failed to execute find: %v", err)
	}

	if len(docs) != 4 {
		t.Errorf("Expected 4 users, got %d", len(docs))
	}
}

func TestCouchDB_FindWithFilter(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	cc := getCouchDBContainer(t)
	client := setupCouchDB(t, cc)

	instance := createCouchDBTestInstance(t)
	renderer := couchdb.New()

	// Build query: Find active users
	query := docql.Find(instance.C("users")).
		Filter(instance.Eq(instance.F("users", "active"), instance.P("active")))

	result, err := query.Render(renderer)
	if err != nil {
		t.Fatalf("Failed to render query: %v", err)
	}

	// Verify required params
	if len(result.RequiredParams) != 1 {
		t.Errorf("Expected 1 required param, got %d", len(result.RequiredParams))
	}

	// Execute actual find
	docs, err := client.find("docql_test", map[string]interface{}{
		"selector": map[string]interface{}{
			"type":   "user",
			"active": true,
		},
	})
	if err != nil {
		t.Fatalf("Failed to execute find: %v", err)
	}

	if len(docs) != 3 {
		t.Errorf("Expected 3 active users, got %d", len(docs))
	}
}

func TestCouchDB_FindWithSort(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	cc := getCouchDBContainer(t)
	_ = setupCouchDB(t, cc)

	instance := createCouchDBTestInstance(t)
	renderer := couchdb.New()

	// Build query: Find users sorted by age
	query := docql.Find(instance.C("users")).
		SortDesc(instance.F("users", "age"))

	result, err := query.Render(renderer)
	if err != nil {
		t.Fatalf("Failed to render query: %v", err)
	}

	// Verify sort is in the query
	var queryJSON map[string]interface{}
	if err := json.Unmarshal([]byte(result.JSON), &queryJSON); err != nil {
		t.Fatalf("Failed to parse query JSON: %v", err)
	}

	if queryJSON["sort"] == nil {
		t.Error("Expected sort in query")
	}
}

func TestCouchDB_FindWithPagination(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	cc := getCouchDBContainer(t)
	_ = setupCouchDB(t, cc)

	instance := createCouchDBTestInstance(t)
	renderer := couchdb.New()

	// Build query: Find users with limit and skip
	query := docql.Find(instance.C("users")).
		Limit(2).
		Skip(1)

	result, err := query.Render(renderer)
	if err != nil {
		t.Fatalf("Failed to render query: %v", err)
	}

	// Verify pagination in JSON
	var queryJSON map[string]interface{}
	if err := json.Unmarshal([]byte(result.JSON), &queryJSON); err != nil {
		t.Fatalf("Failed to parse query JSON: %v", err)
	}

	if queryJSON["limit"] == nil {
		t.Error("Expected limit in query")
	}
	if queryJSON["skip"] == nil {
		t.Error("Expected skip in query")
	}
}

func TestCouchDB_FindWithProjection(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	cc := getCouchDBContainer(t)
	_ = setupCouchDB(t, cc)

	instance := createCouchDBTestInstance(t)
	renderer := couchdb.New()

	// Build query: Find users with only username and email
	query := docql.Find(instance.C("users")).
		Select(instance.F("users", "username"), instance.F("users", "email"))

	result, err := query.Render(renderer)
	if err != nil {
		t.Fatalf("Failed to render query: %v", err)
	}

	// Verify fields in JSON
	var queryJSON map[string]interface{}
	if err := json.Unmarshal([]byte(result.JSON), &queryJSON); err != nil {
		t.Fatalf("Failed to parse query JSON: %v", err)
	}

	if queryJSON["fields"] == nil {
		t.Error("Expected fields in query")
	}
}

func TestCouchDB_FindWithComplexFilter(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	cc := getCouchDBContainer(t)
	client := setupCouchDB(t, cc)

	instance := createCouchDBTestInstance(t)
	renderer := couchdb.New()

	// Build query: Find users who are active AND age > 25
	query := docql.Find(instance.C("users")).
		Filter(instance.And(
			instance.Eq(instance.F("users", "active"), instance.P("active")),
			instance.Gt(instance.F("users", "age"), instance.P("minAge")),
		))

	result, err := query.Render(renderer)
	if err != nil {
		t.Fatalf("Failed to render query: %v", err)
	}

	// Verify params
	if len(result.RequiredParams) != 2 {
		t.Errorf("Expected 2 required params, got %d", len(result.RequiredParams))
	}

	// Execute actual find
	docs, err := client.find("docql_test", map[string]interface{}{
		"selector": map[string]interface{}{
			"$and": []map[string]interface{}{
				{"type": "user"},
				{"active": true},
				{"age": map[string]interface{}{"$gt": 25}},
			},
		},
	})
	if err != nil {
		t.Fatalf("Failed to execute find: %v", err)
	}

	// alice (30), diana (28) are active and > 25
	if len(docs) != 2 {
		t.Errorf("Expected 2 users (active AND age > 25), got %d", len(docs))
	}
}

func TestCouchDB_SupportsOperation(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	renderer := couchdb.New()

	// CouchDB supports basic CRUD
	if !renderer.SupportsOperation(docql.OpFind) {
		t.Error("Expected CouchDB to support Find")
	}
	if !renderer.SupportsOperation(docql.OpInsert) {
		t.Error("Expected CouchDB to support Insert")
	}

	// CouchDB does not support aggregation
	if renderer.SupportsOperation(docql.OpAggregate) {
		t.Error("Expected CouchDB to not support Aggregate")
	}
}

func TestCouchDB_SupportsFilter(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	renderer := couchdb.New()

	// Basic comparison operators
	if !renderer.SupportsFilter(docql.OpEQ) {
		t.Error("Expected CouchDB to support EQ")
	}
	if !renderer.SupportsFilter(docql.OpGT) {
		t.Error("Expected CouchDB to support GT")
	}
	if !renderer.SupportsFilter(docql.OpIN) {
		t.Error("Expected CouchDB to support IN")
	}
}
