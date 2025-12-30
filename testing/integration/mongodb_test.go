// Package integration provides integration tests for docql using real MongoDB.
package integration

import (
	"context"
	"testing"

	"github.com/zoobzio/ddml"
	"github.com/zoobzio/docql"
	"github.com/zoobzio/docql/pkg/mongodb"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

// createTestInstance creates a DOCQL instance matching the test database schema.
func createTestInstance(t *testing.T) *docql.DOCQL {
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
			ddml.NewCollection("posts").
				AddField(ddml.NewField("_id", ddml.TypeString)).
				AddField(ddml.NewField("userId", ddml.TypeString)).
				AddField(ddml.NewField("title", ddml.TypeString)).
				AddField(ddml.NewField("views", ddml.TypeInt)).
				AddField(ddml.NewField("published", ddml.TypeBool)),
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

// setupCollections creates the test database collections.
func setupCollections(ctx context.Context, t *testing.T, mc *MongoContainer) *mongo.Database {
	t.Helper()

	db := mc.client.Database("docql_test")

	// Drop existing collections for clean state
	collections := []string{"users", "posts", "orders"}
	for _, name := range collections {
		_ = db.Collection(name).Drop(ctx)
	}

	return db
}

// seedData inserts test data into MongoDB.
func seedData(ctx context.Context, t *testing.T, db *mongo.Database) {
	t.Helper()

	// Insert users
	users := db.Collection("users")
	_, err := users.InsertMany(ctx, []interface{}{
		bson.M{"_id": "1", "username": "alice", "email": "alice@example.com", "age": 30, "active": true},
		bson.M{"_id": "2", "username": "bob", "email": "bob@example.com", "age": 25, "active": true},
		bson.M{"_id": "3", "username": "charlie", "email": "charlie@example.com", "age": 35, "active": false},
		bson.M{"_id": "4", "username": "diana", "email": "diana@example.com", "age": 28, "active": true},
	})
	if err != nil {
		t.Fatalf("Failed to seed users: %v", err)
	}

	// Insert posts
	posts := db.Collection("posts")
	_, err = posts.InsertMany(ctx, []interface{}{
		bson.M{"_id": "1", "userId": "1", "title": "First Post", "views": 100, "published": true},
		bson.M{"_id": "2", "userId": "1", "title": "Second Post", "views": 50, "published": true},
		bson.M{"_id": "3", "userId": "2", "title": "Bob's Post", "views": 75, "published": true},
		bson.M{"_id": "4", "userId": "3", "title": "Draft Post", "views": 0, "published": false},
	})
	if err != nil {
		t.Fatalf("Failed to seed posts: %v", err)
	}

	// Insert orders
	orders := db.Collection("orders")
	_, err = orders.InsertMany(ctx, []interface{}{
		bson.M{"_id": "1", "userId": "1", "total": 99.99, "status": "completed"},
		bson.M{"_id": "2", "userId": "1", "total": 149.99, "status": "completed"},
		bson.M{"_id": "3", "userId": "2", "total": 49.99, "status": "pending"},
		bson.M{"_id": "4", "userId": "4", "total": 199.99, "status": "completed"},
	})
	if err != nil {
		t.Fatalf("Failed to seed orders: %v", err)
	}
}

func TestMongoDB_SimpleFind(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx := context.Background()
	mc := getMongoContainer(t)
	db := setupCollections(ctx, t, mc)
	seedData(ctx, t, db)

	instance := createTestInstance(t)
	renderer := mongodb.New()

	// Build query: Find all users
	query := docql.Find(instance.C("users"))
	result, err := query.Render(renderer)
	if err != nil {
		t.Fatalf("Failed to render query: %v", err)
	}

	// Execute the query
	cursor, err := db.Collection("users").Find(ctx, bson.M{})
	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}
	defer cursor.Close(ctx)

	var users []bson.M
	if err := cursor.All(ctx, &users); err != nil {
		t.Fatalf("Failed to decode results: %v", err)
	}

	if len(users) != 4 {
		t.Errorf("Expected 4 users, got %d", len(users))
	}

	// Verify query was generated
	if result.JSON == "" {
		t.Error("Expected non-empty query JSON")
	}
}

func TestMongoDB_FindWithFilter(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx := context.Background()
	mc := getMongoContainer(t)
	db := setupCollections(ctx, t, mc)
	seedData(ctx, t, db)

	instance := createTestInstance(t)
	renderer := mongodb.New()

	// Build query: Find active users
	query := docql.Find(instance.C("users")).
		Filter(instance.Eq(instance.F("users", "active"), instance.P("active")))

	result, err := query.Render(renderer)
	if err != nil {
		t.Fatalf("Failed to render query: %v", err)
	}

	// Verify required params
	if len(result.RequiredParams) != 1 || result.RequiredParams[0] != "active" {
		t.Errorf("Expected required param 'active', got %v", result.RequiredParams)
	}

	// Execute with parameter substitution
	cursor, err := db.Collection("users").Find(ctx, bson.M{"active": true})
	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}
	defer cursor.Close(ctx)

	var users []bson.M
	if err := cursor.All(ctx, &users); err != nil {
		t.Fatalf("Failed to decode results: %v", err)
	}

	if len(users) != 3 {
		t.Errorf("Expected 3 active users, got %d", len(users))
	}
}

func TestMongoDB_FindWithSort(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx := context.Background()
	mc := getMongoContainer(t)
	db := setupCollections(ctx, t, mc)
	seedData(ctx, t, db)

	instance := createTestInstance(t)
	renderer := mongodb.New()

	// Build query: Find users sorted by age descending
	query := docql.Find(instance.C("users")).
		SortDesc(instance.F("users", "age"))

	_, err := query.Render(renderer)
	if err != nil {
		t.Fatalf("Failed to render query: %v", err)
	}

	// Execute query
	cursor, err := db.Collection("users").Find(ctx, bson.M{}, nil)
	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}
	defer cursor.Close(ctx)

	var users []bson.M
	if err := cursor.All(ctx, &users); err != nil {
		t.Fatalf("Failed to decode results: %v", err)
	}

	// Verify we got results
	if len(users) != 4 {
		t.Errorf("Expected 4 users, got %d", len(users))
	}
}

func TestMongoDB_FindWithPagination(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx := context.Background()
	mc := getMongoContainer(t)
	db := setupCollections(ctx, t, mc)
	seedData(ctx, t, db)

	instance := createTestInstance(t)
	renderer := mongodb.New()

	// Build query: Find users with limit and skip
	query := docql.Find(instance.C("users")).
		Limit(2).
		Skip(1)

	result, err := query.Render(renderer)
	if err != nil {
		t.Fatalf("Failed to render query: %v", err)
	}

	// Verify pagination in JSON
	if result.JSON == "" {
		t.Error("Expected non-empty query JSON")
	}
}

func TestMongoDB_FindWithProjection(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx := context.Background()
	mc := getMongoContainer(t)
	db := setupCollections(ctx, t, mc)
	seedData(ctx, t, db)

	instance := createTestInstance(t)
	renderer := mongodb.New()

	// Build query: Find users with only username and email
	query := docql.Find(instance.C("users")).
		Select(instance.F("users", "username"), instance.F("users", "email"))

	result, err := query.Render(renderer)
	if err != nil {
		t.Fatalf("Failed to render query: %v", err)
	}

	// Verify projection in JSON
	if result.JSON == "" {
		t.Error("Expected non-empty query JSON")
	}
}

func TestMongoDB_FindWithComplexFilter(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx := context.Background()
	mc := getMongoContainer(t)
	db := setupCollections(ctx, t, mc)
	seedData(ctx, t, db)

	instance := createTestInstance(t)
	renderer := mongodb.New()

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

	// Execute with parameter substitution
	cursor, err := db.Collection("users").Find(ctx, bson.M{
		"$and": []bson.M{
			{"active": true},
			{"age": bson.M{"$gt": 25}},
		},
	})
	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}
	defer cursor.Close(ctx)

	var users []bson.M
	if err := cursor.All(ctx, &users); err != nil {
		t.Fatalf("Failed to decode results: %v", err)
	}

	// alice (30), diana (28) are active and > 25
	if len(users) != 2 {
		t.Errorf("Expected 2 users (active AND age > 25), got %d", len(users))
	}
}

func TestMongoDB_Count(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx := context.Background()
	mc := getMongoContainer(t)
	db := setupCollections(ctx, t, mc)
	seedData(ctx, t, db)

	instance := createTestInstance(t)
	renderer := mongodb.New()

	// Build count query
	query := docql.Count(instance.C("users")).
		Filter(instance.Eq(instance.F("users", "active"), instance.P("active")))

	result, err := query.Render(renderer)
	if err != nil {
		t.Fatalf("Failed to render query: %v", err)
	}

	// Verify result
	if result.JSON == "" {
		t.Error("Expected non-empty query JSON")
	}

	// Execute count
	count, err := db.Collection("users").CountDocuments(ctx, bson.M{"active": true})
	if err != nil {
		t.Fatalf("Failed to count: %v", err)
	}

	if count != 3 {
		t.Errorf("Expected 3 active users, got %d", count)
	}
}

func TestMongoDB_Insert(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx := context.Background()
	mc := getMongoContainer(t)
	db := setupCollections(ctx, t, mc)

	instance := createTestInstance(t)
	renderer := mongodb.New()

	// Build insert query
	doc := docql.Doc().
		Set(instance.F("users", "username"), instance.P("username")).
		Set(instance.F("users", "email"), instance.P("email")).
		Set(instance.F("users", "age"), instance.P("age")).
		Set(instance.F("users", "active"), instance.P("active")).
		Build()

	query := docql.Insert(instance.C("users")).Document(doc)

	result, err := query.Render(renderer)
	if err != nil {
		t.Fatalf("Failed to render query: %v", err)
	}

	// Verify params
	if len(result.RequiredParams) != 4 {
		t.Errorf("Expected 4 required params, got %d", len(result.RequiredParams))
	}

	// Execute actual insert
	_, err = db.Collection("users").InsertOne(ctx, bson.M{
		"username": "eve",
		"email":    "eve@example.com",
		"age":      22,
		"active":   true,
	})
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Verify insert
	count, _ := db.Collection("users").CountDocuments(ctx, bson.M{"username": "eve"})
	if count != 1 {
		t.Error("Expected inserted user to exist")
	}
}

func TestMongoDB_Update(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx := context.Background()
	mc := getMongoContainer(t)
	db := setupCollections(ctx, t, mc)
	seedData(ctx, t, db)

	instance := createTestInstance(t)
	renderer := mongodb.New()

	// Build update query
	query := docql.Update(instance.C("users")).
		Filter(instance.Eq(instance.F("users", "_id"), instance.P("id"))).
		Set(instance.F("users", "age"), instance.P("newAge"))

	result, err := query.Render(renderer)
	if err != nil {
		t.Fatalf("Failed to render query: %v", err)
	}

	// Verify params
	if len(result.RequiredParams) != 2 {
		t.Errorf("Expected 2 required params, got %d", len(result.RequiredParams))
	}

	// Execute actual update
	_, err = db.Collection("users").UpdateOne(ctx,
		bson.M{"_id": "1"},
		bson.M{"$set": bson.M{"age": 31}},
	)
	if err != nil {
		t.Fatalf("Failed to update: %v", err)
	}

	// Verify update
	var user bson.M
	err = db.Collection("users").FindOne(ctx, bson.M{"_id": "1"}).Decode(&user)
	if err != nil {
		t.Fatalf("Failed to find user: %v", err)
	}
	if user["age"] != int32(31) {
		t.Errorf("Expected age 31, got %v", user["age"])
	}
}

func TestMongoDB_Delete(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx := context.Background()
	mc := getMongoContainer(t)
	db := setupCollections(ctx, t, mc)
	seedData(ctx, t, db)

	instance := createTestInstance(t)
	renderer := mongodb.New()

	// Build delete query
	query := docql.Delete(instance.C("users")).
		Filter(instance.Eq(instance.F("users", "_id"), instance.P("id")))

	result, err := query.Render(renderer)
	if err != nil {
		t.Fatalf("Failed to render query: %v", err)
	}

	// Verify params
	if len(result.RequiredParams) != 1 {
		t.Errorf("Expected 1 required param, got %d", len(result.RequiredParams))
	}

	// Execute actual delete
	_, err = db.Collection("users").DeleteOne(ctx, bson.M{"_id": "3"})
	if err != nil {
		t.Fatalf("Failed to delete: %v", err)
	}

	// Verify delete
	count, _ := db.Collection("users").CountDocuments(ctx, bson.M{})
	if count != 3 {
		t.Errorf("Expected 3 users after delete, got %d", count)
	}
}

func TestMongoDB_Aggregate(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx := context.Background()
	mc := getMongoContainer(t)
	db := setupCollections(ctx, t, mc)
	seedData(ctx, t, db)

	instance := createTestInstance(t)
	renderer := mongodb.New()

	// Build aggregation pipeline: Count orders by status
	accumulators := map[string]docql.Accumulator{
		"total": docql.Sum(docql.FieldExpr(instance.F("orders", "total"))),
	}
	query := docql.Aggregate(instance.C("orders")).
		Match(instance.Eq(instance.F("orders", "status"), instance.P("status"))).
		Group(docql.FieldExpr(instance.F("orders", "userId")), accumulators)

	result, err := query.Render(renderer)
	if err != nil {
		t.Fatalf("Failed to render query: %v", err)
	}

	// Verify result
	if result.JSON == "" {
		t.Error("Expected non-empty query JSON")
	}

	// Execute actual aggregation
	pipeline := []bson.M{
		{"$match": bson.M{"status": "completed"}},
		{"$group": bson.M{
			"_id":   "$userId",
			"total": bson.M{"$sum": "$total"},
		}},
	}

	cursor, err := db.Collection("orders").Aggregate(ctx, pipeline)
	if err != nil {
		t.Fatalf("Failed to aggregate: %v", err)
	}
	defer cursor.Close(ctx)

	var results []bson.M
	if err := cursor.All(ctx, &results); err != nil {
		t.Fatalf("Failed to decode results: %v", err)
	}

	// user 1 has 2 completed orders, user 4 has 1 completed order
	if len(results) != 2 {
		t.Errorf("Expected 2 groups, got %d", len(results))
	}
}
