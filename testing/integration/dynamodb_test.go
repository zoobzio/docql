// Package integration provides integration tests for docql using DynamoDB Local.
package integration

import (
	"strings"
	"testing"

	"github.com/zoobzio/docql"
	"github.com/zoobzio/docql/pkg/dynamodb"
)

// Note: DynamoDB Local is available as a Docker container but requires
// table creation before use. These tests validate query rendering.
// For full integration testing, use DynamoDB Local with table setup.

func TestDynamoDB_Find(t *testing.T) {
	skipIfNoDynamoDB(t)
	instance := createTestInstance(t)

	result, err := docql.Find(instance.C("users")).
		Filter(instance.Eq(instance.F("users", "active"), instance.P("active"))).
		Limit(10).
		Render(dynamodb.New())

	if err != nil {
		t.Fatalf("Failed to render find: %v", err)
	}

	if result.JSON == "" {
		t.Error("Expected non-empty JSON result")
	}

	// DynamoDB uses FilterExpression
	if !strings.Contains(result.JSON, "FilterExpression") {
		t.Error("Expected 'FilterExpression' in result")
	}
}

func TestDynamoDB_FindWithComplexFilter(t *testing.T) {
	skipIfNoDynamoDB(t)
	instance := createTestInstance(t)

	result, err := docql.Find(instance.C("users")).
		Filter(instance.And(
			instance.Eq(instance.F("users", "active"), instance.P("active")),
			instance.Gte(instance.F("users", "age"), instance.P("min_age")),
		)).
		Render(dynamodb.New())

	if err != nil {
		t.Fatalf("Failed to render find with complex filter: %v", err)
	}

	// DynamoDB uses AND in expressions
	if !strings.Contains(result.JSON, "AND") {
		t.Error("Expected 'AND' in filter expression")
	}
}

func TestDynamoDB_FindOne(t *testing.T) {
	skipIfNoDynamoDB(t)
	instance := createTestInstance(t)

	result, err := docql.FindOne(instance.C("users")).
		Filter(instance.Eq(instance.F("users", "_id"), instance.P("id"))).
		Render(dynamodb.New())

	if err != nil {
		t.Fatalf("Failed to render findOne: %v", err)
	}

	if result.JSON == "" {
		t.Error("Expected non-empty JSON result")
	}
}

func TestDynamoDB_Insert(t *testing.T) {
	skipIfNoDynamoDB(t)
	instance := createTestInstance(t)

	result, err := docql.Insert(instance.C("users")).
		Doc(
			docql.Set(instance.F("users", "_id"), instance.P("id")),
			docql.Set(instance.F("users", "username"), instance.P("username")),
			docql.Set(instance.F("users", "email"), instance.P("email")),
		).
		Render(dynamodb.New())

	if err != nil {
		t.Fatalf("Failed to render insert: %v", err)
	}

	// DynamoDB uses PutItem with Item
	if !strings.Contains(result.JSON, "Item") {
		t.Error("Expected 'Item' in result")
	}
}

func TestDynamoDB_Update(t *testing.T) {
	skipIfNoDynamoDB(t)
	instance := createTestInstance(t)

	result, err := docql.Update(instance.C("users")).
		Filter(instance.Eq(instance.F("users", "_id"), instance.P("id"))).
		Set(instance.F("users", "email"), instance.P("new_email")).
		Render(dynamodb.New())

	if err != nil {
		t.Fatalf("Failed to render update: %v", err)
	}

	// DynamoDB uses UpdateExpression
	if !strings.Contains(result.JSON, "UpdateExpression") {
		t.Error("Expected 'UpdateExpression' in result")
	}
}

func TestDynamoDB_Delete(t *testing.T) {
	skipIfNoDynamoDB(t)
	instance := createTestInstance(t)

	result, err := docql.Delete(instance.C("users")).
		Filter(instance.Eq(instance.F("users", "_id"), instance.P("id"))).
		Render(dynamodb.New())

	if err != nil {
		t.Fatalf("Failed to render delete: %v", err)
	}

	// DynamoDB uses Key for DeleteItem
	if !strings.Contains(result.JSON, "Key") {
		t.Error("Expected 'Key' in result")
	}
}

func TestDynamoDB_RangeFilter(t *testing.T) {
	skipIfNoDynamoDB(t)
	instance := createTestInstance(t)

	minAge := instance.P("min_age")
	maxAge := instance.P("max_age")

	result, err := docql.Find(instance.C("users")).
		Filter(docql.Range(instance.F("users", "age"), &minAge, &maxAge)).
		Render(dynamodb.New())

	if err != nil {
		t.Fatalf("Failed to render find with range filter: %v", err)
	}

	// DynamoDB uses BETWEEN or comparison operators
	if !strings.Contains(result.JSON, "BETWEEN") && !strings.Contains(result.JSON, ">=") {
		t.Error("Expected range operators in filter")
	}
}

func TestDynamoDB_Projection(t *testing.T) {
	skipIfNoDynamoDB(t)
	instance := createTestInstance(t)

	result, err := docql.Find(instance.C("users")).
		Filter(instance.Eq(instance.F("users", "active"), instance.P("active"))).
		Select(instance.F("users", "username"), instance.F("users", "email")).
		Render(dynamodb.New())

	if err != nil {
		t.Fatalf("Failed to render find with projection: %v", err)
	}

	// DynamoDB uses ProjectionExpression
	if !strings.Contains(result.JSON, "ProjectionExpression") {
		t.Error("Expected 'ProjectionExpression' in result")
	}
}
