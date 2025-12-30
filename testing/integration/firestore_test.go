// Package integration provides integration tests for docql using Firestore Emulator.
package integration

import (
	"strings"
	"testing"

	"github.com/zoobzio/docql"
	"github.com/zoobzio/docql/pkg/firestore"
)

// Note: Firestore is available as an emulator but requires specific setup.
// These tests validate query rendering without requiring a live emulator.
// For full integration testing, run the Firestore emulator.

func TestFirestore_Find(t *testing.T) {
	skipIfNoFirestore(t)
	instance := createTestInstance(t)

	result, err := docql.Find(instance.C("users")).
		Filter(instance.Eq(instance.F("users", "active"), instance.P("active"))).
		Limit(10).
		Render(firestore.New())

	if err != nil {
		t.Fatalf("Failed to render find: %v", err)
	}

	if result.JSON == "" {
		t.Error("Expected non-empty JSON result")
	}

	// Firestore uses where clause
	if !strings.Contains(result.JSON, "where") {
		t.Error("Expected 'where' in result")
	}
}

func TestFirestore_FindWithComplexFilter(t *testing.T) {
	skipIfNoFirestore(t)
	instance := createTestInstance(t)

	// Firestore has limitations on OR queries and inequalities
	result, err := docql.Find(instance.C("users")).
		Filter(instance.And(
			instance.Eq(instance.F("users", "active"), instance.P("active")),
			instance.Gte(instance.F("users", "age"), instance.P("min_age")),
		)).
		Render(firestore.New())

	if err != nil {
		t.Fatalf("Failed to render find with complex filter: %v", err)
	}

	if result.JSON == "" {
		t.Error("Expected non-empty JSON result")
	}
}

func TestFirestore_FindOne(t *testing.T) {
	skipIfNoFirestore(t)
	instance := createTestInstance(t)

	result, err := docql.FindOne(instance.C("users")).
		Filter(instance.Eq(instance.F("users", "_id"), instance.P("id"))).
		Render(firestore.New())

	if err != nil {
		t.Fatalf("Failed to render findOne: %v", err)
	}

	if result.JSON == "" {
		t.Error("Expected non-empty JSON result")
	}
}

func TestFirestore_Insert(t *testing.T) {
	skipIfNoFirestore(t)
	instance := createTestInstance(t)

	result, err := docql.Insert(instance.C("users")).
		Doc(
			docql.Set(instance.F("users", "_id"), instance.P("id")),
			docql.Set(instance.F("users", "username"), instance.P("username")),
			docql.Set(instance.F("users", "email"), instance.P("email")),
		).
		Render(firestore.New())

	if err != nil {
		t.Fatalf("Failed to render insert: %v", err)
	}

	// Firestore uses set operations
	if result.JSON == "" {
		t.Error("Expected non-empty JSON result")
	}
}

func TestFirestore_Update(t *testing.T) {
	skipIfNoFirestore(t)
	instance := createTestInstance(t)

	result, err := docql.Update(instance.C("users")).
		Filter(instance.Eq(instance.F("users", "_id"), instance.P("id"))).
		Set(instance.F("users", "email"), instance.P("new_email")).
		Render(firestore.New())

	if err != nil {
		t.Fatalf("Failed to render update: %v", err)
	}

	// Firestore uses update operations
	if !strings.Contains(result.JSON, "update") {
		t.Error("Expected 'update' in result")
	}
}

func TestFirestore_Delete(t *testing.T) {
	skipIfNoFirestore(t)
	instance := createTestInstance(t)

	result, err := docql.Delete(instance.C("users")).
		Filter(instance.Eq(instance.F("users", "_id"), instance.P("id"))).
		Render(firestore.New())

	if err != nil {
		t.Fatalf("Failed to render delete: %v", err)
	}

	if result.JSON == "" {
		t.Error("Expected non-empty JSON result")
	}
}

func TestFirestore_Sort(t *testing.T) {
	skipIfNoFirestore(t)
	instance := createTestInstance(t)

	result, err := docql.Find(instance.C("users")).
		Filter(instance.Eq(instance.F("users", "active"), instance.P("active"))).
		SortDesc(instance.F("users", "age")).
		Limit(10).
		Render(firestore.New())

	if err != nil {
		t.Fatalf("Failed to render find with sort: %v", err)
	}

	// Firestore uses orderBy
	if !strings.Contains(result.JSON, "orderBy") {
		t.Error("Expected 'orderBy' in result")
	}
}

func TestFirestore_Pagination(t *testing.T) {
	skipIfNoFirestore(t)
	instance := createTestInstance(t)

	result, err := docql.Find(instance.C("users")).
		Filter(instance.Eq(instance.F("users", "active"), instance.P("active"))).
		Skip(10).
		Limit(10).
		Render(firestore.New())

	if err != nil {
		t.Fatalf("Failed to render find with pagination: %v", err)
	}

	// Firestore uses limit
	if !strings.Contains(result.JSON, "limit") {
		t.Error("Expected 'limit' in result")
	}
}

func TestFirestore_Projection(t *testing.T) {
	skipIfNoFirestore(t)
	instance := createTestInstance(t)

	result, err := docql.Find(instance.C("users")).
		Filter(instance.Eq(instance.F("users", "active"), instance.P("active"))).
		Select(instance.F("users", "username"), instance.F("users", "email")).
		Render(firestore.New())

	if err != nil {
		t.Fatalf("Failed to render find with projection: %v", err)
	}

	// Firestore uses select
	if !strings.Contains(result.JSON, "select") {
		t.Error("Expected 'select' in result")
	}
}

func TestFirestore_InFilter(t *testing.T) {
	skipIfNoFirestore(t)
	instance := createTestInstance(t)

	result, err := docql.Find(instance.C("users")).
		Filter(instance.In(instance.F("users", "username"), instance.P("usernames"))).
		Render(firestore.New())

	if err != nil {
		t.Fatalf("Failed to render find with IN filter: %v", err)
	}

	// Firestore uses in operator
	if !strings.Contains(result.JSON, "in") {
		t.Error("Expected 'in' operator in result")
	}
}
