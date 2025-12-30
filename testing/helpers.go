// Package testing provides test utilities for docql.
package testing

import (
	"encoding/json"
	"testing"

	"github.com/zoobzio/ddml"
	"github.com/zoobzio/docql"
)

// TestInstance creates a fully-featured DOCQL instance for testing.
// Includes users, posts, orders, and products collections.
func TestInstance(t *testing.T) *docql.DOCQL {
	t.Helper()

	schema := ddml.NewSchema("test")

	// Users collection
	users := ddml.NewCollection("users")
	users.AddField(ddml.NewField("_id", ddml.TypeObjectID))
	users.AddField(ddml.NewField("username", ddml.TypeString))
	users.AddField(ddml.NewField("email", ddml.TypeString))
	users.AddField(ddml.NewField("age", ddml.TypeInt))
	users.AddField(ddml.NewField("active", ddml.TypeBool))
	users.AddField(ddml.NewField("createdAt", ddml.TypeDate))
	users.AddField(ddml.NewField("tags", ddml.TypeArray))
	users.AddField(ddml.NewField("status", ddml.TypeString))

	// Nested address field
	address := ddml.NewField("address", ddml.TypeObject)
	address.AddField(ddml.NewField("street", ddml.TypeString))
	address.AddField(ddml.NewField("city", ddml.TypeString))
	address.AddField(ddml.NewField("zip", ddml.TypeString))
	users.AddField(address)

	schema.AddCollection(users)

	// Posts collection
	posts := ddml.NewCollection("posts")
	posts.AddField(ddml.NewField("_id", ddml.TypeObjectID))
	posts.AddField(ddml.NewField("userId", ddml.TypeObjectID))
	posts.AddField(ddml.NewField("title", ddml.TypeString))
	posts.AddField(ddml.NewField("body", ddml.TypeString))
	posts.AddField(ddml.NewField("published", ddml.TypeBool))
	posts.AddField(ddml.NewField("views", ddml.TypeInt))
	posts.AddField(ddml.NewField("createdAt", ddml.TypeDate))
	schema.AddCollection(posts)

	// Orders collection
	orders := ddml.NewCollection("orders")
	orders.AddField(ddml.NewField("_id", ddml.TypeObjectID))
	orders.AddField(ddml.NewField("userId", ddml.TypeObjectID))
	orders.AddField(ddml.NewField("total", ddml.TypeFloat))
	orders.AddField(ddml.NewField("status", ddml.TypeString))
	orders.AddField(ddml.NewField("createdAt", ddml.TypeDate))
	schema.AddCollection(orders)

	// Products collection
	products := ddml.NewCollection("products")
	products.AddField(ddml.NewField("_id", ddml.TypeObjectID))
	products.AddField(ddml.NewField("name", ddml.TypeString))
	products.AddField(ddml.NewField("price", ddml.TypeFloat))
	products.AddField(ddml.NewField("category", ddml.TypeString))
	products.AddField(ddml.NewField("stock", ddml.TypeInt))
	schema.AddCollection(products)

	instance, err := docql.NewFromDDML(schema)
	if err != nil {
		t.Fatalf("Failed to create test instance: %v", err)
	}
	return instance
}

// AssertJSON compares expected and actual JSON strings.
func AssertJSON(t *testing.T, expected, actual string) {
	t.Helper()

	var expectedMap, actualMap map[string]interface{}

	if err := json.Unmarshal([]byte(expected), &expectedMap); err != nil {
		t.Fatalf("Failed to parse expected JSON: %v", err)
	}
	if err := json.Unmarshal([]byte(actual), &actualMap); err != nil {
		t.Fatalf("Failed to parse actual JSON: %v\nActual: %s", err, actual)
	}

	expectedBytes, _ := json.Marshal(expectedMap)
	actualBytes, _ := json.Marshal(actualMap)

	if string(expectedBytes) != string(actualBytes) {
		t.Errorf("JSON mismatch:\nExpected: %s\nActual:   %s", expected, actual)
	}
}

// AssertJSONContains checks that actual JSON contains all keys from expected.
func AssertJSONContains(t *testing.T, actual string, key string, expectedValue interface{}) {
	t.Helper()

	var actualMap map[string]interface{}
	if err := json.Unmarshal([]byte(actual), &actualMap); err != nil {
		t.Fatalf("Failed to parse actual JSON: %v", err)
	}

	actualValue, ok := actualMap[key]
	if !ok {
		t.Errorf("Expected key %q not found in JSON: %s", key, actual)
		return
	}

	expectedJSON, _ := json.Marshal(expectedValue)
	actualJSON, _ := json.Marshal(actualValue)

	if string(expectedJSON) != string(actualJSON) {
		t.Errorf("Value mismatch for key %q:\nExpected: %s\nActual:   %s", key, expectedJSON, actualJSON)
	}
}

// AssertParams checks that the required params match expected values.
func AssertParams(t *testing.T, expected, actual []string) {
	t.Helper()
	if len(expected) != len(actual) {
		t.Errorf("Param count mismatch: expected %d, got %d\nExpected: %v\nActual: %v",
			len(expected), len(actual), expected, actual)
		return
	}

	expectedMap := make(map[string]bool)
	for _, p := range expected {
		expectedMap[p] = true
	}

	for _, p := range actual {
		if !expectedMap[p] {
			t.Errorf("Unexpected param: %s\nExpected: %v\nActual: %v", p, expected, actual)
		}
	}
}

// AssertContainsParam checks that a specific param is in the list.
func AssertContainsParam(t *testing.T, params []string, param string) {
	t.Helper()
	for _, p := range params {
		if p == param {
			return
		}
	}
	t.Errorf("Expected param %q not found in %v", param, params)
}

// AssertNoError fails the test if err is not nil.
func AssertNoError(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
}

// AssertError fails the test if err is nil.
func AssertError(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("Expected error but got nil")
	}
}

// AssertErrorContains checks that error message contains substring.
func AssertErrorContains(t *testing.T, err error, substr string) {
	t.Helper()
	if err == nil {
		t.Fatalf("Expected error containing %q but got nil", substr)
	}
	if !containsString(err.Error(), substr) {
		t.Errorf("Expected error containing %q, got: %v", substr, err)
	}
}

// AssertPanics verifies that a function panics.
func AssertPanics(t *testing.T, fn func()) {
	t.Helper()
	defer func() {
		if r := recover(); r == nil {
			t.Error("Expected panic but function completed normally")
		}
	}()
	fn()
}

// AssertPanicsWithMessage verifies that a function panics with a specific message.
func AssertPanicsWithMessage(t *testing.T, fn func(), substr string) {
	t.Helper()
	defer func() {
		r := recover()
		if r == nil {
			t.Errorf("Expected panic containing %q but function completed normally", substr)
			return
		}
		var msg string
		switch v := r.(type) {
		case error:
			msg = v.Error()
		case string:
			msg = v
		default:
			t.Errorf("Panic value is not string or error: %T", r)
			return
		}
		if !containsString(msg, substr) {
			t.Errorf("Expected panic containing %q, got: %s", substr, msg)
		}
	}()
	fn()
}

func containsString(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || substr == "" ||
		(s != "" && substr != "" && findSubstring(s, substr)))
}

func findSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
