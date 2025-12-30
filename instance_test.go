package docql_test

import (
	"testing"

	"github.com/zoobzio/ddml"
	"github.com/zoobzio/docql"
	"github.com/zoobzio/docql/internal/types"
	"github.com/zoobzio/docql/pkg/mongodb"
)

func createTestInstance(t *testing.T) *docql.DOCQL {
	t.Helper()

	schema := ddml.NewSchema("test_db")

	users := ddml.NewCollection("users")
	users.AddField(ddml.NewField("_id", ddml.TypeObjectID))
	users.AddField(ddml.NewField("username", ddml.TypeString))
	users.AddField(ddml.NewField("email", ddml.TypeString))
	users.AddField(ddml.NewField("active", ddml.TypeBool))
	users.AddField(ddml.NewField("status", ddml.TypeString))
	schema.AddCollection(users)

	posts := ddml.NewCollection("posts")
	posts.AddField(ddml.NewField("_id", ddml.TypeObjectID))
	posts.AddField(ddml.NewField("userId", ddml.TypeObjectID))
	posts.AddField(ddml.NewField("title", ddml.TypeString))
	schema.AddCollection(posts)

	instance, err := docql.NewFromDDML(schema)
	if err != nil {
		t.Fatalf("Failed to create test instance: %v", err)
	}

	return instance
}

func TestNewFromDDML(t *testing.T) {
	schema := ddml.NewSchema("test")
	coll := ddml.NewCollection("users")
	coll.AddField(ddml.NewField("_id", ddml.TypeObjectID))
	schema.AddCollection(coll)

	instance, err := docql.NewFromDDML(schema)
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}
	if instance == nil {
		t.Fatal("Expected instance, got nil")
	}
}

func TestNewFromDDML_NilSchema(t *testing.T) {
	_, err := docql.NewFromDDML(nil)
	if err == nil {
		t.Fatal("Expected error for nil schema")
	}
}

func TestTryC_ValidCollection(t *testing.T) {
	instance := createTestInstance(t)

	coll, err := instance.TryC("users")
	if err != nil {
		t.Fatalf("Expected no error for valid collection, got: %v", err)
	}
	if coll.Name != "users" {
		t.Errorf("Expected collection name 'users', got '%s'", coll.Name)
	}
}

func TestTryC_InvalidCollection(t *testing.T) {
	instance := createTestInstance(t)

	_, err := instance.TryC("nonexistent")
	if err == nil {
		t.Fatal("Expected error for invalid collection")
	}
}

func TestC_ValidCollection(t *testing.T) {
	instance := createTestInstance(t)

	coll := instance.C("users")
	if coll.Name != "users" {
		t.Errorf("Expected collection name 'users', got '%s'", coll.Name)
	}
}

func TestC_InvalidCollection_Panics(t *testing.T) {
	instance := createTestInstance(t)

	defer func() {
		if r := recover(); r == nil {
			t.Error("Expected panic for invalid collection")
		}
	}()

	instance.C("nonexistent")
}

func TestTryF_ValidField(t *testing.T) {
	instance := createTestInstance(t)

	field, err := instance.TryF("users", "email")
	if err != nil {
		t.Fatalf("Expected no error for valid field, got: %v", err)
	}
	if field.Path != "email" {
		t.Errorf("Expected field path 'email', got '%s'", field.Path)
	}
	if field.Collection != "users" {
		t.Errorf("Expected collection 'users', got '%s'", field.Collection)
	}
}

func TestTryF_InvalidCollection(t *testing.T) {
	instance := createTestInstance(t)

	_, err := instance.TryF("nonexistent", "email")
	if err == nil {
		t.Fatal("Expected error for invalid collection")
	}
}

func TestTryF_InvalidField(t *testing.T) {
	instance := createTestInstance(t)

	_, err := instance.TryF("users", "nonexistent")
	if err == nil {
		t.Fatal("Expected error for invalid field")
	}
}

func TestF_ValidField(t *testing.T) {
	instance := createTestInstance(t)

	field := instance.F("users", "username")
	if field.Path != "username" {
		t.Errorf("Expected field path 'username', got '%s'", field.Path)
	}
}

func TestF_InvalidField_Panics(t *testing.T) {
	instance := createTestInstance(t)

	defer func() {
		if r := recover(); r == nil {
			t.Error("Expected panic for invalid field")
		}
	}()

	instance.F("users", "nonexistent")
}

func TestTryP_ValidParam(t *testing.T) {
	instance := createTestInstance(t)

	param, err := instance.TryP("user_id")
	if err != nil {
		t.Fatalf("Expected no error for valid param, got: %v", err)
	}
	if param.Name != "user_id" {
		t.Errorf("Expected param name 'user_id', got '%s'", param.Name)
	}
}

func TestTryP_InvalidParam(t *testing.T) {
	instance := createTestInstance(t)

	tests := []struct {
		name  string
		param string
	}{
		{"starts with number", "123abc"},
		{"contains space", "user id"},
		{"SQL injection attempt", "id; DROP TABLE"},
		{"contains comment", "id--"},
		{"empty string", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := instance.TryP(tt.param)
			if err == nil {
				t.Errorf("Expected error for param '%s'", tt.param)
			}
		})
	}
}

func TestP_ValidParam(t *testing.T) {
	instance := createTestInstance(t)

	param := instance.P("status")
	if param.Name != "status" {
		t.Errorf("Expected param name 'status', got '%s'", param.Name)
	}
}

func TestP_InvalidParam_Panics(t *testing.T) {
	instance := createTestInstance(t)

	defer func() {
		if r := recover(); r == nil {
			t.Error("Expected panic for invalid param")
		}
	}()

	instance.P("invalid param")
}

func TestEq_Filter(t *testing.T) {
	instance := createTestInstance(t)

	field := instance.F("users", "status")
	param := instance.P("status")

	cond := instance.Eq(field, param)
	if cond.Field.Path != "status" {
		t.Errorf("Expected field 'status', got '%s'", cond.Field.Path)
	}
	if cond.Operator != types.EQ {
		t.Errorf("Expected EQ operator, got %v", cond.Operator)
	}
}

func TestAnd_Filter(t *testing.T) {
	instance := createTestInstance(t)

	cond1 := instance.Eq(instance.F("users", "status"), instance.P("status"))
	cond2 := instance.Eq(instance.F("users", "active"), instance.P("active"))

	group := instance.And(cond1, cond2)
	if group.Logic != types.AND {
		t.Errorf("Expected AND logic, got %v", group.Logic)
	}
	if len(group.Conditions) != 2 {
		t.Errorf("Expected 2 conditions, got %d", len(group.Conditions))
	}
}

func TestOr_Filter(t *testing.T) {
	instance := createTestInstance(t)

	cond1 := instance.Eq(instance.F("users", "status"), instance.P("status"))
	cond2 := instance.Eq(instance.F("users", "active"), instance.P("active"))

	group := instance.Or(cond1, cond2)
	if group.Logic != types.OR {
		t.Errorf("Expected OR logic, got %v", group.Logic)
	}
}

func TestOperatorAccessors(t *testing.T) {
	instance := createTestInstance(t)

	tests := []struct {
		name     string
		accessor func() types.FilterOperator
		expected types.FilterOperator
	}{
		{"OpEQ", instance.OpEQ, types.EQ},
		{"OpNE", instance.OpNE, types.NE},
		{"OpGT", instance.OpGT, types.GT},
		{"OpGTE", instance.OpGTE, types.GTE},
		{"OpLT", instance.OpLT, types.LT},
		{"OpLTE", instance.OpLTE, types.LTE},
		{"OpIN", instance.OpIN, types.IN},
		{"OpNIN", instance.OpNIN, types.NotIn},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.accessor()
			if result != tt.expected {
				t.Errorf("Expected %v, got %v", tt.expected, result)
			}
		})
	}
}

func TestUpdateOperatorAccessors(t *testing.T) {
	instance := createTestInstance(t)

	tests := []struct {
		name     string
		accessor func() types.UpdateOperator
		expected types.UpdateOperator
	}{
		{"UpdateSet", instance.UpdateSet, types.Set},
		{"UpdateUnset", instance.UpdateUnset, types.Unset},
		{"UpdateInc", instance.UpdateInc, types.Inc},
		{"UpdatePush", instance.UpdatePush, types.Push},
		{"UpdatePull", instance.UpdatePull, types.Pull},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.accessor()
			if result != tt.expected {
				t.Errorf("Expected %v, got %v", tt.expected, result)
			}
		})
	}
}

func TestSortOrderAccessors(t *testing.T) {
	instance := createTestInstance(t)

	if instance.Asc() != types.Ascending {
		t.Error("Expected Ascending")
	}
	if instance.Desc() != types.Descending {
		t.Error("Expected Descending")
	}
}

func TestIntegration_BuildAndRender(t *testing.T) {
	instance := createTestInstance(t)

	query := docql.Find(instance.C("users")).
		Filter(instance.Eq(instance.F("users", "status"), instance.P("status"))).
		Limit(10)

	result, err := query.Render(mongodb.New())
	if err != nil {
		t.Fatalf("Render failed: %v", err)
	}

	if len(result.RequiredParams) != 1 {
		t.Errorf("Expected 1 param, got %d", len(result.RequiredParams))
	}
	if result.RequiredParams[0] != "status" {
		t.Errorf("Expected param 'status', got '%s'", result.RequiredParams[0])
	}
}

func TestSecurityValidation_InjectionAttempts(t *testing.T) {
	instance := createTestInstance(t)

	injectionAttempts := []string{
		"'; DROP TABLE users; --",
		"status; DELETE FROM users",
		"field /* comment */",
		"' OR '1'='1",
		"param\"; exec('cmd')",
		"status UNION SELECT",
	}

	for _, attempt := range injectionAttempts {
		t.Run(attempt, func(t *testing.T) {
			_, err := instance.TryP(attempt)
			if err == nil {
				t.Errorf("Expected error for injection attempt: %s", attempt)
			}
		})
	}
}
