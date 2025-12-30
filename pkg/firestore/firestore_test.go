package firestore

import (
	"encoding/json"
	"testing"

	"github.com/zoobzio/docql/internal/types"
)

func TestRenderFind(t *testing.T) {
	ast := &types.DocumentAST{
		Operation: types.OpFind,
		Target:    types.Collection{Name: "users"},
	}

	renderer := New()
	result, err := renderer.Render(ast)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var query map[string]interface{}
	if err := json.Unmarshal([]byte(result.JSON), &query); err != nil {
		t.Fatalf("failed to parse JSON: %v", err)
	}

	if query["collection"] != "users" {
		t.Errorf("expected collection users, got %v", query["collection"])
	}
	if query["operation"] != "FIND" {
		t.Errorf("expected operation FIND, got %v", query["operation"])
	}
}

func TestRenderFind_WithFilter(t *testing.T) {
	ast := &types.DocumentAST{
		Operation: types.OpFind,
		Target:    types.Collection{Name: "users"},
		FilterClause: types.FilterCondition{
			Field:    types.Field{Path: "status", Collection: "users"},
			Operator: types.EQ,
			Value:    types.Param{Name: "status"},
		},
	}

	renderer := New()
	result, err := renderer.Render(ast)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(result.RequiredParams) != 1 {
		t.Errorf("expected 1 required param, got %d", len(result.RequiredParams))
	}

	var query map[string]interface{}
	if err := json.Unmarshal([]byte(result.JSON), &query); err != nil {
		t.Fatalf("failed to parse JSON: %v", err)
	}

	if query["where"] == nil {
		t.Error("expected where clause to be set")
	}
}

func TestRenderFind_WithSort(t *testing.T) {
	ast := &types.DocumentAST{
		Operation: types.OpFind,
		Target:    types.Collection{Name: "users"},
		SortClauses: []types.SortClause{
			{Field: types.Field{Path: "createdAt"}, Order: types.Descending},
		},
	}

	renderer := New()
	result, err := renderer.Render(ast)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var query map[string]interface{}
	if err := json.Unmarshal([]byte(result.JSON), &query); err != nil {
		t.Fatalf("failed to parse JSON: %v", err)
	}

	orderBy, ok := query["orderBy"].([]interface{})
	if !ok {
		t.Fatal("expected orderBy to be an array")
	}
	if len(orderBy) != 1 {
		t.Errorf("expected 1 orderBy clause, got %d", len(orderBy))
	}
}

func TestRenderFind_WithPagination(t *testing.T) {
	limit := 10
	skip := 20
	ast := &types.DocumentAST{
		Operation: types.OpFind,
		Target:    types.Collection{Name: "users"},
		Limit:     &types.PaginationValue{Static: &limit},
		Skip:      &types.PaginationValue{Static: &skip},
	}

	renderer := New()
	result, err := renderer.Render(ast)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var query map[string]interface{}
	if err := json.Unmarshal([]byte(result.JSON), &query); err != nil {
		t.Fatalf("failed to parse JSON: %v", err)
	}

	if query["limit"] != float64(10) {
		t.Errorf("expected limit 10, got %v", query["limit"])
	}
	if query["offset"] != float64(20) {
		t.Errorf("expected offset 20, got %v", query["offset"])
	}
}

func TestRenderInsert(t *testing.T) {
	ast := &types.DocumentAST{
		Operation: types.OpInsert,
		Target:    types.Collection{Name: "users"},
		Documents: []types.Document{
			{
				Fields: map[types.Field]types.Param{
					{Path: "email"}: {Name: "email"},
					{Path: "name"}:  {Name: "name"},
				},
			},
		},
	}

	renderer := New()
	result, err := renderer.Render(ast)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(result.RequiredParams) != 2 {
		t.Errorf("expected 2 required params, got %d", len(result.RequiredParams))
	}

	var query map[string]interface{}
	if err := json.Unmarshal([]byte(result.JSON), &query); err != nil {
		t.Fatalf("failed to parse JSON: %v", err)
	}

	if query["data"] == nil {
		t.Error("expected data to be set")
	}
}

func TestRenderUpdate(t *testing.T) {
	ast := &types.DocumentAST{
		Operation: types.OpUpdate,
		Target:    types.Collection{Name: "users"},
		UpdateOps: []types.UpdateOperation{
			{
				Operator: types.Set,
				Fields: map[types.Field]types.Param{
					{Path: "status"}: {Name: "newStatus"},
				},
			},
		},
	}

	renderer := New()
	result, err := renderer.Render(ast)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var query map[string]interface{}
	if err := json.Unmarshal([]byte(result.JSON), &query); err != nil {
		t.Fatalf("failed to parse JSON: %v", err)
	}

	if query["data"] == nil {
		t.Error("expected data to be set")
	}
}

func TestRenderUpdate_WithUnset(t *testing.T) {
	ast := &types.DocumentAST{
		Operation: types.OpUpdate,
		Target:    types.Collection{Name: "users"},
		UpdateOps: []types.UpdateOperation{
			{
				Operator: types.Unset,
				Fields: map[types.Field]types.Param{
					{Path: "oldField"}: {},
				},
			},
		},
	}

	renderer := New()
	result, err := renderer.Render(ast)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var query map[string]interface{}
	if err := json.Unmarshal([]byte(result.JSON), &query); err != nil {
		t.Fatalf("failed to parse JSON: %v", err)
	}

	data, ok := query["data"].(map[string]interface{})
	if !ok {
		t.Fatal("expected data to be a map")
	}
	if data["oldField"] != "FieldValue.delete()" {
		t.Errorf("expected FieldValue.delete(), got %v", data["oldField"])
	}
}

func TestRenderUpdate_UnsupportedOperator(t *testing.T) {
	ast := &types.DocumentAST{
		Operation: types.OpUpdate,
		Target:    types.Collection{Name: "users"},
		UpdateOps: []types.UpdateOperation{
			{
				Operator: types.Inc,
				Fields: map[types.Field]types.Param{
					{Path: "count"}: {Name: "increment"},
				},
			},
		},
	}

	renderer := New()
	_, err := renderer.Render(ast)

	if err == nil {
		t.Error("expected error for unsupported update operator")
	}
}

func TestRenderDelete(t *testing.T) {
	ast := &types.DocumentAST{
		Operation: types.OpDelete,
		Target:    types.Collection{Name: "users"},
	}

	renderer := New()
	result, err := renderer.Render(ast)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var query map[string]interface{}
	if err := json.Unmarshal([]byte(result.JSON), &query); err != nil {
		t.Fatalf("failed to parse JSON: %v", err)
	}

	if query["operation"] != "DELETE" {
		t.Errorf("expected operation DELETE, got %v", query["operation"])
	}
}

func TestSupportsOperation(t *testing.T) {
	renderer := New()

	supported := []types.Operation{
		types.OpFind, types.OpFindOne, types.OpInsert, types.OpUpdate, types.OpDelete,
	}

	for _, op := range supported {
		if !renderer.SupportsOperation(op) {
			t.Errorf("expected Firestore to support %s", op)
		}
	}

	unsupported := []types.Operation{
		types.OpAggregate, types.OpCount, types.OpDistinct,
	}

	for _, op := range unsupported {
		if renderer.SupportsOperation(op) {
			t.Errorf("expected Firestore to not support %s", op)
		}
	}
}

func TestSupportsFilter(t *testing.T) {
	renderer := New()

	supported := []types.FilterOperator{
		types.EQ, types.NE, types.GT, types.GTE, types.LT, types.LTE, types.IN, types.NotIn, types.All,
	}

	for _, op := range supported {
		if !renderer.SupportsFilter(op) {
			t.Errorf("expected Firestore to support filter %v", op)
		}
	}

	unsupported := []types.FilterOperator{
		types.Regex, types.Text,
	}

	for _, op := range unsupported {
		if renderer.SupportsFilter(op) {
			t.Errorf("expected Firestore to not support filter %v", op)
		}
	}
}

func TestSupportsUpdate(t *testing.T) {
	renderer := New()

	if !renderer.SupportsUpdate(types.Set) {
		t.Error("expected Firestore to support Set")
	}
	if !renderer.SupportsUpdate(types.Unset) {
		t.Error("expected Firestore to support Unset")
	}
	if renderer.SupportsUpdate(types.Inc) {
		t.Error("expected Firestore to not support Inc")
	}
}

func TestSupportsPipelineStage(t *testing.T) {
	renderer := New()

	if renderer.SupportsPipelineStage("$match") {
		t.Error("Firestore should not support pipeline stages")
	}
}

func TestRenderFind_WithORFilter_NotSupported(t *testing.T) {
	ast := &types.DocumentAST{
		Operation: types.OpFind,
		Target:    types.Collection{Name: "users"},
		FilterClause: types.FilterGroup{
			Logic: types.OR,
			Conditions: []types.FilterItem{
				types.FilterCondition{Field: types.Field{Path: "a"}, Operator: types.EQ, Value: types.Param{Name: "a"}},
				types.FilterCondition{Field: types.Field{Path: "b"}, Operator: types.EQ, Value: types.Param{Name: "b"}},
			},
		},
	}

	renderer := New()
	_, err := renderer.Render(ast)

	if err == nil {
		t.Error("expected error for OR logic in Firestore")
	}
}

func TestRenderFind_WithANDFilter(t *testing.T) {
	ast := &types.DocumentAST{
		Operation: types.OpFind,
		Target:    types.Collection{Name: "users"},
		FilterClause: types.FilterGroup{
			Logic: types.AND,
			Conditions: []types.FilterItem{
				types.FilterCondition{Field: types.Field{Path: "a"}, Operator: types.EQ, Value: types.Param{Name: "a"}},
				types.FilterCondition{Field: types.Field{Path: "b"}, Operator: types.EQ, Value: types.Param{Name: "b"}},
			},
		},
	}

	renderer := New()
	result, err := renderer.Render(ast)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(result.RequiredParams) != 2 {
		t.Errorf("expected 2 params, got %d", len(result.RequiredParams))
	}
}
