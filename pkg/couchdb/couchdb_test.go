package couchdb

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

	if query["selector"] == nil {
		t.Error("expected selector to be present")
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

	selector, ok := query["selector"].(map[string]interface{})
	if !ok {
		t.Fatal("expected selector to be a map")
	}
	if selector["status"] == nil {
		t.Error("expected status field in selector")
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

	sort, ok := query["sort"].([]interface{})
	if !ok {
		t.Fatal("expected sort to be an array")
	}
	if len(sort) != 1 {
		t.Errorf("expected 1 sort clause, got %d", len(sort))
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
	if query["skip"] != float64(20) {
		t.Errorf("expected skip 20, got %v", query["skip"])
	}
}

func TestRenderFind_WithProjection(t *testing.T) {
	ast := &types.DocumentAST{
		Operation: types.OpFind,
		Target:    types.Collection{Name: "users"},
		Projection: &types.Projection{
			Fields: []types.ProjectionField{
				{Field: types.Field{Path: "email"}, Include: true},
				{Field: types.Field{Path: "name"}, Include: true},
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

	fields, ok := query["fields"].([]interface{})
	if !ok {
		t.Fatal("expected fields to be an array")
	}
	if len(fields) != 2 {
		t.Errorf("expected 2 fields, got %d", len(fields))
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

	if query["operation"] != "insert" {
		t.Errorf("expected operation insert, got %v", query["operation"])
	}
	if query["doc"] == nil {
		t.Error("expected doc to be set")
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

	if query["operation"] != "update" {
		t.Errorf("expected operation update, got %v", query["operation"])
	}
	if query["updates"] == nil {
		t.Error("expected updates to be set")
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

	if query["operation"] != "delete" {
		t.Errorf("expected operation delete, got %v", query["operation"])
	}
}

func TestSupportsOperation(t *testing.T) {
	renderer := New()

	supported := []types.Operation{
		types.OpFind, types.OpFindOne, types.OpInsert, types.OpUpdate, types.OpDelete,
	}

	for _, op := range supported {
		if !renderer.SupportsOperation(op) {
			t.Errorf("expected CouchDB to support %s", op)
		}
	}

	unsupported := []types.Operation{
		types.OpAggregate, types.OpCount, types.OpDistinct,
	}

	for _, op := range unsupported {
		if renderer.SupportsOperation(op) {
			t.Errorf("expected CouchDB to not support %s", op)
		}
	}
}

func TestSupportsFilter(t *testing.T) {
	renderer := New()

	supported := []types.FilterOperator{
		types.EQ, types.NE, types.GT, types.GTE, types.LT, types.LTE, types.IN, types.NotIn, types.Regex, types.Exists,
	}

	for _, op := range supported {
		if !renderer.SupportsFilter(op) {
			t.Errorf("expected CouchDB to support filter %v", op)
		}
	}
}

func TestSupportsUpdate(t *testing.T) {
	renderer := New()

	if !renderer.SupportsUpdate(types.Set) {
		t.Error("expected CouchDB to support Set")
	}
	if renderer.SupportsUpdate(types.Inc) {
		t.Error("expected CouchDB to not support Inc")
	}
}

func TestSupportsPipelineStage(t *testing.T) {
	renderer := New()

	if renderer.SupportsPipelineStage("$match") {
		t.Error("CouchDB should not support pipeline stages")
	}
}

func TestRenderFind_WithRangeFilter(t *testing.T) {
	minVal := types.Param{Name: "minAge"}
	maxVal := types.Param{Name: "maxAge"}
	ast := &types.DocumentAST{
		Operation: types.OpFind,
		Target:    types.Collection{Name: "users"},
		FilterClause: types.RangeFilter{
			Field: types.Field{Path: "age"},
			Min:   &minVal,
			Max:   &maxVal,
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

func TestRenderFind_WithExistsFilter(t *testing.T) {
	ast := &types.DocumentAST{
		Operation: types.OpFind,
		Target:    types.Collection{Name: "users"},
		FilterClause: types.ExistsFilter{
			Field:  types.Field{Path: "email"},
			Exists: true,
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

	selector, ok := query["selector"].(map[string]interface{})
	if !ok {
		t.Fatal("expected selector to be a map")
	}
	if selector["email"] == nil {
		t.Error("expected email field in selector")
	}
}

func TestRenderFind_WithRegexFilter(t *testing.T) {
	ast := &types.DocumentAST{
		Operation: types.OpFind,
		Target:    types.Collection{Name: "users"},
		FilterClause: types.RegexFilter{
			Field:   types.Field{Path: "name"},
			Pattern: types.Param{Name: "pattern"},
		},
	}

	renderer := New()
	result, err := renderer.Render(ast)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(result.RequiredParams) != 1 {
		t.Errorf("expected 1 param, got %d", len(result.RequiredParams))
	}
}

func TestRenderFind_WithFilterGroup(t *testing.T) {
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

	var query map[string]interface{}
	if err := json.Unmarshal([]byte(result.JSON), &query); err != nil {
		t.Fatalf("failed to parse JSON: %v", err)
	}

	selector, ok := query["selector"].(map[string]interface{})
	if !ok {
		t.Fatal("expected selector to be a map")
	}
	if selector["$and"] == nil {
		t.Error("expected $and in selector")
	}
}
