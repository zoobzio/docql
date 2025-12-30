package mongodb

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
	if result.RequiredParams[0] != "status" {
		t.Errorf("expected param status, got %s", result.RequiredParams[0])
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

	sort, ok := query["sort"].(map[string]interface{})
	if !ok {
		t.Fatal("expected sort to be a map")
	}
	if sort["createdAt"] != float64(-1) {
		t.Errorf("expected sort createdAt to be -1, got %v", sort["createdAt"])
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

	update, ok := query["update"].(map[string]interface{})
	if !ok {
		t.Fatal("expected update to be a map")
	}
	if _, ok := update["$set"]; !ok {
		t.Error("expected $set in update")
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

func TestRenderAggregate(t *testing.T) {
	ast := &types.DocumentAST{
		Operation: types.OpAggregate,
		Target:    types.Collection{Name: "orders"},
		Pipeline: []types.PipelineStage{
			types.MatchStage{
				Filter: types.FilterCondition{
					Field:    types.Field{Path: "status"},
					Operator: types.EQ,
					Value:    types.Param{Name: "status"},
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

	pipeline, ok := query["pipeline"].([]interface{})
	if !ok {
		t.Fatal("expected pipeline to be an array")
	}
	if len(pipeline) != 1 {
		t.Errorf("expected 1 pipeline stage, got %d", len(pipeline))
	}
}

func TestSupportsOperation(t *testing.T) {
	renderer := New()

	if !renderer.SupportsOperation(types.OpFind) {
		t.Error("expected MongoDB to support OpFind")
	}
	if !renderer.SupportsOperation(types.OpAggregate) {
		t.Error("expected MongoDB to support OpAggregate")
	}
}
