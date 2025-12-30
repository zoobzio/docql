package dynamodb

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

	if query["TableName"] != "users" {
		t.Errorf("expected TableName users, got %v", query["TableName"])
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

	var query map[string]interface{}
	if err := json.Unmarshal([]byte(result.JSON), &query); err != nil {
		t.Fatalf("failed to parse JSON: %v", err)
	}

	if query["FilterExpression"] == nil {
		t.Error("expected FilterExpression to be set")
	}
}

func TestRenderFind_WithPagination(t *testing.T) {
	limit := 10
	ast := &types.DocumentAST{
		Operation: types.OpFind,
		Target:    types.Collection{Name: "users"},
		Limit:     &types.PaginationValue{Static: &limit},
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

	if query["Limit"] != float64(10) {
		t.Errorf("expected Limit 10, got %v", query["Limit"])
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

	if query["TableName"] != "users" {
		t.Errorf("expected TableName users, got %v", query["TableName"])
	}
	if query["Item"] == nil {
		t.Error("expected Item to be set")
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

	if query["UpdateExpression"] == nil {
		t.Error("expected UpdateExpression to be set")
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

	if query["TableName"] != "users" {
		t.Errorf("expected TableName users, got %v", query["TableName"])
	}
}

func TestSupportsOperation(t *testing.T) {
	renderer := New()

	supported := []types.Operation{
		types.OpFind, types.OpFindOne, types.OpInsert, types.OpUpdate, types.OpDelete,
	}

	for _, op := range supported {
		if !renderer.SupportsOperation(op) {
			t.Errorf("expected DynamoDB to support %s", op)
		}
	}

	unsupported := []types.Operation{
		types.OpAggregate, types.OpCount, types.OpDistinct,
	}

	for _, op := range unsupported {
		if renderer.SupportsOperation(op) {
			t.Errorf("expected DynamoDB to not support %s", op)
		}
	}
}

func TestSupportsFilter(t *testing.T) {
	renderer := New()

	supported := []types.FilterOperator{
		types.EQ, types.NE, types.GT, types.GTE, types.LT, types.LTE, types.Exists,
	}

	for _, op := range supported {
		if !renderer.SupportsFilter(op) {
			t.Errorf("expected DynamoDB to support filter %v", op)
		}
	}
}

func TestSupportsPipelineStage(t *testing.T) {
	renderer := New()

	if renderer.SupportsPipelineStage("$match") {
		t.Error("DynamoDB should not support pipeline stages")
	}
}

func TestRenderAggregate_NotSupported(t *testing.T) {
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
	_, err := renderer.Render(ast)

	if err == nil {
		t.Error("expected error for unsupported Aggregate operation")
	}
}
