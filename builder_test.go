package docql

import (
	"testing"

	"github.com/zoobzio/docql/internal/types"
)

func TestFind(t *testing.T) {
	coll := types.Collection{Name: "users"}
	builder := Find(coll)

	ast, err := builder.Build()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ast.Operation != types.OpFind {
		t.Errorf("expected OpFind, got %s", ast.Operation)
	}
	if ast.Target.Name != "users" {
		t.Errorf("expected users, got %s", ast.Target.Name)
	}
}

func TestFindOne(t *testing.T) {
	coll := types.Collection{Name: "users"}
	builder := FindOne(coll)

	ast, err := builder.Build()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ast.Operation != types.OpFindOne {
		t.Errorf("expected OpFindOne, got %s", ast.Operation)
	}
}

func TestFind_WithFilter(t *testing.T) {
	coll := types.Collection{Name: "users"}
	field := types.Field{Path: "status", Collection: "users"}
	param := types.Param{Name: "status"}

	ast, err := Find(coll).
		Filter(Eq(field, param)).
		Build()

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ast.FilterClause == nil {
		t.Fatal("expected FilterClause to be set")
	}
}

func TestFind_WithSort(t *testing.T) {
	coll := types.Collection{Name: "users"}
	field := types.Field{Path: "createdAt", Collection: "users"}

	ast, err := Find(coll).
		SortDesc(field).
		Build()

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(ast.SortClauses) != 1 {
		t.Fatalf("expected 1 sort clause, got %d", len(ast.SortClauses))
	}
	if ast.SortClauses[0].Order != types.Descending {
		t.Errorf("expected Descending, got %d", ast.SortClauses[0].Order)
	}
}

func TestFind_WithPagination(t *testing.T) {
	coll := types.Collection{Name: "users"}

	ast, err := Find(coll).
		Skip(10).
		Limit(20).
		Build()

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ast.Skip == nil || ast.Skip.Static == nil || *ast.Skip.Static != 10 {
		t.Error("expected Skip to be 10")
	}
	if ast.Limit == nil || ast.Limit.Static == nil || *ast.Limit.Static != 20 {
		t.Error("expected Limit to be 20")
	}
}

func TestFind_LimitExceedsMax(t *testing.T) {
	coll := types.Collection{Name: "users"}

	_, err := Find(coll).
		Limit(types.MaxLimit + 1).
		Build()

	if err == nil {
		t.Fatal("expected error for exceeding limit")
	}
}

func TestInsert(t *testing.T) {
	coll := types.Collection{Name: "users"}
	field := types.Field{Path: "email", Collection: "users"}
	param := types.Param{Name: "email"}

	doc := Doc().Set(field, param).Build()

	ast, err := Insert(coll).
		Document(doc).
		Build()

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ast.Operation != types.OpInsert {
		t.Errorf("expected OpInsert, got %s", ast.Operation)
	}
	if len(ast.Documents) != 1 {
		t.Errorf("expected 1 document, got %d", len(ast.Documents))
	}
}

func TestInsert_RequiresDocument(t *testing.T) {
	coll := types.Collection{Name: "users"}

	_, err := Insert(coll).Build()

	if err == nil {
		t.Fatal("expected error for missing document")
	}
}

func TestUpdate(t *testing.T) {
	coll := types.Collection{Name: "users"}
	field := types.Field{Path: "status", Collection: "users"}
	param := types.Param{Name: "newStatus"}

	ast, err := Update(coll).
		Set(field, param).
		Build()

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ast.Operation != types.OpUpdate {
		t.Errorf("expected OpUpdate, got %s", ast.Operation)
	}
	if len(ast.UpdateOps) != 1 {
		t.Errorf("expected 1 update op, got %d", len(ast.UpdateOps))
	}
}

func TestUpdate_RequiresUpdateOps(t *testing.T) {
	coll := types.Collection{Name: "users"}

	_, err := Update(coll).Build()

	if err == nil {
		t.Fatal("expected error for missing update operations")
	}
}

func TestUpdateMany_RequiresFilter(t *testing.T) {
	coll := types.Collection{Name: "users"}
	field := types.Field{Path: "status", Collection: "users"}
	param := types.Param{Name: "newStatus"}

	_, err := UpdateMany(coll).
		Set(field, param).
		Build()

	if err == nil {
		t.Fatal("expected error for missing filter in UpdateMany")
	}
}

func TestDelete(t *testing.T) {
	coll := types.Collection{Name: "users"}

	ast, err := Delete(coll).Build()

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ast.Operation != types.OpDelete {
		t.Errorf("expected OpDelete, got %s", ast.Operation)
	}
}

func TestDeleteMany_RequiresFilter(t *testing.T) {
	coll := types.Collection{Name: "users"}

	_, err := DeleteMany(coll).Build()

	if err == nil {
		t.Fatal("expected error for missing filter in DeleteMany")
	}
}

func TestAggregate(t *testing.T) {
	coll := types.Collection{Name: "orders"}
	statusField := types.Field{Path: "status", Collection: "orders"}
	param := types.Param{Name: "status"}

	ast, err := Aggregate(coll).
		Match(Eq(statusField, param)).
		Build()

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ast.Operation != types.OpAggregate {
		t.Errorf("expected OpAggregate, got %s", ast.Operation)
	}
	if len(ast.Pipeline) != 1 {
		t.Errorf("expected 1 pipeline stage, got %d", len(ast.Pipeline))
	}
}

func TestAggregate_RequiresPipeline(t *testing.T) {
	coll := types.Collection{Name: "orders"}

	_, err := Aggregate(coll).Build()

	if err == nil {
		t.Fatal("expected error for empty pipeline")
	}
}

func TestCount(t *testing.T) {
	coll := types.Collection{Name: "users"}

	ast, err := Count(coll).Build()

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ast.Operation != types.OpCount {
		t.Errorf("expected OpCount, got %s", ast.Operation)
	}
}

func TestDistinct(t *testing.T) {
	coll := types.Collection{Name: "users"}
	field := types.Field{Path: "status", Collection: "users"}

	ast, err := Distinct(coll, field).Build()

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ast.Operation != types.OpDistinct {
		t.Errorf("expected OpDistinct, got %s", ast.Operation)
	}
	if ast.DistinctField == nil || ast.DistinctField.Path != "status" {
		t.Error("expected DistinctField to be status")
	}
}

func TestOperationMismatch(t *testing.T) {
	coll := types.Collection{Name: "users"}
	field := types.Field{Path: "status", Collection: "users"}
	param := types.Param{Name: "value"}

	// Set() on Find
	_, err := Find(coll).Set(field, param).Build()
	if err == nil {
		t.Error("expected error for Set() on Find")
	}

	// Sort() on Insert
	_, err = Insert(coll).Sort(field, types.Ascending).Build()
	if err == nil {
		t.Error("expected error for Sort() on Insert")
	}

	// Match() on Find
	_, err = Find(coll).Match(Eq(field, param)).Build()
	if err == nil {
		t.Error("expected error for Match() on Find")
	}
}
