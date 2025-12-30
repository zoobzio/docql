package types

import "testing"

func TestDocumentAST_Validate_FindOperation(t *testing.T) {
	ast := &DocumentAST{
		Operation: OpFind,
		Target:    Collection{Name: "users"},
	}

	err := ast.Validate()
	if err != nil {
		t.Errorf("Expected no error for valid Find, got: %v", err)
	}
}

func TestDocumentAST_Validate_FindOne(t *testing.T) {
	ast := &DocumentAST{
		Operation: OpFindOne,
		Target:    Collection{Name: "users"},
	}

	err := ast.Validate()
	if err != nil {
		t.Errorf("Expected no error for valid FindOne, got: %v", err)
	}
}

func TestDocumentAST_Validate_Insert_RequiresDocument(t *testing.T) {
	ast := &DocumentAST{
		Operation: OpInsert,
		Target:    Collection{Name: "users"},
	}

	err := ast.Validate()
	if err == nil {
		t.Error("Expected error for Insert without documents")
	}
}

func TestDocumentAST_Validate_Insert_WithDocument(t *testing.T) {
	ast := &DocumentAST{
		Operation: OpInsert,
		Target:    Collection{Name: "users"},
		Documents: []Document{
			{Fields: map[Field]Param{{Path: "email"}: {Name: "email"}}},
		},
	}

	err := ast.Validate()
	if err != nil {
		t.Errorf("Expected no error for valid Insert, got: %v", err)
	}
}

func TestDocumentAST_Validate_Update_RequiresUpdateOps(t *testing.T) {
	ast := &DocumentAST{
		Operation: OpUpdate,
		Target:    Collection{Name: "users"},
	}

	err := ast.Validate()
	if err == nil {
		t.Error("Expected error for Update without update operations")
	}
}

func TestDocumentAST_Validate_Update_WithOps(t *testing.T) {
	ast := &DocumentAST{
		Operation: OpUpdate,
		Target:    Collection{Name: "users"},
		UpdateOps: []UpdateOperation{
			{Operator: Set, Fields: map[Field]Param{{Path: "status"}: {Name: "status"}}},
		},
	}

	err := ast.Validate()
	if err != nil {
		t.Errorf("Expected no error for valid Update, got: %v", err)
	}
}

func TestDocumentAST_Validate_UpdateMany_RequiresFilter(t *testing.T) {
	ast := &DocumentAST{
		Operation: OpUpdateMany,
		Target:    Collection{Name: "users"},
		UpdateOps: []UpdateOperation{
			{Operator: Set, Fields: map[Field]Param{{Path: "status"}: {Name: "status"}}},
		},
	}

	err := ast.Validate()
	if err == nil {
		t.Error("Expected error for UpdateMany without filter")
	}
}

func TestDocumentAST_Validate_UpdateMany_WithFilter(t *testing.T) {
	ast := &DocumentAST{
		Operation:    OpUpdateMany,
		Target:       Collection{Name: "users"},
		FilterClause: FilterCondition{Field: Field{Path: "active"}, Operator: EQ, Value: Param{Name: "active"}},
		UpdateOps: []UpdateOperation{
			{Operator: Set, Fields: map[Field]Param{{Path: "status"}: {Name: "status"}}},
		},
	}

	err := ast.Validate()
	if err != nil {
		t.Errorf("Expected no error for valid UpdateMany, got: %v", err)
	}
}

func TestDocumentAST_Validate_Delete(t *testing.T) {
	ast := &DocumentAST{
		Operation: OpDelete,
		Target:    Collection{Name: "users"},
	}

	err := ast.Validate()
	if err != nil {
		t.Errorf("Expected no error for valid Delete, got: %v", err)
	}
}

func TestDocumentAST_Validate_DeleteMany_RequiresFilter(t *testing.T) {
	ast := &DocumentAST{
		Operation: OpDeleteMany,
		Target:    Collection{Name: "users"},
	}

	err := ast.Validate()
	if err == nil {
		t.Error("Expected error for DeleteMany without filter")
	}
}

func TestDocumentAST_Validate_DeleteMany_WithFilter(t *testing.T) {
	ast := &DocumentAST{
		Operation:    OpDeleteMany,
		Target:       Collection{Name: "users"},
		FilterClause: FilterCondition{Field: Field{Path: "active"}, Operator: EQ, Value: Param{Name: "active"}},
	}

	err := ast.Validate()
	if err != nil {
		t.Errorf("Expected no error for valid DeleteMany, got: %v", err)
	}
}

func TestDocumentAST_Validate_Aggregate_RequiresPipeline(t *testing.T) {
	ast := &DocumentAST{
		Operation: OpAggregate,
		Target:    Collection{Name: "orders"},
	}

	err := ast.Validate()
	if err == nil {
		t.Error("Expected error for Aggregate without pipeline")
	}
}

func TestDocumentAST_Validate_Aggregate_WithPipeline(t *testing.T) {
	ast := &DocumentAST{
		Operation: OpAggregate,
		Target:    Collection{Name: "orders"},
		Pipeline: []PipelineStage{
			MatchStage{Filter: FilterCondition{Field: Field{Path: "status"}, Operator: EQ, Value: Param{Name: "status"}}},
		},
	}

	err := ast.Validate()
	if err != nil {
		t.Errorf("Expected no error for valid Aggregate, got: %v", err)
	}
}

func TestDocumentAST_Validate_Count(t *testing.T) {
	ast := &DocumentAST{
		Operation: OpCount,
		Target:    Collection{Name: "users"},
	}

	err := ast.Validate()
	if err != nil {
		t.Errorf("Expected no error for valid Count, got: %v", err)
	}
}

func TestDocumentAST_Validate_Distinct_RequiresField(t *testing.T) {
	ast := &DocumentAST{
		Operation: OpDistinct,
		Target:    Collection{Name: "users"},
	}

	err := ast.Validate()
	if err == nil {
		t.Error("Expected error for Distinct without field")
	}
}

func TestDocumentAST_Validate_Distinct_WithField(t *testing.T) {
	field := Field{Path: "status"}
	ast := &DocumentAST{
		Operation:     OpDistinct,
		Target:        Collection{Name: "users"},
		DistinctField: &field,
	}

	err := ast.Validate()
	if err != nil {
		t.Errorf("Expected no error for valid Distinct, got: %v", err)
	}
}

func TestDocumentAST_Validate_LimitExceedsMax(t *testing.T) {
	limit := MaxLimit + 1
	ast := &DocumentAST{
		Operation: OpFind,
		Target:    Collection{Name: "users"},
		Limit:     &PaginationValue{Static: &limit},
	}

	err := ast.Validate()
	if err == nil {
		t.Error("Expected error for limit exceeding MaxLimit")
	}
}

func TestDocumentAST_Validate_LimitWithinMax(t *testing.T) {
	limit := 100
	ast := &DocumentAST{
		Operation: OpFind,
		Target:    Collection{Name: "users"},
		Limit:     &PaginationValue{Static: &limit},
	}

	err := ast.Validate()
	if err != nil {
		t.Errorf("Expected no error for valid limit, got: %v", err)
	}
}

func TestDocumentAST_Validate_EmptyTarget(t *testing.T) {
	ast := &DocumentAST{
		Operation: OpFind,
		Target:    Collection{Name: ""},
	}

	err := ast.Validate()
	if err == nil {
		t.Error("Expected error for empty target")
	}
}

func TestPipelineStage_StageName(t *testing.T) {
	tests := []struct {
		stage    PipelineStage
		expected string
	}{
		{MatchStage{}, "$match"},
		{ProjectStage{}, "$project"},
		{GroupStage{}, "$group"},
		{SortStage{}, "$sort"},
		{LimitStage{}, "$limit"},
		{SkipStage{}, "$skip"},
		{UnwindStage{}, "$unwind"},
		{LookupStage{}, "$lookup"},
		{AddFieldsStage{}, "$addFields"},
		{CountStage{}, "$count"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			if tt.stage.StageName() != tt.expected {
				t.Errorf("Expected stage name %s, got %s", tt.expected, tt.stage.StageName())
			}
		})
	}
}

func TestFilterCondition_IsFilterItem(t *testing.T) {
	cond := FilterCondition{
		Field:    Field{Path: "status"},
		Operator: EQ,
		Value:    Param{Name: "status"},
	}

	// Just verify it implements the interface
	var _ FilterItem = cond
}

func TestFilterGroup_IsFilterItem(t *testing.T) {
	group := FilterGroup{
		Logic:      AND,
		Conditions: []FilterItem{},
	}

	var _ FilterItem = group
}

func TestRangeFilter_IsFilterItem(t *testing.T) {
	filter := RangeFilter{
		Field: Field{Path: "age"},
	}

	var _ FilterItem = filter
}

func TestExpression_Implementations(t *testing.T) {
	// Verify interface implementations
	var _ Expression = FieldExpression{}
	var _ Expression = LiteralExpression{}
}

func TestSortOrder_Values(t *testing.T) {
	if Ascending != 1 {
		t.Errorf("Expected Ascending to be 1, got %d", Ascending)
	}
	if Descending != -1 {
		t.Errorf("Expected Descending to be -1, got %d", Descending)
	}
}

func TestMaxLimit_Value(t *testing.T) {
	if MaxLimit != 10000 {
		t.Errorf("Expected MaxLimit to be 10000, got %d", MaxLimit)
	}
}
