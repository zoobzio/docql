package types

import "fmt"

// DocumentAST represents the abstract syntax tree for document database queries.
type DocumentAST struct {
	// Core operation.
	Operation Operation
	Target    Collection

	// Filter clause.
	FilterClause FilterItem

	// Projection/field selection.
	Projection *Projection

	// Sorting.
	SortClauses []SortClause

	// Pagination.
	Skip  *PaginationValue
	Limit *PaginationValue

	// Insert-specific.
	Documents []Document

	// Update-specific.
	UpdateOps []UpdateOperation
	Upsert    bool

	// Aggregation pipeline.
	Pipeline []PipelineStage

	// Distinct field (for OpDistinct).
	DistinctField *Field
}

// Validate validates the DocumentAST.
func (ast *DocumentAST) Validate() error {
	if ast.Target.Name == "" {
		return fmt.Errorf("target collection is required")
	}

	switch ast.Operation {
	case OpFind, OpFindOne:
		return ast.validateFind()
	case OpInsert:
		return ast.validateInsert()
	case OpInsertMany:
		return ast.validateInsertMany()
	case OpUpdate:
		return ast.validateUpdate()
	case OpUpdateMany:
		return ast.validateUpdateMany()
	case OpDelete:
		return ast.validateDelete()
	case OpDeleteMany:
		return ast.validateDeleteMany()
	case OpAggregate:
		return ast.validateAggregate()
	case OpCount:
		return ast.validateCount()
	case OpDistinct:
		return ast.validateDistinct()
	default:
		return fmt.Errorf("unsupported operation: %s", ast.Operation)
	}
}

func (ast *DocumentAST) validateFind() error {
	if ast.Limit != nil && ast.Limit.Static != nil && *ast.Limit.Static > MaxLimit {
		return fmt.Errorf("limit exceeds maximum: %d > %d", *ast.Limit.Static, MaxLimit)
	}
	if ast.Projection != nil && len(ast.Projection.Fields) > MaxProjectionFields {
		return fmt.Errorf("projection fields exceed maximum: %d > %d",
			len(ast.Projection.Fields), MaxProjectionFields)
	}
	if len(ast.SortClauses) > MaxSortFields {
		return fmt.Errorf("sort fields exceed maximum: %d > %d",
			len(ast.SortClauses), MaxSortFields)
	}
	if ast.FilterClause != nil {
		if err := validateFilterDepth(ast.FilterClause, 0); err != nil {
			return err
		}
	}
	return nil
}

func (ast *DocumentAST) validateInsert() error {
	if len(ast.Documents) != 1 {
		return fmt.Errorf("INSERT requires exactly one document")
	}
	return nil
}

func (ast *DocumentAST) validateInsertMany() error {
	if len(ast.Documents) == 0 {
		return fmt.Errorf("INSERT_MANY requires at least one document")
	}
	if len(ast.Documents) > MaxBatchSize {
		return fmt.Errorf("batch size exceeds maximum: %d > %d",
			len(ast.Documents), MaxBatchSize)
	}
	return nil
}

func (ast *DocumentAST) validateUpdate() error {
	if len(ast.UpdateOps) == 0 {
		return fmt.Errorf("UPDATE requires at least one update operation")
	}
	return nil
}

func (ast *DocumentAST) validateUpdateMany() error {
	if len(ast.UpdateOps) == 0 {
		return fmt.Errorf("UPDATE_MANY requires at least one update operation")
	}
	if ast.FilterClause == nil {
		return fmt.Errorf("UPDATE_MANY requires a filter for safety")
	}
	return nil
}

func (ast *DocumentAST) validateDelete() error {
	return nil
}

func (ast *DocumentAST) validateDeleteMany() error {
	if ast.FilterClause == nil {
		return fmt.Errorf("DELETE_MANY requires a filter for safety")
	}
	return nil
}

func (ast *DocumentAST) validateAggregate() error {
	if len(ast.Pipeline) == 0 {
		return fmt.Errorf("AGGREGATE requires at least one pipeline stage")
	}
	if len(ast.Pipeline) > MaxPipelineStages {
		return fmt.Errorf("pipeline stages exceed maximum: %d > %d",
			len(ast.Pipeline), MaxPipelineStages)
	}
	return nil
}

func (ast *DocumentAST) validateCount() error {
	return nil
}

func (ast *DocumentAST) validateDistinct() error {
	if ast.DistinctField == nil {
		return fmt.Errorf("DISTINCT requires a field")
	}
	return nil
}

func validateFilterDepth(f FilterItem, depth int) error {
	if depth > MaxFilterDepth {
		return fmt.Errorf("filter nesting exceeds maximum depth: %d > %d", depth, MaxFilterDepth)
	}

	if group, ok := f.(FilterGroup); ok {
		for _, c := range group.Conditions {
			if err := validateFilterDepth(c, depth+1); err != nil {
				return err
			}
		}
	}

	if em, ok := f.(ElemMatchFilter); ok {
		for _, c := range em.Conditions {
			if err := validateFilterDepth(c, depth+1); err != nil {
				return err
			}
		}
	}

	return nil
}
