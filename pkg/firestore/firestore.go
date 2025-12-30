// Package firestore provides a Firestore renderer for DOCQL.
package firestore

import (
	"encoding/json"
	"fmt"

	"github.com/zoobzio/docql/internal/types"
)

// Renderer renders DocumentAST to Firestore query format.
type Renderer struct{}

// New creates a new Firestore renderer.
func New() *Renderer {
	return &Renderer{}
}

// Render converts a DocumentAST to Firestore query format.
func (r *Renderer) Render(ast *types.DocumentAST) (*types.QueryResult, error) {
	if err := ast.Validate(); err != nil {
		return nil, fmt.Errorf("invalid AST: %w", err)
	}

	if !r.SupportsOperation(ast.Operation) {
		return nil, fmt.Errorf("firestore does not support operation: %s", ast.Operation)
	}

	var params []string

	switch ast.Operation {
	case types.OpFind, types.OpFindOne:
		return r.renderQuery(ast, &params)
	case types.OpInsert:
		return r.renderAdd(ast, &params)
	case types.OpUpdate:
		return r.renderUpdate(ast, &params)
	case types.OpDelete:
		return r.renderDelete(ast, &params)
	default:
		return nil, fmt.Errorf("unsupported operation: %s", ast.Operation)
	}
}

func (r *Renderer) renderQuery(ast *types.DocumentAST, params *[]string) (*types.QueryResult, error) {
	query := make(map[string]interface{})
	query["collection"] = ast.Target.Name
	query["operation"] = string(ast.Operation)

	if ast.FilterClause != nil {
		wheres, err := r.buildWheres(ast.FilterClause, params)
		if err != nil {
			return nil, err
		}
		query["where"] = wheres
	}

	if len(ast.SortClauses) > 0 {
		orderBy := make([]map[string]interface{}, len(ast.SortClauses))
		for i, s := range ast.SortClauses {
			direction := "asc"
			if s.Order == types.Descending {
				direction = "desc"
			}
			orderBy[i] = map[string]interface{}{
				"field":     s.Field.Path,
				"direction": direction,
			}
		}
		query["orderBy"] = orderBy
	}

	if ast.Limit != nil {
		if ast.Limit.Static != nil {
			query["limit"] = *ast.Limit.Static
		} else if ast.Limit.Param != nil {
			*params = append(*params, ast.Limit.Param.Name)
			query["limit"] = fmt.Sprintf(":%s", ast.Limit.Param.Name)
		}
	}

	if ast.Skip != nil {
		if ast.Skip.Static != nil {
			query["offset"] = *ast.Skip.Static
		} else if ast.Skip.Param != nil {
			*params = append(*params, ast.Skip.Param.Name)
			query["offset"] = fmt.Sprintf(":%s", ast.Skip.Param.Name)
		}
	}

	if ast.Projection != nil {
		fields := make([]string, 0)
		for _, f := range ast.Projection.Fields {
			if f.Include {
				fields = append(fields, f.Field.Path)
			}
		}
		if len(fields) > 0 {
			query["select"] = fields
		}
	}

	return toResult(query, *params)
}

func (r *Renderer) renderAdd(ast *types.DocumentAST, params *[]string) (*types.QueryResult, error) {
	query := make(map[string]interface{})
	query["collection"] = ast.Target.Name
	query["operation"] = string(ast.Operation)

	if len(ast.Documents) > 0 {
		data := make(map[string]interface{})
		for field, value := range ast.Documents[0].Fields {
			*params = append(*params, value.Name)
			data[field.Path] = fmt.Sprintf(":%s", value.Name)
		}
		query["data"] = data
	}

	return toResult(query, *params)
}

func (r *Renderer) renderUpdate(ast *types.DocumentAST, params *[]string) (*types.QueryResult, error) {
	query := make(map[string]interface{})
	query["collection"] = ast.Target.Name
	query["operation"] = string(ast.Operation)

	data := make(map[string]interface{})
	for _, op := range ast.UpdateOps {
		if op.Operator != types.Set && op.Operator != types.Unset {
			return nil, fmt.Errorf("firestore does not support update operator: %s", op.Operator)
		}
		for field, value := range op.Fields {
			if op.Operator == types.Unset {
				data[field.Path] = "FieldValue.delete()"
			} else {
				*params = append(*params, value.Name)
				data[field.Path] = fmt.Sprintf(":%s", value.Name)
			}
		}
	}
	query["data"] = data

	return toResult(query, *params)
}

func (r *Renderer) renderDelete(ast *types.DocumentAST, params *[]string) (*types.QueryResult, error) {
	query := make(map[string]interface{})
	query["collection"] = ast.Target.Name
	query["operation"] = string(ast.Operation)

	return toResult(query, *params)
}

func (r *Renderer) buildWheres(f types.FilterItem, params *[]string) ([]map[string]interface{}, error) {
	var wheres []map[string]interface{}

	switch filter := f.(type) {
	case types.FilterCondition:
		op, err := mapOperator(filter.Operator)
		if err != nil {
			return nil, err
		}
		*params = append(*params, filter.Value.Name)
		wheres = append(wheres, map[string]interface{}{
			"field":    filter.Field.Path,
			"operator": op,
			"value":    fmt.Sprintf(":%s", filter.Value.Name),
		})

	case types.FilterGroup:
		if filter.Logic != types.AND {
			return nil, fmt.Errorf("firestore only supports AND logic in compound queries")
		}
		for _, c := range filter.Conditions {
			childWheres, err := r.buildWheres(c, params)
			if err != nil {
				return nil, err
			}
			wheres = append(wheres, childWheres...)
		}

	case types.RangeFilter:
		if filter.Min != nil {
			*params = append(*params, filter.Min.Name)
			op := ">="
			if filter.MinExclusive {
				op = ">"
			}
			wheres = append(wheres, map[string]interface{}{
				"field":    filter.Field.Path,
				"operator": op,
				"value":    fmt.Sprintf(":%s", filter.Min.Name),
			})
		}
		if filter.Max != nil {
			*params = append(*params, filter.Max.Name)
			op := "<="
			if filter.MaxExclusive {
				op = "<"
			}
			wheres = append(wheres, map[string]interface{}{
				"field":    filter.Field.Path,
				"operator": op,
				"value":    fmt.Sprintf(":%s", filter.Max.Name),
			})
		}

	default:
		return nil, fmt.Errorf("firestore does not support filter type: %T", f)
	}

	return wheres, nil
}

func mapOperator(op types.FilterOperator) (string, error) {
	switch op {
	case types.EQ:
		return "==", nil
	case types.NE:
		return "!=", nil
	case types.GT:
		return ">", nil
	case types.GTE:
		return ">=", nil
	case types.LT:
		return "<", nil
	case types.LTE:
		return "<=", nil
	case types.IN:
		return "in", nil
	case types.NotIn:
		return "not-in", nil
	case types.All:
		return "array-contains", nil
	default:
		return "", fmt.Errorf("firestore does not support filter operator: %s", op)
	}
}

// SupportsOperation indicates if Firestore supports an operation.
func (r *Renderer) SupportsOperation(op types.Operation) bool {
	switch op {
	case types.OpFind, types.OpFindOne, types.OpInsert, types.OpUpdate, types.OpDelete:
		return true
	default:
		return false
	}
}

// SupportsFilter indicates if Firestore supports a filter operator.
func (r *Renderer) SupportsFilter(op types.FilterOperator) bool {
	switch op {
	case types.EQ, types.NE, types.GT, types.GTE, types.LT, types.LTE, types.IN, types.NotIn, types.All:
		return true
	default:
		return false
	}
}

// SupportsUpdate indicates if Firestore supports an update operator.
func (r *Renderer) SupportsUpdate(op types.UpdateOperator) bool {
	switch op {
	case types.Set, types.Unset:
		return true
	default:
		return false
	}
}

// SupportsPipelineStage indicates if Firestore supports a pipeline stage.
func (r *Renderer) SupportsPipelineStage(stage string) bool {
	return false
}

func toResult(query map[string]interface{}, params []string) (*types.QueryResult, error) {
	jsonBytes, err := json.Marshal(query)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize query: %w", err)
	}
	return &types.QueryResult{
		JSON:           string(jsonBytes),
		RequiredParams: params,
	}, nil
}
