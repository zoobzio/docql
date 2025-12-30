// Package couchdb provides a CouchDB renderer for DOCQL.
package couchdb

import (
	"encoding/json"
	"fmt"

	"github.com/zoobzio/docql/internal/types"
)

// Renderer renders DocumentAST to CouchDB Mango query format.
type Renderer struct{}

// New creates a new CouchDB renderer.
func New() *Renderer {
	return &Renderer{}
}

// Render converts a DocumentAST to CouchDB Mango query format.
func (r *Renderer) Render(ast *types.DocumentAST) (*types.QueryResult, error) {
	if err := ast.Validate(); err != nil {
		return nil, fmt.Errorf("invalid AST: %w", err)
	}

	if !r.SupportsOperation(ast.Operation) {
		return nil, fmt.Errorf("CouchDB does not support operation: %s", ast.Operation)
	}

	var params []string

	switch ast.Operation {
	case types.OpFind, types.OpFindOne:
		return r.renderFind(ast, &params)
	case types.OpInsert:
		return r.renderInsert(ast, &params)
	case types.OpUpdate:
		return r.renderUpdate(ast, &params)
	case types.OpDelete:
		return r.renderDelete(ast, &params)
	default:
		return nil, fmt.Errorf("unsupported operation: %s", ast.Operation)
	}
}

func (r *Renderer) renderFind(ast *types.DocumentAST, params *[]string) (*types.QueryResult, error) {
	query := make(map[string]interface{})

	if ast.FilterClause != nil {
		selector, err := r.buildSelector(ast.FilterClause, params)
		if err != nil {
			return nil, err
		}
		query["selector"] = selector
	} else {
		query["selector"] = map[string]interface{}{}
	}

	if ast.Projection != nil {
		fields := make([]string, 0)
		for _, f := range ast.Projection.Fields {
			if f.Include {
				fields = append(fields, f.Field.Path)
			}
		}
		if len(fields) > 0 {
			query["fields"] = fields
		}
	}

	if len(ast.SortClauses) > 0 {
		sort := make([]map[string]string, len(ast.SortClauses))
		for i, s := range ast.SortClauses {
			direction := "asc"
			if s.Order == types.Descending {
				direction = "desc"
			}
			sort[i] = map[string]string{
				s.Field.Path: direction,
			}
		}
		query["sort"] = sort
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
			query["skip"] = *ast.Skip.Static
		} else if ast.Skip.Param != nil {
			*params = append(*params, ast.Skip.Param.Name)
			query["skip"] = fmt.Sprintf(":%s", ast.Skip.Param.Name)
		}
	}

	return toResult(query, *params)
}

func (r *Renderer) renderInsert(ast *types.DocumentAST, params *[]string) (*types.QueryResult, error) {
	query := make(map[string]interface{})
	query["operation"] = "insert"

	if len(ast.Documents) > 0 {
		doc := make(map[string]interface{})
		for field, value := range ast.Documents[0].Fields {
			*params = append(*params, value.Name)
			doc[field.Path] = fmt.Sprintf(":%s", value.Name)
		}
		query["doc"] = doc
	}

	return toResult(query, *params)
}

func (r *Renderer) renderUpdate(ast *types.DocumentAST, params *[]string) (*types.QueryResult, error) {
	query := make(map[string]interface{})
	query["operation"] = "update"

	if ast.FilterClause != nil {
		selector, err := r.buildSelector(ast.FilterClause, params)
		if err != nil {
			return nil, err
		}
		query["selector"] = selector
	}

	updates := make(map[string]interface{})
	for _, op := range ast.UpdateOps {
		if op.Operator != types.Set {
			return nil, fmt.Errorf("CouchDB only supports $set updates")
		}
		for field, value := range op.Fields {
			*params = append(*params, value.Name)
			updates[field.Path] = fmt.Sprintf(":%s", value.Name)
		}
	}
	query["updates"] = updates

	return toResult(query, *params)
}

func (r *Renderer) renderDelete(ast *types.DocumentAST, params *[]string) (*types.QueryResult, error) {
	query := make(map[string]interface{})
	query["operation"] = "delete"

	if ast.FilterClause != nil {
		selector, err := r.buildSelector(ast.FilterClause, params)
		if err != nil {
			return nil, err
		}
		query["selector"] = selector
	}

	return toResult(query, *params)
}

func (r *Renderer) buildSelector(f types.FilterItem, params *[]string) (interface{}, error) {
	switch filter := f.(type) {
	case types.FilterCondition:
		*params = append(*params, filter.Value.Name)
		op := mapOperator(filter.Operator)
		if op == "" {
			return nil, fmt.Errorf("CouchDB does not support filter operator: %s", filter.Operator)
		}
		return map[string]interface{}{
			filter.Field.Path: map[string]interface{}{
				op: fmt.Sprintf(":%s", filter.Value.Name),
			},
		}, nil

	case types.FilterGroup:
		conditions := make([]interface{}, 0, len(filter.Conditions))
		for _, c := range filter.Conditions {
			rendered, err := r.buildSelector(c, params)
			if err != nil {
				return nil, err
			}
			conditions = append(conditions, rendered)
		}
		logic := mapLogic(filter.Logic)
		return map[string]interface{}{
			logic: conditions,
		}, nil

	case types.RangeFilter:
		rangeSelector := make(map[string]interface{})
		if filter.Min != nil {
			*params = append(*params, filter.Min.Name)
			op := "$gte"
			if filter.MinExclusive {
				op = "$gt"
			}
			rangeSelector[op] = fmt.Sprintf(":%s", filter.Min.Name)
		}
		if filter.Max != nil {
			*params = append(*params, filter.Max.Name)
			op := "$lte"
			if filter.MaxExclusive {
				op = "$lt"
			}
			rangeSelector[op] = fmt.Sprintf(":%s", filter.Max.Name)
		}
		return map[string]interface{}{
			filter.Field.Path: rangeSelector,
		}, nil

	case types.RegexFilter:
		*params = append(*params, filter.Pattern.Name)
		return map[string]interface{}{
			filter.Field.Path: map[string]interface{}{
				"$regex": fmt.Sprintf(":%s", filter.Pattern.Name),
			},
		}, nil

	case types.ExistsFilter:
		return map[string]interface{}{
			filter.Field.Path: map[string]interface{}{
				"$exists": filter.Exists,
			},
		}, nil

	default:
		return nil, fmt.Errorf("CouchDB does not support filter type: %T", f)
	}
}

func mapOperator(op types.FilterOperator) string {
	switch op {
	case types.EQ:
		return "$eq"
	case types.NE:
		return "$ne"
	case types.GT:
		return "$gt"
	case types.GTE:
		return "$gte"
	case types.LT:
		return "$lt"
	case types.LTE:
		return "$lte"
	case types.IN:
		return "$in"
	case types.NotIn:
		return "$nin"
	case types.Regex:
		return "$regex"
	case types.Exists:
		return "$exists"
	default:
		return ""
	}
}

func mapLogic(op types.LogicOperator) string {
	switch op {
	case types.AND:
		return "$and"
	case types.OR:
		return "$or"
	case types.NOR:
		return "$nor"
	default:
		return "$and"
	}
}

// SupportsOperation indicates if CouchDB supports an operation.
func (r *Renderer) SupportsOperation(op types.Operation) bool {
	switch op {
	case types.OpFind, types.OpFindOne, types.OpInsert, types.OpUpdate, types.OpDelete:
		return true
	default:
		return false
	}
}

// SupportsFilter indicates if CouchDB supports a filter operator.
func (r *Renderer) SupportsFilter(op types.FilterOperator) bool {
	switch op {
	case types.EQ, types.NE, types.GT, types.GTE, types.LT, types.LTE, types.IN, types.NotIn, types.Regex, types.Exists:
		return true
	default:
		return false
	}
}

// SupportsUpdate indicates if CouchDB supports an update operator.
func (r *Renderer) SupportsUpdate(op types.UpdateOperator) bool {
	return op == types.Set
}

// SupportsPipelineStage indicates if CouchDB supports a pipeline stage.
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
