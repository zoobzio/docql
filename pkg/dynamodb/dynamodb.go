// Package dynamodb provides a DynamoDB renderer for DOCQL.
package dynamodb

import (
	"encoding/json"
	"fmt"

	"github.com/zoobzio/docql/internal/types"
)

// Renderer renders DocumentAST to DynamoDB query format.
type Renderer struct {
	// PartitionKey specifies the partition key attribute name.
	PartitionKey string
	// SortKey specifies the sort key attribute name (optional).
	SortKey string
}

// New creates a new DynamoDB renderer.
func New() *Renderer {
	return &Renderer{
		PartitionKey: "pk",
	}
}

// WithPartitionKey sets the partition key attribute name.
func (r *Renderer) WithPartitionKey(pk string) *Renderer {
	r.PartitionKey = pk
	return r
}

// WithSortKey sets the sort key attribute name.
func (r *Renderer) WithSortKey(sk string) *Renderer {
	r.SortKey = sk
	return r
}

// Render converts a DocumentAST to DynamoDB query format.
func (r *Renderer) Render(ast *types.DocumentAST) (*types.QueryResult, error) {
	if err := ast.Validate(); err != nil {
		return nil, fmt.Errorf("invalid AST: %w", err)
	}

	if !r.SupportsOperation(ast.Operation) {
		return nil, fmt.Errorf("DynamoDB does not support operation: %s", ast.Operation)
	}

	var params []string

	switch ast.Operation {
	case types.OpFind, types.OpFindOne:
		return r.renderQuery(ast, &params)
	case types.OpInsert:
		return r.renderPutItem(ast, &params)
	case types.OpUpdate:
		return r.renderUpdateItem(ast, &params)
	case types.OpDelete:
		return r.renderDeleteItem(ast, &params)
	default:
		return nil, fmt.Errorf("unsupported operation: %s", ast.Operation)
	}
}

func (r *Renderer) renderQuery(ast *types.DocumentAST, params *[]string) (*types.QueryResult, error) {
	query := make(map[string]interface{})
	query["TableName"] = ast.Target.Name

	attrNames := make(map[string]string)
	attrValues := make(map[string]string)
	nameCounter := 0
	valueCounter := 0

	getName := func(field string) string {
		key := fmt.Sprintf("#n%d", nameCounter)
		nameCounter++
		attrNames[key] = field
		return key
	}

	getValue := func(param string) string {
		key := fmt.Sprintf(":v%d", valueCounter)
		valueCounter++
		attrValues[key] = fmt.Sprintf(":%s", param)
		*params = append(*params, param)
		return key
	}

	if ast.FilterClause != nil {
		expr, err := r.buildFilterExpression(ast.FilterClause, getName, getValue)
		if err != nil {
			return nil, err
		}
		query["FilterExpression"] = expr
	}

	if len(attrNames) > 0 {
		query["ExpressionAttributeNames"] = attrNames
	}
	if len(attrValues) > 0 {
		query["ExpressionAttributeValues"] = attrValues
	}

	if ast.Limit != nil {
		if ast.Limit.Static != nil {
			query["Limit"] = *ast.Limit.Static
		} else if ast.Limit.Param != nil {
			*params = append(*params, ast.Limit.Param.Name)
			query["Limit"] = fmt.Sprintf(":%s", ast.Limit.Param.Name)
		}
	}

	if ast.Projection != nil {
		projExpr := ""
		for i, f := range ast.Projection.Fields {
			if f.Include {
				if i > 0 {
					projExpr += ", "
				}
				projExpr += getName(f.Field.Path)
			}
		}
		if projExpr != "" {
			query["ProjectionExpression"] = projExpr
		}
	}

	return toResult(query, *params)
}

func (r *Renderer) renderPutItem(ast *types.DocumentAST, params *[]string) (*types.QueryResult, error) {
	query := make(map[string]interface{})
	query["TableName"] = ast.Target.Name

	if len(ast.Documents) > 0 {
		item := make(map[string]interface{})
		for field, value := range ast.Documents[0].Fields {
			*params = append(*params, value.Name)
			item[field.Path] = fmt.Sprintf(":%s", value.Name)
		}
		query["Item"] = item
	}

	return toResult(query, *params)
}

func (r *Renderer) renderUpdateItem(ast *types.DocumentAST, params *[]string) (*types.QueryResult, error) {
	query := make(map[string]interface{})
	query["TableName"] = ast.Target.Name

	attrNames := make(map[string]string)
	attrValues := make(map[string]string)
	nameCounter := 0
	valueCounter := 0

	getName := func(field string) string {
		key := fmt.Sprintf("#n%d", nameCounter)
		nameCounter++
		attrNames[key] = field
		return key
	}

	getValue := func(param string) string {
		key := fmt.Sprintf(":v%d", valueCounter)
		valueCounter++
		attrValues[key] = fmt.Sprintf(":%s", param)
		*params = append(*params, param)
		return key
	}

	var setExprs []string
	var removeExprs []string

	for _, op := range ast.UpdateOps {
		switch op.Operator {
		case types.Set, types.Inc:
			for field, value := range op.Fields {
				nameKey := getName(field.Path)
				valueKey := getValue(value.Name)
				if op.Operator == types.Inc {
					setExprs = append(setExprs, fmt.Sprintf("%s = %s + %s", nameKey, nameKey, valueKey))
				} else {
					setExprs = append(setExprs, fmt.Sprintf("%s = %s", nameKey, valueKey))
				}
			}
		case types.Unset:
			for field := range op.Fields {
				nameKey := getName(field.Path)
				removeExprs = append(removeExprs, nameKey)
			}
		default:
			return nil, fmt.Errorf("DynamoDB does not support update operator: %s", op.Operator)
		}
	}

	var updateExpr string
	if len(setExprs) > 0 {
		updateExpr = "SET "
		for i, expr := range setExprs {
			if i > 0 {
				updateExpr += ", "
			}
			updateExpr += expr
		}
	}
	if len(removeExprs) > 0 {
		if updateExpr != "" {
			updateExpr += " "
		}
		updateExpr += "REMOVE "
		for i, expr := range removeExprs {
			if i > 0 {
				updateExpr += ", "
			}
			updateExpr += expr
		}
	}

	query["UpdateExpression"] = updateExpr

	if len(attrNames) > 0 {
		query["ExpressionAttributeNames"] = attrNames
	}
	if len(attrValues) > 0 {
		query["ExpressionAttributeValues"] = attrValues
	}

	return toResult(query, *params)
}

func (r *Renderer) renderDeleteItem(ast *types.DocumentAST, params *[]string) (*types.QueryResult, error) {
	query := make(map[string]interface{})
	query["TableName"] = ast.Target.Name

	return toResult(query, *params)
}

func (r *Renderer) buildFilterExpression(f types.FilterItem, getName func(string) string, getValue func(string) string) (string, error) {
	switch filter := f.(type) {
	case types.FilterCondition:
		nameKey := getName(filter.Field.Path)
		valueKey := getValue(filter.Value.Name)
		op := mapOperator(filter.Operator)
		if op == "" {
			return "", fmt.Errorf("DynamoDB does not support filter operator: %s", filter.Operator)
		}
		return fmt.Sprintf("%s %s %s", nameKey, op, valueKey), nil

	case types.FilterGroup:
		if len(filter.Conditions) == 0 {
			return "", nil
		}
		exprs := make([]string, 0, len(filter.Conditions))
		for _, c := range filter.Conditions {
			expr, err := r.buildFilterExpression(c, getName, getValue)
			if err != nil {
				return "", err
			}
			exprs = append(exprs, "("+expr+")")
		}
		logic := "AND"
		if filter.Logic == types.OR {
			logic = "OR"
		}
		result := exprs[0]
		for i := 1; i < len(exprs); i++ {
			result = fmt.Sprintf("%s %s %s", result, logic, exprs[i])
		}
		return result, nil

	case types.ExistsFilter:
		nameKey := getName(filter.Field.Path)
		if filter.Exists {
			return fmt.Sprintf("attribute_exists(%s)", nameKey), nil
		}
		return fmt.Sprintf("attribute_not_exists(%s)", nameKey), nil

	default:
		return "", fmt.Errorf("DynamoDB does not support filter type: %T", f)
	}
}

func mapOperator(op types.FilterOperator) string {
	switch op {
	case types.EQ:
		return "="
	case types.NE:
		return "<>"
	case types.GT:
		return ">"
	case types.GTE:
		return ">="
	case types.LT:
		return "<"
	case types.LTE:
		return "<="
	default:
		return ""
	}
}

// SupportsOperation indicates if DynamoDB supports an operation.
func (r *Renderer) SupportsOperation(op types.Operation) bool {
	switch op {
	case types.OpFind, types.OpFindOne, types.OpInsert, types.OpUpdate, types.OpDelete:
		return true
	default:
		return false
	}
}

// SupportsFilter indicates if DynamoDB supports a filter operator.
func (r *Renderer) SupportsFilter(op types.FilterOperator) bool {
	switch op {
	case types.EQ, types.NE, types.GT, types.GTE, types.LT, types.LTE, types.Exists:
		return true
	default:
		return false
	}
}

// SupportsUpdate indicates if DynamoDB supports an update operator.
func (r *Renderer) SupportsUpdate(op types.UpdateOperator) bool {
	switch op {
	case types.Set, types.Unset, types.Inc:
		return true
	default:
		return false
	}
}

// SupportsPipelineStage indicates if DynamoDB supports a pipeline stage.
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
