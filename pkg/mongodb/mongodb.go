// Package mongodb provides a MongoDB renderer for DOCQL.
package mongodb

import (
	"encoding/json"
	"fmt"

	"github.com/zoobzio/docql/internal/types"
)

// Renderer renders DocumentAST to MongoDB query format.
type Renderer struct{}

// New creates a new MongoDB renderer.
func New() *Renderer {
	return &Renderer{}
}

// Render converts a DocumentAST to MongoDB query format.
func (r *Renderer) Render(ast *types.DocumentAST) (*types.QueryResult, error) {
	if err := ast.Validate(); err != nil {
		return nil, fmt.Errorf("invalid AST: %w", err)
	}

	var params []string

	switch ast.Operation {
	case types.OpFind, types.OpFindOne:
		return r.renderFind(ast, &params)
	case types.OpInsert:
		return r.renderInsert(ast, &params)
	case types.OpInsertMany:
		return r.renderInsertMany(ast, &params)
	case types.OpUpdate:
		return r.renderUpdate(ast, &params)
	case types.OpUpdateMany:
		return r.renderUpdateMany(ast, &params)
	case types.OpDelete:
		return r.renderDelete(ast, &params)
	case types.OpDeleteMany:
		return r.renderDeleteMany(ast, &params)
	case types.OpAggregate:
		return r.renderAggregate(ast, &params)
	case types.OpCount:
		return r.renderCount(ast, &params)
	case types.OpDistinct:
		return r.renderDistinct(ast, &params)
	default:
		return nil, fmt.Errorf("unsupported operation: %s", ast.Operation)
	}
}

func (r *Renderer) renderFind(ast *types.DocumentAST, params *[]string) (*types.QueryResult, error) {
	query := make(map[string]interface{})
	query["collection"] = ast.Target.Name
	query["operation"] = string(ast.Operation)

	if ast.FilterClause != nil {
		filter, err := r.renderFilter(ast.FilterClause, params)
		if err != nil {
			return nil, err
		}
		query["filter"] = filter
	} else {
		query["filter"] = map[string]interface{}{}
	}

	if ast.Projection != nil {
		query["projection"] = r.renderProjection(ast.Projection)
	}

	if len(ast.SortClauses) > 0 {
		sort := make(map[string]interface{})
		for _, s := range ast.SortClauses {
			sort[s.Field.Path] = int(s.Order)
		}
		query["sort"] = sort
	}

	if ast.Skip != nil {
		if ast.Skip.Static != nil {
			query["skip"] = *ast.Skip.Static
		} else if ast.Skip.Param != nil {
			*params = append(*params, ast.Skip.Param.Name)
			query["skip"] = fmt.Sprintf(":%s", ast.Skip.Param.Name)
		}
	}

	if ast.Limit != nil {
		if ast.Limit.Static != nil {
			query["limit"] = *ast.Limit.Static
		} else if ast.Limit.Param != nil {
			*params = append(*params, ast.Limit.Param.Name)
			query["limit"] = fmt.Sprintf(":%s", ast.Limit.Param.Name)
		}
	}

	return toResult(query, *params)
}

func (r *Renderer) renderInsert(ast *types.DocumentAST, params *[]string) (*types.QueryResult, error) {
	query := make(map[string]interface{})
	query["collection"] = ast.Target.Name
	query["operation"] = string(ast.Operation)

	if len(ast.Documents) > 0 {
		doc := r.renderDocument(ast.Documents[0], params)
		query["document"] = doc
	}

	return toResult(query, *params)
}

func (r *Renderer) renderInsertMany(ast *types.DocumentAST, params *[]string) (*types.QueryResult, error) {
	query := make(map[string]interface{})
	query["collection"] = ast.Target.Name
	query["operation"] = string(ast.Operation)

	docs := make([]map[string]interface{}, len(ast.Documents))
	for i, doc := range ast.Documents {
		docs[i] = r.renderDocument(doc, params)
	}
	query["documents"] = docs

	return toResult(query, *params)
}

func (r *Renderer) renderUpdate(ast *types.DocumentAST, params *[]string) (*types.QueryResult, error) {
	query := make(map[string]interface{})
	query["collection"] = ast.Target.Name
	query["operation"] = string(ast.Operation)

	if ast.FilterClause != nil {
		filter, err := r.renderFilter(ast.FilterClause, params)
		if err != nil {
			return nil, err
		}
		query["filter"] = filter
	} else {
		query["filter"] = map[string]interface{}{}
	}

	query["update"] = r.renderUpdateOps(ast.UpdateOps, params)

	if ast.Upsert {
		query["upsert"] = true
	}

	return toResult(query, *params)
}

func (r *Renderer) renderUpdateMany(ast *types.DocumentAST, params *[]string) (*types.QueryResult, error) {
	return r.renderUpdate(ast, params)
}

func (r *Renderer) renderDelete(ast *types.DocumentAST, params *[]string) (*types.QueryResult, error) {
	query := make(map[string]interface{})
	query["collection"] = ast.Target.Name
	query["operation"] = string(ast.Operation)

	if ast.FilterClause != nil {
		filter, err := r.renderFilter(ast.FilterClause, params)
		if err != nil {
			return nil, err
		}
		query["filter"] = filter
	} else {
		query["filter"] = map[string]interface{}{}
	}

	return toResult(query, *params)
}

func (r *Renderer) renderDeleteMany(ast *types.DocumentAST, params *[]string) (*types.QueryResult, error) {
	return r.renderDelete(ast, params)
}

func (r *Renderer) renderAggregate(ast *types.DocumentAST, params *[]string) (*types.QueryResult, error) {
	query := make(map[string]interface{})
	query["collection"] = ast.Target.Name
	query["operation"] = string(ast.Operation)

	pipeline := make([]map[string]interface{}, 0, len(ast.Pipeline))
	for _, stage := range ast.Pipeline {
		rendered, err := r.renderPipelineStage(stage, params)
		if err != nil {
			return nil, err
		}
		pipeline = append(pipeline, rendered)
	}
	query["pipeline"] = pipeline

	return toResult(query, *params)
}

func (r *Renderer) renderCount(ast *types.DocumentAST, params *[]string) (*types.QueryResult, error) {
	query := make(map[string]interface{})
	query["collection"] = ast.Target.Name
	query["operation"] = string(ast.Operation)

	if ast.FilterClause != nil {
		filter, err := r.renderFilter(ast.FilterClause, params)
		if err != nil {
			return nil, err
		}
		query["filter"] = filter
	} else {
		query["filter"] = map[string]interface{}{}
	}

	return toResult(query, *params)
}

func (r *Renderer) renderDistinct(ast *types.DocumentAST, params *[]string) (*types.QueryResult, error) {
	query := make(map[string]interface{})
	query["collection"] = ast.Target.Name
	query["operation"] = string(ast.Operation)
	query["field"] = ast.DistinctField.Path

	if ast.FilterClause != nil {
		filter, err := r.renderFilter(ast.FilterClause, params)
		if err != nil {
			return nil, err
		}
		query["filter"] = filter
	}

	return toResult(query, *params)
}

func (r *Renderer) renderFilter(f types.FilterItem, params *[]string) (interface{}, error) {
	switch filter := f.(type) {
	case types.FilterCondition:
		if filter.Value.Name != "" {
			*params = append(*params, filter.Value.Name)
		}
		return map[string]interface{}{
			filter.Field.Path: map[string]interface{}{
				string(filter.Operator): fmt.Sprintf(":%s", filter.Value.Name),
			},
		}, nil

	case types.FilterGroup:
		conditions := make([]interface{}, 0, len(filter.Conditions))
		for _, c := range filter.Conditions {
			rendered, err := r.renderFilter(c, params)
			if err != nil {
				return nil, err
			}
			conditions = append(conditions, rendered)
		}
		return map[string]interface{}{
			string(filter.Logic): conditions,
		}, nil

	case types.RangeFilter:
		rangeFilter := make(map[string]interface{})
		if filter.Min != nil {
			*params = append(*params, filter.Min.Name)
			op := "$gte"
			if filter.MinExclusive {
				op = "$gt"
			}
			rangeFilter[op] = fmt.Sprintf(":%s", filter.Min.Name)
		}
		if filter.Max != nil {
			*params = append(*params, filter.Max.Name)
			op := "$lte"
			if filter.MaxExclusive {
				op = "$lt"
			}
			rangeFilter[op] = fmt.Sprintf(":%s", filter.Max.Name)
		}
		return map[string]interface{}{
			filter.Field.Path: rangeFilter,
		}, nil

	case types.RegexFilter:
		*params = append(*params, filter.Pattern.Name)
		regexFilter := map[string]interface{}{
			"$regex": fmt.Sprintf(":%s", filter.Pattern.Name),
		}
		if filter.Options != nil {
			*params = append(*params, filter.Options.Name)
			regexFilter["$options"] = fmt.Sprintf(":%s", filter.Options.Name)
		}
		return map[string]interface{}{
			filter.Field.Path: regexFilter,
		}, nil

	case types.ExistsFilter:
		return map[string]interface{}{
			filter.Field.Path: map[string]interface{}{
				"$exists": filter.Exists,
			},
		}, nil

	case types.GeoFilter:
		*params = append(*params, filter.Center.Lon.Name)
		*params = append(*params, filter.Center.Lat.Name)
		geoQuery := map[string]interface{}{
			"$geometry": map[string]interface{}{
				"type": "Point",
				"coordinates": []string{
					fmt.Sprintf(":%s", filter.Center.Lon.Name),
					fmt.Sprintf(":%s", filter.Center.Lat.Name),
				},
			},
		}
		if filter.Radius != nil {
			*params = append(*params, filter.Radius.Name)
			geoQuery["$maxDistance"] = fmt.Sprintf(":%s", filter.Radius.Name)
		}
		return map[string]interface{}{
			filter.Field.Path: map[string]interface{}{
				string(filter.Operator): geoQuery,
			},
		}, nil

	case types.ArrayFilter:
		*params = append(*params, filter.Value.Name)
		return map[string]interface{}{
			filter.Field.Path: map[string]interface{}{
				string(filter.Operator): fmt.Sprintf(":%s", filter.Value.Name),
			},
		}, nil

	case types.ElemMatchFilter:
		conditions := make(map[string]interface{})
		for _, c := range filter.Conditions {
			rendered, err := r.renderFilter(c, params)
			if err != nil {
				return nil, err
			}
			if m, ok := rendered.(map[string]interface{}); ok {
				for k, v := range m {
					conditions[k] = v
				}
			}
		}
		return map[string]interface{}{
			filter.Field.Path: map[string]interface{}{
				"$elemMatch": conditions,
			},
		}, nil

	case types.TextSearchFilter:
		*params = append(*params, filter.Search.Name)
		textQuery := map[string]interface{}{
			"$search": fmt.Sprintf(":%s", filter.Search.Name),
		}
		if filter.Language != nil {
			*params = append(*params, filter.Language.Name)
			textQuery["$language"] = fmt.Sprintf(":%s", filter.Language.Name)
		}
		if filter.CaseSensitive {
			textQuery["$caseSensitive"] = true
		}
		if filter.DiacriticSensitive {
			textQuery["$diacriticSensitive"] = true
		}
		return map[string]interface{}{
			"$text": textQuery,
		}, nil

	default:
		return nil, fmt.Errorf("unsupported filter type: %T", f)
	}
}

func (r *Renderer) renderProjection(p *types.Projection) map[string]interface{} {
	proj := make(map[string]interface{})
	for _, f := range p.Fields {
		if f.Include {
			proj[f.Field.Path] = 1
		} else {
			proj[f.Field.Path] = 0
		}
	}
	return proj
}

func (r *Renderer) renderDocument(doc types.Document, params *[]string) map[string]interface{} {
	result := make(map[string]interface{})
	for field, value := range doc.Fields {
		*params = append(*params, value.Name)
		result[field.Path] = fmt.Sprintf(":%s", value.Name)
	}
	return result
}

func (r *Renderer) renderUpdateOps(ops []types.UpdateOperation, params *[]string) map[string]interface{} {
	result := make(map[string]interface{})
	for _, op := range ops {
		fields := make(map[string]interface{})
		for field, value := range op.Fields {
			if value.Name != "" {
				*params = append(*params, value.Name)
				fields[field.Path] = fmt.Sprintf(":%s", value.Name)
			} else {
				fields[field.Path] = ""
			}
		}
		result[string(op.Operator)] = fields
	}
	return result
}

func (r *Renderer) renderPipelineStage(stage types.PipelineStage, params *[]string) (map[string]interface{}, error) {
	switch s := stage.(type) {
	case types.MatchStage:
		filter, err := r.renderFilter(s.Filter, params)
		if err != nil {
			return nil, err
		}
		return map[string]interface{}{
			"$match": filter,
		}, nil

	case types.ProjectStage:
		return map[string]interface{}{
			"$project": r.renderProjection(&s.Projection),
		}, nil

	case types.GroupStage:
		group := make(map[string]interface{})
		group["_id"] = r.renderExpression(s.ID, params)
		for name, acc := range s.Accumulators {
			group[name] = map[string]interface{}{
				acc.Operator: r.renderExpression(acc.Expr, params),
			}
		}
		return map[string]interface{}{
			"$group": group,
		}, nil

	case types.SortStage:
		sort := make(map[string]interface{})
		for _, sc := range s.Sorts {
			sort[sc.Field.Path] = int(sc.Order)
		}
		return map[string]interface{}{
			"$sort": sort,
		}, nil

	case types.LimitStage:
		var limit interface{}
		if s.Limit.Static != nil {
			limit = *s.Limit.Static
		} else if s.Limit.Param != nil {
			*params = append(*params, s.Limit.Param.Name)
			limit = fmt.Sprintf(":%s", s.Limit.Param.Name)
		}
		return map[string]interface{}{
			"$limit": limit,
		}, nil

	case types.SkipStage:
		var skip interface{}
		if s.Skip.Static != nil {
			skip = *s.Skip.Static
		} else if s.Skip.Param != nil {
			*params = append(*params, s.Skip.Param.Name)
			skip = fmt.Sprintf(":%s", s.Skip.Param.Name)
		}
		return map[string]interface{}{
			"$skip": skip,
		}, nil

	case types.UnwindStage:
		unwind := map[string]interface{}{
			"path": "$" + s.Path.Path,
		}
		if s.IncludeArrayIndex != nil {
			unwind["includeArrayIndex"] = *s.IncludeArrayIndex
		}
		if s.PreserveNullAndEmptyArrays {
			unwind["preserveNullAndEmptyArrays"] = true
		}
		return map[string]interface{}{
			"$unwind": unwind,
		}, nil

	case types.LookupStage:
		lookup := map[string]interface{}{
			"from":         s.From,
			"localField":   s.LocalField.Path,
			"foreignField": s.ForeignField.Path,
			"as":           s.As,
		}
		return map[string]interface{}{
			"$lookup": lookup,
		}, nil

	case types.AddFieldsStage:
		fields := make(map[string]interface{})
		for name, expr := range s.Fields {
			fields[name] = r.renderExpression(expr, params)
		}
		return map[string]interface{}{
			"$addFields": fields,
		}, nil

	case types.CountStage:
		return map[string]interface{}{
			"$count": s.FieldName,
		}, nil

	default:
		return nil, fmt.Errorf("unsupported pipeline stage: %T", stage)
	}
}

func (r *Renderer) renderExpression(expr types.Expression, params *[]string) interface{} {
	if expr == nil {
		return nil
	}

	switch e := expr.(type) {
	case types.FieldExpression:
		return "$" + e.Field.Path

	case types.LiteralExpression:
		*params = append(*params, e.Value.Name)
		return fmt.Sprintf(":%s", e.Value.Name)

	case types.OperatorExpression:
		args := make([]interface{}, len(e.Args))
		for i, arg := range e.Args {
			args[i] = r.renderExpression(arg, params)
		}
		return map[string]interface{}{
			e.Operator: args,
		}

	case types.ConditionalExpression:
		return map[string]interface{}{
			"$cond": map[string]interface{}{
				"if":   r.renderExpression(e.If, params),
				"then": r.renderExpression(e.Then, params),
				"else": r.renderExpression(e.Else, params),
			},
		}

	default:
		return nil
	}
}

// SupportsOperation indicates if MongoDB supports an operation.
func (r *Renderer) SupportsOperation(op types.Operation) bool {
	return true
}

// SupportsFilter indicates if MongoDB supports a filter operator.
func (r *Renderer) SupportsFilter(op types.FilterOperator) bool {
	return true
}

// SupportsUpdate indicates if MongoDB supports an update operator.
func (r *Renderer) SupportsUpdate(op types.UpdateOperator) bool {
	return true
}

// SupportsPipelineStage indicates if MongoDB supports a pipeline stage.
func (r *Renderer) SupportsPipelineStage(stage string) bool {
	return true
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
