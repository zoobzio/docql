package docql

import "github.com/zoobzio/docql/internal/types"

// Renderer defines the interface for provider-specific query rendering.
type Renderer interface {
	// Render converts a DocumentAST to a provider-specific QueryResult.
	Render(ast *types.DocumentAST) (*types.QueryResult, error)

	// SupportsOperation indicates if the provider supports an operation.
	SupportsOperation(op types.Operation) bool

	// SupportsFilter indicates if the provider supports a filter operator.
	SupportsFilter(op types.FilterOperator) bool

	// SupportsUpdate indicates if the provider supports an update operator.
	SupportsUpdate(op types.UpdateOperator) bool

	// SupportsPipelineStage indicates if the provider supports a pipeline stage.
	SupportsPipelineStage(stage string) bool
}
