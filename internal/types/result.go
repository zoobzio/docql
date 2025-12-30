package types

// QueryResult represents the result of rendering a document query.
type QueryResult struct {
	// JSON contains the rendered query in provider-specific format.
	JSON string

	// RequiredParams lists the parameter names that must be provided at execution time.
	RequiredParams []string
}
