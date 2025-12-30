package types

// Field represents a reference to a document field.
// Path uses dot-notation for nested fields (e.g., "address.city").
type Field struct {
	Path       string
	Collection string
}
