package types

// FilterItem represents either a single filter condition or a group of conditions.
type FilterItem interface {
	isFilterItem()
}

// FilterCondition represents a single document filter.
type FilterCondition struct {
	Field    Field
	Operator FilterOperator
	Value    Param
}

func (FilterCondition) isFilterItem() {}

// FilterGroup represents grouped conditions with AND/OR/NOR logic.
type FilterGroup struct {
	Logic      LogicOperator
	Conditions []FilterItem
}

func (FilterGroup) isFilterItem() {}

// RangeFilter represents a range query with min/max bounds.
type RangeFilter struct {
	Field        Field
	Min          *Param
	Max          *Param
	MinExclusive bool
	MaxExclusive bool
}

func (RangeFilter) isFilterItem() {}

// RegexFilter represents a regular expression filter.
type RegexFilter struct {
	Field   Field
	Pattern Param
	Options *Param
}

func (RegexFilter) isFilterItem() {}

// TextSearchFilter represents full-text search.
type TextSearchFilter struct {
	Search             Param
	Language           *Param
	CaseSensitive      bool
	DiacriticSensitive bool
}

func (TextSearchFilter) isFilterItem() {}

// GeoPoint represents a geographic coordinate.
type GeoPoint struct {
	Lon Param
	Lat Param
}

// GeoFilter represents a geospatial query.
type GeoFilter struct {
	Field       Field
	Operator    FilterOperator
	Center      GeoPoint
	Radius      *Param
	MaxDistance *Param
	MinDistance *Param
}

func (GeoFilter) isFilterItem() {}

// ArrayFilter represents an array query with $all or $size.
type ArrayFilter struct {
	Field    Field
	Operator FilterOperator
	Value    Param
}

func (ArrayFilter) isFilterItem() {}

// ElemMatchFilter represents an $elemMatch query for array elements.
type ElemMatchFilter struct {
	Field      Field
	Conditions []FilterItem
}

func (ElemMatchFilter) isFilterItem() {}

// ExistsFilter represents a field existence check.
type ExistsFilter struct {
	Field  Field
	Exists bool
}

func (ExistsFilter) isFilterItem() {}
