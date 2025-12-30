package types

// FilterOperator represents document filter operators.
type FilterOperator string

// Comparison operators.
const (
	EQ  FilterOperator = "$eq"
	NE  FilterOperator = "$ne"
	GT  FilterOperator = "$gt"
	GTE FilterOperator = "$gte"
	LT  FilterOperator = "$lt"
	LTE FilterOperator = "$lte"
)

// Set operators.
const (
	IN    FilterOperator = "$in"
	NotIn FilterOperator = "$nin"
)

// Element operators.
const (
	Exists FilterOperator = "$exists"
	Type   FilterOperator = "$type"
)

// Evaluation operators.
const (
	Regex FilterOperator = "$regex"
	Text  FilterOperator = "$text"
	Mod   FilterOperator = "$mod"
)

// Array operators.
const (
	All       FilterOperator = "$all"
	ElemMatch FilterOperator = "$elemMatch"
	Size      FilterOperator = "$size"
)

// Geospatial operators.
const (
	GeoWithin     FilterOperator = "$geoWithin"
	GeoIntersects FilterOperator = "$geoIntersects"
	Near          FilterOperator = "$near"
	NearSphere    FilterOperator = "$nearSphere"
)

// LogicOperator represents logical operators for combining filter conditions.
type LogicOperator string

// Logic operators.
const (
	AND LogicOperator = "$and"
	OR  LogicOperator = "$or"
	NOR LogicOperator = "$nor"
	NOT LogicOperator = "$not"
)

// UpdateOperator represents document update operators.
type UpdateOperator string

// Field update operators.
const (
	Set         UpdateOperator = "$set"
	Unset       UpdateOperator = "$unset"
	SetOnInsert UpdateOperator = "$setOnInsert"
	Inc         UpdateOperator = "$inc"
	Mul         UpdateOperator = "$mul"
	Min         UpdateOperator = "$min"
	Max         UpdateOperator = "$max"
	Rename      UpdateOperator = "$rename"
	CurrentDate UpdateOperator = "$currentDate"
)

// Array update operators.
const (
	AddToSet UpdateOperator = "$addToSet"
	Pop      UpdateOperator = "$pop"
	Pull     UpdateOperator = "$pull"
	Push     UpdateOperator = "$push"
	PullAll  UpdateOperator = "$pullAll"
)

// SortOrder represents sort direction.
type SortOrder int

// Sort order constants.
const (
	Ascending  SortOrder = 1
	Descending SortOrder = -1
)

// Operation represents the type of document database operation.
type Operation string

// Document database operations.
const (
	OpFind       Operation = "FIND"
	OpFindOne    Operation = "FIND_ONE"
	OpInsert     Operation = "INSERT"
	OpInsertMany Operation = "INSERT_MANY"
	OpUpdate     Operation = "UPDATE"
	OpUpdateMany Operation = "UPDATE_MANY"
	OpDelete     Operation = "DELETE"
	OpDeleteMany Operation = "DELETE_MANY"
	OpAggregate  Operation = "AGGREGATE"
	OpCount      Operation = "COUNT"
	OpDistinct   Operation = "DISTINCT"
)

// Complexity limits.
const (
	MaxFilterDepth      = 10
	MaxBatchSize        = 1000
	MaxLimit            = 10000
	MaxProjectionFields = 100
	MaxSortFields       = 10
	MaxPipelineStages   = 50
)
