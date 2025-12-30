# docql

[![CI](https://github.com/zoobzio/docql/actions/workflows/ci.yml/badge.svg)](https://github.com/zoobzio/docql/actions/workflows/ci.yml)
[![Coverage](https://codecov.io/gh/zoobzio/docql/branch/main/graph/badge.svg)](https://codecov.io/gh/zoobzio/docql)
[![Go Report Card](https://goreportcard.com/badge/github.com/zoobzio/docql)](https://goreportcard.com/report/github.com/zoobzio/docql)
[![CodeQL](https://github.com/zoobzio/docql/actions/workflows/codeql.yml/badge.svg)](https://github.com/zoobzio/docql/actions/workflows/codeql.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/zoobzio/docql.svg)](https://pkg.go.dev/github.com/zoobzio/docql)
[![License](https://img.shields.io/github/license/zoobzio/docql)](LICENSE)
[![Go Version](https://img.shields.io/github/go-mod/go-version/zoobzio/docql)](go.mod)
[![Release](https://img.shields.io/github/v/release/zoobzio/docql)](https://github.com/zoobzio/docql/releases)

Type-safe document database query builder with DDML schema validation.

Build queries as an AST, validate against your schema, render to provider-specific JSON.

## Build, Validate, Render

```go
// Build
query := docql.Find(instance.C("users")).
    Filter(instance.Eq(instance.F("users", "status"), instance.P("status"))).
    SortDesc(instance.F("users", "createdAt")).
    Limit(10)

// Validate — C(), F(), P() check against your DDML schema

// Render
result, _ := query.Render(mongodb.New())
// {"collection":"users","operation":"FIND","filter":{"status":{"$eq":":status"}},"sort":{"createdAt":-1},"limit":10}
```

Collections and fields validated at construction. Values always parameterized. Use `TryC`, `TryF`, `TryP` for runtime validation with error returns.

## Install

```bash
go get github.com/zoobzio/docql
go get github.com/zoobzio/ddml
```

Requires Go 1.24+.

## Quick Start

```go
package main

import (
    "fmt"
    "github.com/zoobzio/docql"
    "github.com/zoobzio/docql/pkg/mongodb"
    "github.com/zoobzio/ddml"
)

func main() {
    // Define schema
    schema := ddml.NewSchema("myapp")
    users := ddml.NewCollection("users")
    users.AddField(ddml.NewField("_id", ddml.TypeObjectID))
    users.AddField(ddml.NewField("username", ddml.TypeString))
    users.AddField(ddml.NewField("email", ddml.TypeString))
    users.AddField(ddml.NewField("status", ddml.TypeString))
    users.AddField(ddml.NewField("createdAt", ddml.TypeDate))
    schema.AddCollection(users)

    // Create instance
    instance, err := docql.NewFromDDML(schema)
    if err != nil {
        panic(err)
    }

    // Build and render
    result, err := docql.Find(instance.C("users")).
        Filter(instance.Eq(instance.F("users", "status"), instance.P("status"))).
        SortDesc(instance.F("users", "createdAt")).
        Limit(10).
        Render(mongodb.New())

    if err != nil {
        panic(err)
    }

    fmt.Println(result.JSON)
    fmt.Println(result.RequiredParams)
    // [status]
}
```

## Providers

Same AST, different backends:

```go
import (
    "github.com/zoobzio/docql/pkg/mongodb"
    "github.com/zoobzio/docql/pkg/dynamodb"
    "github.com/zoobzio/docql/pkg/firestore"
    "github.com/zoobzio/docql/pkg/couchdb"
)

result, _ := query.Render(mongodb.New())   // MongoDB
result, _ := query.Render(dynamodb.New())  // DynamoDB
result, _ := query.Render(firestore.New()) // Firestore
result, _ := query.Render(couchdb.New())   // CouchDB
```

Each provider handles dialect differences and returns errors for unsupported operations.

### Provider Capabilities

| Feature | MongoDB | DynamoDB | Firestore | CouchDB |
|---------|---------|----------|-----------|---------|
| Find/FindOne | Yes | Yes | Yes | Yes |
| Insert | Yes | Yes | Yes | Yes |
| Update | Yes | Yes | Yes | Yes |
| Delete | Yes | Yes | Yes | Yes |
| Aggregate | Yes | No | No | No |
| Count | Yes | No | No | No |
| Distinct | Yes | No | No | No |

## Operations

```go
// Queries
docql.Find(collection)           // Find documents
docql.FindOne(collection)        // Find single document
docql.Count(collection)          // Count documents
docql.Distinct(collection, field) // Distinct values

// Mutations
docql.Insert(collection)         // Insert document
docql.InsertMany(collection)     // Insert multiple documents
docql.Update(collection)         // Update document
docql.UpdateMany(collection)     // Update multiple documents
docql.Delete(collection)         // Delete document
docql.DeleteMany(collection)     // Delete multiple documents

// Aggregation
docql.Aggregate(collection)      // Aggregation pipeline
```

## Filters

```go
// Comparison
docql.Eq(field, param)   // Equal
docql.Ne(field, param)   // Not equal
docql.Gt(field, param)   // Greater than
docql.Gte(field, param)  // Greater than or equal
docql.Lt(field, param)   // Less than
docql.Lte(field, param)  // Less than or equal

// Set operations
docql.In(field, param)    // In array
docql.NotIn(field, param) // Not in array

// Logical
docql.And(conditions...)  // AND group
docql.Or(conditions...)   // OR group
docql.Nor(conditions...)  // NOR group

// Special
docql.Exists(field)       // Field exists
docql.NotExists(field)    // Field does not exist
docql.Regex(field, param) // Regex match
docql.Range(field, min, max) // Range filter
```

## Aggregation Pipeline

```go
docql.Aggregate(instance.C("orders")).
    Match(instance.Eq(instance.F("orders", "status"), instance.P("status"))).
    Group(
        instance.F("orders", "userId"),
        map[string]types.Accumulator{
            "total": docql.Sum(docql.FieldExpr(instance.F("orders", "amount"))),
            "count": docql.CountAcc(),
        },
    ).
    Sort(instance.F("orders", "total"), types.Descending).
    Limit(10)
```

## Why DOCQL?

- **Schema-validated** — `C("users")` and `F("users", "email")` checked against DDML at build time
- **Injection-resistant** — parameterized values, validated identifiers, no string concatenation
- **Multi-backend** — one query, four databases
- **Composable** — filters, aggregations, pagination, sorting

## Related Projects

DOCQL is part of a family of type-safe query builders:

- **[ASTQL](https://github.com/zoobzio/astql)** — SQL query builder with DBML schema validation (PostgreSQL, MySQL, SQLite, SQL Server)
- **[VECTQL](https://github.com/zoobzio/vectql)** — Vector database query builder with VDML schema validation (Pinecone, Qdrant, Milvus, Weaviate)
- **DOCQL** — Document database query builder with DDML schema validation (MongoDB, DynamoDB, Firestore, CouchDB)

## Contributing

Contributions welcome. See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

For security vulnerabilities, see [SECURITY.md](SECURITY.md).

## License

MIT — see [LICENSE](LICENSE).
