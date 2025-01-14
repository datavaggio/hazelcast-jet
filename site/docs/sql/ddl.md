---
title: DDL statements
description: DDL commands for Hazelcast Jet SQL
---

## CREATE and DROP EXTERNAL MAPPING

### Introduction

The SQL language works with _tables_ that have a fixed list of columns
with data types. To use a remote object as a table, you must first
create an _EXTERNAL MAPPING_. Jet can also access an IMap within its own
cluster and in that case the mapping is already created for you.

The mapping specifies the table name, an optional column list with
types, connection parameters, and other connector-specific parameters.

Jet creates the mappings in the `public` schema. It currently doesn't
support user-created schemas. The implicit mappings for IMaps exist in
the `partitioned` schema. You can't drop mappings in this schema, nor
are they listed in the [information schema](#information-schema) tables.

We currently do not support `ALTER MAPPING`: use `CREATE OR REPLACE
MAPPING` to replace the mapping definition or DROP the mapping first. A
change to a mapping does not affect any jobs that are already running
based on it, only new jobs are affected.

### CREATE MAPPING Synopsis

```sql
CREATE [OR REPLACE] [EXTERNAL] MAPPING [IF NOT EXISTS] mapping_name
[ ( column_name column_type [EXTERNAL NAME external_name] [, ...] ) ]
TYPE type_identifier
[ OPTIONS ( 'option_name' = 'option_value' [, ...] ) ]
```

- `OR REPLACE`: overwrite the mapping if it already exists

- `EXTERNAL`: an optional keyword, does not affect the semantics

- `IF NOT EXISTS`: do nothing if the external mapping already exists

- `mapping_name`: an SQL identifier that identifies the mapping in SQL
  queries

- `column_name`, `column_type`: the name and type of the column. For the
  list of supported types see the Hazelcast IMDG Reference Manual.

- `external_name`: the optional external name of a column. If omitted,
  Jet will generally assume it's equal to `column_name`, but a given
  connector can implement specific rules. For example, the key-value
  connectors such as IMap or Kafka assume the column to refer to a field
  in the value part of a message, except for the special names `__key`
  and `this` (referring to the key and the value, respectively). See the
  connector specification for details.

- `type_identifier`: the connector type.

- `option_name`, `option_value`: a connector-specific option. For a list
  of possible options, check out the connector javadoc. The
  `option_name` and `option_value` are string literals and must be
  enclosed in apostrophes. The `objectName` option is common for all
  connectors: it's the name of the object in the external system. By
  default, it's equal to the mapping name.

#### Auto-resolving of columns and options

The columns in the column list are optional. The connector can also
resolve the columns using the options you provide, or by sampling a
random record in the input. For example, if you give the java class name
for IMap value, we'll resolve the columns by reflecting on that class.

If the connector fails to resolve the columns, the statement will fail.
Check out individual connector documentation for details.

Here's an example with an explicit field list and options:

```sql
CREATE MAPPING my_table(
    __key INT,
    ticker VARCHAR,
    amount BIGINT EXTERNAL NAME "amountNormalized"
)
TYPE IMap
OPTIONS (
    'serialization.key.format' = 'java',
    'serialization.key.java.class' = 'java.lang.Integer'
    'serialization.value.format' = 'json'
)
```

This corresponds to an `IMap<Integer, String>` named `my_table` where
the string value is a JSON object like this:

```json
{
    ticker: "CERP",
    amountNormalized: 3000
}
```

For details regarding the above statement see the [IMap
connector](imap-connector.md) chapter.

### DROP MAPPING Synopsis

```sql
DROP [EXTERNAL] MAPPING [IF EXISTS] mapping_name
```

- `EXTERNAL`: an optional keyword, doesn't affect the semantics

- `IF EXISTS`: if the external mapping doesn't exist, do nothing; fail
  otherwise.

- `mapping_name`: the name of the mapping

## Information Schema

The information about existing mappings is available through
`information_schema` tables.

Currently, two tables are exposed:

- `mappings`: contains information about existing mappings

- `columns`: contains information about mapping columns

To query the information schema, use:

```sql
SELECT * FROM information_schema.mappings

SELECT * FROM information_schema.columns
```

## SHOW MAPPINGS

```sql
SHOW [EXTERNAL] MAPPINGS
```

- `EXTERNAL`: an optional keyword, doesn't affect the semantics

This command returns the names of existing external mappings. The output
is a shortcut to this query:

```sql
SELECT mapping_name AS name
FROM information_schema.mappings
ORDER BY name
```

## Custom connectors

Implementation of custom SQL connectors is currently not a public API,
we plan to define an API in the future.
