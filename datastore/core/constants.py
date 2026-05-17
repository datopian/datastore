
from __future__ import annotations

POSTGRES_TYPES: dict[str, str] = {
    # integer
    "int2": "int2",
    "smallint": "int2",
    "smallserial": "int2",
    "int4": "int4",
    "int": "int4",
    "integer": "int4",
    "serial": "int4",
    "int8": "int8",
    "bigint": "int8",
    "bigserial": "int8",
    # floating-point
    "float4": "float4",
    "real": "float4",
    "float8": "float8",
    "float": "float8",
    "double": "float8",
    "double precision": "float8",
    # exact numeric
    "numeric": "numeric",
    "decimal": "numeric",
    # boolean
    "bool": "bool",
    "boolean": "bool",
    # character
    "text": "text",
    "varchar": "varchar",
    "character varying": "varchar",
    "char": "char",
    "character": "char",
    # binary
    "bytea": "bytea",
    # date / time
    "date": "date",
    "time": "time",
    "time without time zone": "time",
    "timetz": "timetz",
    "time with time zone": "timetz",
    "timestamp": "timestamp",
    "timestamp without time zone": "timestamp",
    "timestamptz": "timestamptz",
    "timestamp with time zone": "timestamptz",
    "interval": "interval",
    # json
    "json": "json",
    "jsonb": "jsonb",
    # uuid
    "uuid": "uuid",
    # network
    "inet": "inet",
    "cidr": "cidr",
    "macaddr": "macaddr",
    # markup
    "xml": "xml",
}


