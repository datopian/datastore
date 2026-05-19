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


# Canonical Postgres type → Frictionless field type. Used when converting
# the legacy `fields` shape into a Frictionless schema for `datastore_create`.
# Many-to-one on purpose: all width-variants of integer map to `integer`,
# all of timestamp to `datetime`, etc. Anything without a closer match
# falls through to `string`.
POSTGRES_TO_FRICTIONLESS: dict[str, str] = {
    "int2": "integer",
    "int4": "integer",
    "int8": "integer",
    "float4": "number",
    "float8": "number",
    "numeric": "number",
    "bool": "boolean",
    "text": "string",
    "varchar": "string",
    "char": "string",
    "bytea": "string",
    "date": "date",
    "time": "time",
    "timetz": "time",
    "timestamp": "datetime",
    "timestamptz": "datetime",
    "interval": "duration",
    "json": "object",
    "jsonb": "object",
    "uuid": "string",
    "inet": "string",
    "cidr": "string",
    "macaddr": "string",
    "xml": "string",
}


# Frictionless field type → canonical Postgres type. Used when deriving the
# legacy `fields` shape from a Frictionless schema. Lossy in the other
# direction (e.g., a Frictionless `integer` could have been any width); we
# pick the widest/most-permissive Postgres type so the column accepts
# anything the Frictionless type implies.
FRICTIONLESS_TO_POSTGRES: dict[str, str] = {
    "integer": "int8",
    "number": "numeric",
    "boolean": "bool",
    "string": "text",
    "date": "date",
    "time": "timetz",
    "datetime": "timestamptz",
    "duration": "interval",
    "object": "jsonb",
    "array": "jsonb",
    "geojson": "jsonb",
    "geopoint": "text",
    "year": "int4",
    "yearmonth": "text",
    "any": "text",
}
