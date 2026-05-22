# bson-spark

A Spark DataSource V2 for reading MongoDB BSON and Extended JSON (EJSON) files into DataFrames.

Reads line-delimited EJSON files (as exported by MongoDB Atlas Data Federation) and binary BSON files directly into Spark DataFrames — no intermediate conversion steps, no Python overhead. All parsing and type coercion happens natively in the JVM.

## Supported Formats

| Format | Extensions | Description |
|--------|-----------|-------------|
| EJSON v2 (Canonical Extended JSON) | `.json`, `.json.gz` | Line-delimited JSON with BSON type wrappers (`$oid`, `$date`, `$numberInt`, etc.) |
| Binary BSON | `.bson`, `.bson.gz` | Concatenated raw BSON documents |

## Installation

### Databricks

Upload the JAR as a cluster library, or add to cluster config:

```
spark.jars.packages  io.github.alvinhenrick:bson-spark_2.12:0.1.0
```

### Local / Spark Submit

```bash
spark-submit --packages io.github.alvinhenrick:bson-spark_2.12:0.1.0 your_app.py
```

## Usage

### With Explicit Schema (Recommended for Production)

```python
from pyspark.sql.types import StructType, StructField, StringType, MapType, IntegerType

schema = StructType([
    StructField("_id", StringType(), nullable=False),
    StructField("resourceType", StringType()),
    StructField("id", StringType()),
    StructField("status", StringType()),
    StructField("_access", MapType(StringType(), IntegerType())),
    StructField("_sourceAssigningAuthority", StringType()),
    StructField("_uuid", StringType()),
    StructField("_sourceId", StringType()),
])

df = (
    spark.read
    .format("bson")
    .schema(schema)
    .load("/Volumes/bronze/fhir_lake/atlas_data_federation/Account_4_0_0/2026-03-26T14-28-01/")
)

df.show()
```

### With Schema Inference

```python
# Automatically samples first 50 documents to infer schema
df = spark.read.format("bson").load("/path/to/ejson/files/")

# Control sample size
df = (
    spark.read
    .format("bson")
    .option("sampleSize", "100")
    .load("/path/to/ejson/files/")
)
```

### Reading from Various Paths

```python
# Unity Catalog Volumes
df = spark.read.format("bson").schema(schema).load("/Volumes/catalog/schema/volume/path/")

# S3
df = spark.read.format("bson").schema(schema).load("s3://bucket/prefix/")

# DBFS
df = spark.read.format("bson").schema(schema).load("dbfs:/mnt/data/exports/")

# Local filesystem
df = spark.read.format("bson").schema(schema).load("file:///tmp/exports/")
```

### Corrupt Record Handling

```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

schema = StructType([
    StructField("_id", StringType(), nullable=False),
    StructField("count", IntegerType()),
    StructField("_error", StringType()),  # receives malformed documents
])

df = (
    spark.read
    .format("bson")
    .schema(schema)
    .option("columnNameOfCorruptRecord", "_error")
    .load("/path/to/files/")
)

# Rows that fail conversion get null for all fields except _error,
# which contains the original document JSON
df.filter(df._error.isNotNull()).show()
```

### Using the "ejson" Alias

```python
# Functionally identical to format("bson") — both read .json and .bson files
df = spark.read.format("ejson").schema(schema).load("/path/to/federation/output/")
```

### Scala / Java

```scala
val df = spark.read
  .format("bson")
  .schema(schema)
  .load("/path/to/bson/files/")
```

## Type Mapping

The converter uses the declared schema to drive type coercion:

| BSON / EJSON Type | Schema DataType | Result |
|-------------------|-----------------|--------|
| `{"$oid": "hex"}` | `StringType` | `"62b196c8..."` |
| `{"$date": {"$numberLong": "ms"}}` | `TimestampType` | epoch microseconds |
| `{"$date": {"$numberLong": "ms"}}` | `StringType` | `"2024-07-20T06:40:38Z"` |
| `{"$numberInt": "42"}` | `IntegerType` | `42` |
| `{"$numberInt": "42"}` | `StringType` | `"42"` |
| `{"$numberLong": "123"}` | `LongType` | `123L` |
| `{"$numberDouble": "1.5"}` | `DoubleType` | `1.5` |
| `{"$decimal128": "1.23"}` | `DecimalType` | `Decimal(1.23)` |
| `{"$decimal128": "1.23"}` | `StringType` | `"1.23"` |
| `{"$binary": {...}}` | `BinaryType` | `byte[]` |
| `{"$binary": {...}}` | `StringType` | base64 string |
| `{"$regularExpression": {...}}` | `StringType` | pattern string |
| Nested document | `StructType` | nested row |
| Array | `ArrayType` | array |
| Document (as map) | `MapType` | map |

**Key principle:** The schema declares the target type. If the schema says `StringType` for a date field, you get an ISO string. If it says `TimestampType`, you get epoch microseconds. The schema drives everything.

## Options

| Option | Default | Description |
|--------|---------|-------------|
| `columnNameOfCorruptRecord` | `""` (disabled) | Column name to store original JSON of documents that fail conversion |
| `sampleSize` | `50` | Number of documents to sample for schema inference |

## Building

```bash
# Build for Spark 3.x (Scala 2.12) — Databricks Runtime 13.x, 14.x
./gradlew shadowJar

# Build for Spark 4.x (Scala 2.13) — Databricks Runtime 15.x+
./gradlew shadowJar -PscalaVersion=2.13

# Run tests
./gradlew test

# Build both versions
./gradlew shadowJar -PscalaVersion=2.12
./gradlew shadowJar -PscalaVersion=2.13
```

The output JAR is in `build/libs/bson-spark_2.12-0.1.0.jar` (or `_2.13`).

## How It Works

1. **File discovery** — Lists `.json` and `.bson` files at the load path. Each file becomes one Spark partition.
2. **Format detection** — Determines binary BSON vs line-delimited EJSON from file extension.
3. **Parsing** — EJSON lines are parsed with `BsonDocument.parse()` (the BSON Java driver natively understands Extended JSON v2 type wrappers). Binary BSON is decoded with `RawBsonDocument`.
4. **Type conversion** — Each `BsonValue` is converted to the Spark internal representation based on the declared schema, producing `InternalRow` objects directly in the JVM.

No Python, no intermediate files, no double-parsing.

## License

Apache License 2.0
