package com.bson.spark;

import com.bson.spark.options.BsonReadOptions;
import com.bson.spark.read.BsonTable;
import com.bson.spark.schema.BsonSchemaInferer;
import java.util.Map;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/**
 * Spark DataSource V2 entry point for reading BSON/EJSON files.
 *
 * <p>Registers under the short name {@code "bson"}. Supports both binary BSON files (.bson) and
 * line-delimited Extended JSON files (.json) as exported by MongoDB Atlas Data Federation.
 *
 * <p>Usage:
 *
 * <pre>{@code
 * // With explicit schema (recommended for production)
 * spark.read.format("bson").schema(schema).load("/Volumes/.../path/")
 *
 * // With schema inference (samples first N documents)
 * spark.read.format("bson").load("/Volumes/.../path/")
 * }</pre>
 */
public final class BsonDataSource implements TableProvider, DataSourceRegister {

  @Override
  public String shortName() {
    return "bson";
  }

  @Override
  public StructType inferSchema(CaseInsensitiveStringMap options) {
    BsonReadOptions readOptions = new BsonReadOptions(options);
    return BsonSchemaInferer.infer(readOptions.paths(), readOptions.sampleSize());
  }

  @Override
  public Table getTable(
      StructType schema, Transform[] partitioning, Map<String, String> properties) {
    return new BsonTable(schema, new CaseInsensitiveStringMap(properties));
  }

  @Override
  public boolean supportsExternalMetadata() {
    return true;
  }
}
