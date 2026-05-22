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
 * Alias DataSource registered under the short name {@code "ejson"}.
 *
 * <p>Functionally identical to {@link BsonDataSource} — both read BSON binary and line-delimited
 * EJSON files. This alias exists so callers can use the format name that matches their file type:
 *
 * <pre>{@code
 * spark.read.format("ejson").schema(schema).load(path)  // federation JSON output
 * spark.read.format("bson").schema(schema).load(path)   // binary BSON files
 * }</pre>
 */
public final class EjsonDataSource implements TableProvider, DataSourceRegister {

  @Override
  public String shortName() {
    return "ejson";
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
