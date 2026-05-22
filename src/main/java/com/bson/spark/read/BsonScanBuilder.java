package com.bson.spark.read;

import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/**
 * Builds a {@link BsonScan} from the provided schema and options.
 */
public final class BsonScanBuilder implements ScanBuilder {

    private final StructType schema;
    private final CaseInsensitiveStringMap options;

    public BsonScanBuilder(StructType schema, CaseInsensitiveStringMap options) {
        this.schema = schema;
        this.options = options;
    }

    @Override
    public Scan build() {
        return new BsonScan(schema, options);
    }
}
