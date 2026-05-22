package com.bson.spark.read;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/**
 * Represents a BSON file-backed table that supports batch reads.
 */
public final class BsonTable implements Table, SupportsRead {

    private final StructType schema;
    private final CaseInsensitiveStringMap options;

    public BsonTable(StructType schema, CaseInsensitiveStringMap options) {
        this.schema = schema;
        this.options = options;
    }

    @Override
    public String name() {
        return "bson";
    }

    @Override
    public StructType schema() {
        return schema;
    }

    @Override
    public Set<TableCapability> capabilities() {
        Set<TableCapability> capabilities = new HashSet<>();
        capabilities.add(TableCapability.BATCH_READ);
        return capabilities;
    }

    @Override
    public ScanBuilder newScanBuilder(CaseInsensitiveStringMap scanOptions) {
        CaseInsensitiveStringMap merged = mergeOptions(options, scanOptions);
        return new BsonScanBuilder(schema, merged);
    }

    private static CaseInsensitiveStringMap mergeOptions(
            CaseInsensitiveStringMap base, CaseInsensitiveStringMap override) {
        Map<String, String> merged = new HashMap<>(base.asCaseSensitiveMap());
        merged.putAll(override.asCaseSensitiveMap());
        return new CaseInsensitiveStringMap(merged);
    }
}
