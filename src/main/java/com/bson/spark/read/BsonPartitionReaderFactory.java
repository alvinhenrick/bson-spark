package com.bson.spark.read;

import java.io.Serializable;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.types.StructType;

import com.bson.spark.options.BsonReadOptions;
import com.bson.spark.schema.BsonToRowConverter;

/**
 * Factory that creates {@link BsonPartitionReader} instances for each partition.
 */
public final class BsonPartitionReaderFactory implements PartitionReaderFactory, Serializable {

    private static final long serialVersionUID = 1L;

    private final StructType schema;
    private final BsonReadOptions options;

    public BsonPartitionReaderFactory(StructType schema, BsonReadOptions options) {
        this.schema = schema;
        this.options = options;
    }

    @Override
    public PartitionReader<InternalRow> createReader(InputPartition partition) {
        BsonPartition bsonPartition = (BsonPartition) partition;
        BsonToRowConverter converter = new BsonToRowConverter(schema, options);
        return new BsonPartitionReader(bsonPartition, converter);
    }
}
