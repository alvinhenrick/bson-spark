package com.bson.spark.read;

import com.bson.spark.options.BsonReadOptions;
import com.bson.spark.schema.BsonToRowConverter;
import java.io.Serializable;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.util.SerializableConfiguration;

/** Factory that creates {@link BsonPartitionReader} instances for each partition. */
public final class BsonPartitionReaderFactory implements PartitionReaderFactory, Serializable {

  private static final long serialVersionUID = 1L;

  private final StructType schema;
  private final BsonReadOptions options;
  private final SerializableConfiguration hadoopConf;

  public BsonPartitionReaderFactory(
      StructType schema, BsonReadOptions options, Configuration hadoopConf) {
    this.schema = schema;
    this.options = options;
    this.hadoopConf = new SerializableConfiguration(hadoopConf);
  }

  @Override
  public PartitionReader<InternalRow> createReader(InputPartition partition) {
    BsonPartition bsonPartition = (BsonPartition) partition;
    BsonToRowConverter converter = new BsonToRowConverter(schema, options);
    return new BsonPartitionReader(bsonPartition, converter, hadoopConf.value());
  }
}
