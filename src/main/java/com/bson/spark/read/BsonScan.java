package com.bson.spark.read;

import com.bson.spark.options.BsonReadOptions;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/**
 * Plans the input partitions for a BSON/EJSON file scan. Each file becomes one partition for
 * maximum parallelism.
 *
 * <p>Supported file extensions:
 *
 * <ul>
 *   <li>{@code .bson}, {@code .bson.gz} — binary BSON format
 *   <li>{@code .json}, {@code .json.gz} — line-delimited EJSON (Extended JSON)
 * </ul>
 */
public final class BsonScan implements Scan, Batch {

  private final StructType schema;

  private final BsonReadOptions readOptions;

  public BsonScan(StructType schema, CaseInsensitiveStringMap options) {
    this.schema = schema;
    this.readOptions = new BsonReadOptions(options);
  }

  @Override
  public StructType readSchema() {
    return schema;
  }

  @Override
  public Batch toBatch() {
    return this;
  }

  @Override
  public InputPartition[] planInputPartitions() {
    List<InputPartition> partitions = new ArrayList<>();
    Configuration hadoopConf = SparkSession.active().sparkContext().hadoopConfiguration();

    for (String pathStr : readOptions.paths()) {
      try {
        Path path = new Path(pathStr);
        FileSystem fs = path.getFileSystem(hadoopConf);
        collectFiles(fs, path, partitions);
      } catch (IOException e) {
        throw new RuntimeException("Failed to list files at: " + pathStr, e);
      }
    }

    if (partitions.isEmpty()) {
      throw new RuntimeException("No BSON/JSON files found at: " + readOptions.paths());
    }

    return partitions.toArray(new InputPartition[0]);
  }

  @Override
  public PartitionReaderFactory createReaderFactory() {
    Configuration hadoopConf = SparkSession.active().sparkContext().hadoopConfiguration();
    return new BsonPartitionReaderFactory(schema, readOptions, hadoopConf);
  }

  private void collectFiles(FileSystem fs, Path path, List<InputPartition> partitions)
      throws IOException {
    FileStatus status = fs.getFileStatus(path);
    if (status.isDirectory()) {
      FileStatus[] children = fs.listStatus(path);
      for (FileStatus child : children) {
        if (child.isDirectory()) {
          collectFiles(fs, child.getPath(), partitions);
        } else if (isSupportedFile(child.getPath().getName())) {
          partitions.add(new BsonPartition(child.getPath().toString(), child.getLen()));
        }
      }
    } else if (isSupportedFile(status.getPath().getName())) {
      partitions.add(new BsonPartition(status.getPath().toString(), status.getLen()));
    }
  }

  private static boolean isSupportedFile(String filename) {
    String lower = filename.toLowerCase();
    return lower.endsWith(".bson")
        || lower.endsWith(".bson.gz")
        || lower.endsWith(".json")
        || lower.endsWith(".json.gz");
  }
}
