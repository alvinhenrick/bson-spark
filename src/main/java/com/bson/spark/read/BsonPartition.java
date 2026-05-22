package com.bson.spark.read;

import java.io.Serializable;
import org.apache.spark.sql.connector.read.InputPartition;

/** Represents a single BSON file as an input partition. */
public final class BsonPartition implements InputPartition, Serializable {

  private static final long serialVersionUID = 1L;

  private final String filePath;
  private final long fileSize;

  public BsonPartition(String filePath, long fileSize) {
    this.filePath = filePath;
    this.fileSize = fileSize;
  }

  public String filePath() {
    return filePath;
  }

  public long fileSize() {
    return fileSize;
  }

  @Override
  public String toString() {
    return String.format("BsonPartition{path=%s, size=%d}", filePath, fileSize);
  }
}
