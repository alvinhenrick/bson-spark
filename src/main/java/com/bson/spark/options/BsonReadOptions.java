package com.bson.spark.options;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/**
 * Configuration options for the BSON DataSource reader.
 *
 * <p>Values are eagerly resolved at construction time, so this object is safely serializable to
 * Spark executors.
 */
public final class BsonReadOptions implements Serializable {

  private static final long serialVersionUID = 1L;

  public static final String PATH = "path";
  public static final String PATHS = "paths";
  public static final String SAMPLE_SIZE = "sampleSize";
  public static final String COLUMN_NAME_OF_CORRUPT_RECORD = "columnNameOfCorruptRecord";

  private static final int DEFAULT_SAMPLE_SIZE = 50;

  private final List<String> paths;
  private final int sampleSize;
  private final String columnNameOfCorruptRecord;

  public BsonReadOptions(CaseInsensitiveStringMap options) {
    this.paths = resolvePaths(options);
    this.sampleSize = options.getInt(SAMPLE_SIZE, DEFAULT_SAMPLE_SIZE);
    this.columnNameOfCorruptRecord = options.getOrDefault(COLUMN_NAME_OF_CORRUPT_RECORD, "");
  }

  public List<String> paths() {
    return paths;
  }

  public int sampleSize() {
    return sampleSize;
  }

  public String columnNameOfCorruptRecord() {
    return columnNameOfCorruptRecord;
  }

  private static List<String> resolvePaths(CaseInsensitiveStringMap options) {
    String path = options.get(PATH);
    if (path != null) {
      return Collections.singletonList(path);
    }
    String pathsStr = options.get(PATHS);
    if (pathsStr != null) {
      return Arrays.asList(pathsStr.split(","));
    }
    return Collections.emptyList();
  }
}
