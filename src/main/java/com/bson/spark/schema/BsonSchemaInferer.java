package com.bson.spark.schema;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DoubleType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.RawBsonDocument;

/**
 * Infers a Spark {@link StructType} schema by sampling BSON/EJSON documents
 * from the input files.
 *
 * <p>BSON's typed nature makes inference more reliable than plain JSON:
 * numeric types (int32, int64, double, decimal128) are unambiguous,
 * dates are native types, and binary data is explicitly typed.
 */
public final class BsonSchemaInferer {

    private BsonSchemaInferer() {}

    /**
     * Infer a schema from BSON/EJSON files at the given paths.
     *
     * @param paths list of file/directory paths containing BSON or EJSON files
     * @param sampleSize number of documents to sample for inference
     * @return the inferred StructType
     */
    public static StructType infer(List<String> paths, int sampleSize) {
        List<BsonDocument> samples = sampleDocuments(paths, sampleSize);
        if (samples.isEmpty()) {
            return new StructType();
        }

        StructType schema = inferDocumentSchema(samples.get(0));
        for (int i = 1; i < samples.size(); i++) {
            schema = mergeSchemas(schema, inferDocumentSchema(samples.get(i)));
        }
        return schema;
    }

    private static StructType inferDocumentSchema(BsonDocument document) {
        List<StructField> fields = new ArrayList<>();
        for (Map.Entry<String, BsonValue> entry : document.entrySet()) {
            DataType dataType = inferType(entry.getValue());
            fields.add(DataTypes.createStructField(entry.getKey(), dataType, true));
        }
        return DataTypes.createStructType(fields);
    }

    private static DataType inferType(BsonValue value) {
        switch (value.getBsonType()) {
            case INT32:
                return DataTypes.IntegerType;
            case INT64:
                return DataTypes.LongType;
            case DOUBLE:
                return DataTypes.DoubleType;
            case DECIMAL128:
                return DataTypes.createDecimalType(34, 6);
            case STRING:
                return DataTypes.StringType;
            case BOOLEAN:
                return DataTypes.BooleanType;
            case DATE_TIME:
            case TIMESTAMP:
                return DataTypes.TimestampType;
            case BINARY:
                return DataTypes.BinaryType;
            case OBJECT_ID:
                return DataTypes.StringType;
            case DOCUMENT:
                return inferDocumentSchema(value.asDocument());
            case ARRAY:
                return inferArrayType(value.asArray());
            case NULL:
            case UNDEFINED:
                return DataTypes.NullType;
            case REGULAR_EXPRESSION:
            case JAVASCRIPT:
            case SYMBOL:
            case DB_POINTER:
                return DataTypes.StringType;
            default:
                return DataTypes.StringType;
        }
    }

    private static DataType inferArrayType(BsonArray array) {
        if (array.isEmpty()) {
            return DataTypes.createArrayType(DataTypes.NullType, true);
        }
        DataType elementType = inferType(array.get(0));
        for (int i = 1; i < array.size(); i++) {
            elementType = widenType(elementType, inferType(array.get(i)));
        }
        return DataTypes.createArrayType(elementType, true);
    }

    static StructType mergeSchemas(StructType left, StructType right) {
        Map<String, StructField> merged = new LinkedHashMap<>();
        java.util.Set<String> rightFieldNames = new java.util.HashSet<>();
        for (StructField field : right.fields()) {
            rightFieldNames.add(field.name());
        }
        for (StructField field : left.fields()) {
            if (!rightFieldNames.contains(field.name())) {
                // Field missing in right → must be nullable
                merged.put(field.name(), DataTypes.createStructField(
                        field.name(), field.dataType(), true));
            } else {
                merged.put(field.name(), field);
            }
        }
        for (StructField rightField : right.fields()) {
            String name = rightField.name();
            if (merged.containsKey(name)) {
                StructField leftField = merged.get(name);
                DataType widened = widenType(leftField.dataType(), rightField.dataType());
                boolean nullable = leftField.nullable() || rightField.nullable();
                merged.put(name, DataTypes.createStructField(name, widened, nullable));
            } else {
                // Field missing in left → must be nullable
                merged.put(name, DataTypes.createStructField(
                        name, rightField.dataType(), true));
            }
        }
        return DataTypes.createStructType(new ArrayList<>(merged.values()));
    }

    private static DataType widenType(DataType left, DataType right) {
        if (left.equals(right)) {
            return left;
        }
        if (left instanceof org.apache.spark.sql.types.NullType) {
            return right;
        }
        if (right instanceof org.apache.spark.sql.types.NullType) {
            return left;
        }
        if (isNumeric(left) && isNumeric(right)) {
            return widenNumeric(left, right);
        }
        if (left instanceof StructType && right instanceof StructType) {
            return mergeSchemas((StructType) left, (StructType) right);
        }
        if (left instanceof ArrayType && right instanceof ArrayType) {
            DataType widenedElement = widenType(
                    ((ArrayType) left).elementType(), ((ArrayType) right).elementType());
            boolean nullable = ((ArrayType) left).containsNull()
                    || ((ArrayType) right).containsNull();
            return DataTypes.createArrayType(widenedElement, nullable);
        }
        return DataTypes.StringType;
    }

    private static boolean isNumeric(DataType type) {
        return type instanceof org.apache.spark.sql.types.IntegerType
                || type instanceof org.apache.spark.sql.types.LongType
                || type instanceof DoubleType
                || type instanceof org.apache.spark.sql.types.FloatType
                || type instanceof org.apache.spark.sql.types.ShortType
                || type instanceof org.apache.spark.sql.types.DecimalType;
    }

    private static DataType widenNumeric(DataType left, DataType right) {
        if (left instanceof org.apache.spark.sql.types.DecimalType
                || right instanceof org.apache.spark.sql.types.DecimalType) {
            return DataTypes.createDecimalType(34, 6);
        }
        if (left instanceof DoubleType || right instanceof DoubleType) {
            return DataTypes.DoubleType;
        }
        if (left instanceof org.apache.spark.sql.types.FloatType
                || right instanceof org.apache.spark.sql.types.FloatType) {
            return DataTypes.DoubleType;
        }
        if (left instanceof org.apache.spark.sql.types.LongType
                || right instanceof org.apache.spark.sql.types.LongType) {
            return DataTypes.LongType;
        }
        return DataTypes.IntegerType;
    }

    private static List<BsonDocument> sampleDocuments(List<String> paths, int sampleSize) {
        List<BsonDocument> samples = new ArrayList<>();
        Configuration hadoopConf = SparkSession.active().sparkContext().hadoopConfiguration();

        for (String pathStr : paths) {
            if (samples.size() >= sampleSize) {
                break;
            }
            try {
                Path path = new Path(pathStr);
                FileSystem fs = path.getFileSystem(hadoopConf);
                sampleFromPath(fs, path, samples, sampleSize);
            } catch (IOException e) {
                throw new RuntimeException("Failed to sample documents from: " + pathStr, e);
            }
        }
        return samples;
    }

    private static void sampleFromPath(
            FileSystem fs, Path path, List<BsonDocument> samples, int sampleSize)
            throws IOException {
        FileStatus status = fs.getFileStatus(path);
        if (status.isDirectory()) {
            FileStatus[] children = fs.listStatus(path);
            for (FileStatus child : children) {
                if (samples.size() >= sampleSize) {
                    return;
                }
                sampleFromPath(fs, child.getPath(), samples, sampleSize);
            }
        } else if (isSupportedFile(path.getName())) {
            if (isBsonFile(path.getName())) {
                readBsonDocumentsFromFile(fs, path, samples, sampleSize);
            } else {
                readEjsonDocumentsFromFile(fs, path, samples, sampleSize);
            }
        }
    }

    private static void readBsonDocumentsFromFile(
            FileSystem fs, Path path, List<BsonDocument> samples, int sampleSize)
            throws IOException {
        try (InputStream inputStream = fs.open(path)) {
            while (samples.size() < sampleSize) {
                byte[] sizeBytes = new byte[4];
                int read = readFully(inputStream, sizeBytes);
                if (read < 4) {
                    break;
                }

                int docSize = ByteBuffer.wrap(sizeBytes).order(ByteOrder.LITTLE_ENDIAN).getInt();
                if (docSize < 5) {
                    break;
                }

                byte[] docBytes = new byte[docSize];
                System.arraycopy(sizeBytes, 0, docBytes, 0, 4);
                int remaining = readFully(inputStream, docBytes, 4, docSize - 4);
                if (remaining < docSize - 4) {
                    break;
                }

                samples.add(new RawBsonDocument(docBytes));
            }
        }
    }

    private static void readEjsonDocumentsFromFile(
            FileSystem fs, Path path, List<BsonDocument> samples, int sampleSize)
            throws IOException {
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(fs.open(path), StandardCharsets.UTF_8))) {
            String line;
            while (samples.size() < sampleSize && (line = reader.readLine()) != null) {
                line = line.trim();
                if (!line.isEmpty()) {
                    samples.add(BsonDocument.parse(line));
                }
            }
        }
    }

    private static boolean isSupportedFile(String filename) {
        String lower = filename.toLowerCase();
        return lower.endsWith(".bson") || lower.endsWith(".bson.gz")
                || lower.endsWith(".json") || lower.endsWith(".json.gz");
    }

    private static boolean isBsonFile(String filename) {
        String lower = filename.toLowerCase();
        return lower.endsWith(".bson") || lower.endsWith(".bson.gz");
    }

    private static int readFully(InputStream in, byte[] buffer) throws IOException {
        return readFully(in, buffer, 0, buffer.length);
    }

    private static int readFully(InputStream in, byte[] buffer, int offset, int length)
            throws IOException {
        int totalRead = 0;
        while (totalRead < length) {
            int read = in.read(buffer, offset + totalRead, length - totalRead);
            if (read < 0) {
                break;
            }
            totalRead += read;
        }
        return totalRead;
    }
}
