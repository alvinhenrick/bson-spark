package com.bson.spark.schema;

import com.bson.spark.options.BsonReadOptions;
import java.io.Serializable;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Base64;
import java.util.Map;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.util.ArrayBasedMapData;
import org.apache.spark.sql.catalyst.util.GenericArrayData;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.BinaryType;
import org.apache.spark.sql.types.BooleanType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DateType;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.DoubleType;
import org.apache.spark.sql.types.FloatType;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.LongType;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.ShortType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.TimestampType;
import org.apache.spark.unsafe.types.UTF8String;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonValue;

/**
 * Converts a {@link BsonDocument} to a Spark {@link InternalRow} based on the provided schema.
 *
 * <p>This is the core of the DataSource — it maps BSON typed values directly to Spark's internal
 * representation without any intermediate JSON serialization step.
 *
 * <p>Type mapping follows the declared schema. If the schema says {@code StringType} for a field
 * that contains an ObjectId, the ObjectId is converted to its hex string. If the schema says {@code
 * TimestampType}, the BSON datetime is converted to epoch microseconds. The schema drives the
 * conversion.
 */
public final class BsonToRowConverter implements Serializable {

  private static final long serialVersionUID = 1L;

  private final StructType schema;
  private final String corruptRecordColumn;
  private final boolean hasCorruptRecordColumn;

  public BsonToRowConverter(StructType schema, BsonReadOptions options) {
    this.schema = schema;
    this.corruptRecordColumn = options.columnNameOfCorruptRecord();
    this.hasCorruptRecordColumn =
        !corruptRecordColumn.isEmpty()
            && Arrays.asList(schema.fieldNames()).contains(corruptRecordColumn);
  }

  /**
   * Convert a BsonDocument to an InternalRow.
   *
   * @param document the BSON document to convert
   * @return the InternalRow, or null if the document is malformed
   */
  public InternalRow convert(BsonDocument document) {
    try {
      return documentToInternalRow(document, schema);
    } catch (Exception e) {
      if (hasCorruptRecordColumn) {
        return createCorruptRecordRow(document);
      }
      return null;
    }
  }

  private InternalRow documentToInternalRow(BsonDocument document, StructType structType) {
    StructField[] fields = structType.fields();
    Object[] values = new Object[fields.length];

    for (int i = 0; i < fields.length; i++) {
      StructField field = fields[i];
      String fieldName = field.name();

      if (hasCorruptRecordColumn && fieldName.equals(corruptRecordColumn)) {
        values[i] = null;
        continue;
      }

      if (!document.containsKey(fieldName)) {
        if (!field.nullable()) {
          throw new BsonConversionException("Missing required field: " + fieldName);
        }
        values[i] = null;
        continue;
      }

      BsonValue bsonValue = document.get(fieldName);
      if (bsonValue == null || bsonValue.isNull()) {
        values[i] = null;
      } else {
        values[i] = convertValue(fieldName, field.dataType(), bsonValue);
      }
    }

    return new GenericInternalRow(values);
  }

  private Object convertValue(String fieldName, DataType dataType, BsonValue bsonValue) {
    if (bsonValue.isNull()) {
      return null;
    }

    if (dataType instanceof StructType) {
      return convertToStruct(fieldName, (StructType) dataType, bsonValue);
    } else if (dataType instanceof ArrayType) {
      return convertToArray(fieldName, (ArrayType) dataType, bsonValue);
    } else if (dataType instanceof MapType) {
      return convertToMap(fieldName, (MapType) dataType, bsonValue);
    } else if (dataType instanceof StringType) {
      return convertToString(bsonValue);
    } else if (dataType instanceof IntegerType) {
      return convertToInt(bsonValue);
    } else if (dataType instanceof LongType) {
      return convertToLong(bsonValue);
    } else if (dataType instanceof DoubleType) {
      return convertToDouble(bsonValue);
    } else if (dataType instanceof FloatType) {
      return (float) convertToDouble(bsonValue);
    } else if (dataType instanceof ShortType) {
      return (short) convertToInt(bsonValue);
    } else if (dataType instanceof BooleanType) {
      return convertToBoolean(bsonValue);
    } else if (dataType instanceof BinaryType) {
      return convertToBinary(bsonValue);
    } else if (dataType instanceof TimestampType) {
      return convertToTimestamp(bsonValue);
    } else if (dataType instanceof DateType) {
      return convertToDate(bsonValue);
    } else if (dataType instanceof DecimalType) {
      return convertToDecimal(bsonValue, (DecimalType) dataType);
    }

    throw new BsonConversionException(
        String.format("Unsupported data type '%s' for field '%s'", dataType.typeName(), fieldName));
  }

  // ─── Struct ────────────────────────────────────────────────────────

  private InternalRow convertToStruct(
      String fieldName, StructType structType, BsonValue bsonValue) {
    if (!bsonValue.isDocument()) {
      throw new BsonConversionException(
          String.format(
              "Expected document for field '%s', got %s", fieldName, bsonValue.getBsonType()));
    }
    return documentToInternalRow(bsonValue.asDocument(), structType);
  }

  // ─── Array ─────────────────────────────────────────────────────────

  private GenericArrayData convertToArray(
      String fieldName, ArrayType arrayType, BsonValue bsonValue) {
    if (!bsonValue.isArray()) {
      throw new BsonConversionException(
          String.format(
              "Expected array for field '%s', got %s", fieldName, bsonValue.getBsonType()));
    }
    BsonArray bsonArray = bsonValue.asArray();
    DataType elementType = arrayType.elementType();
    Object[] elements = new Object[bsonArray.size()];

    for (int i = 0; i < bsonArray.size(); i++) {
      BsonValue element = bsonArray.get(i);
      if (element.isNull()) {
        elements[i] = null;
      } else {
        elements[i] = convertValue(fieldName + "[" + i + "]", elementType, element);
      }
    }
    return new GenericArrayData(elements);
  }

  // ─── Map ───────────────────────────────────────────────────────────

  private ArrayBasedMapData convertToMap(String fieldName, MapType mapType, BsonValue bsonValue) {
    if (!bsonValue.isDocument()) {
      throw new BsonConversionException(
          String.format(
              "Expected document for map field '%s', got %s", fieldName, bsonValue.getBsonType()));
    }
    BsonDocument mapDoc = bsonValue.asDocument();
    DataType valueType = mapType.valueType();

    Object[] keys = new Object[mapDoc.size()];
    Object[] values = new Object[mapDoc.size()];

    int i = 0;
    for (Map.Entry<String, BsonValue> entry : mapDoc.entrySet()) {
      keys[i] = UTF8String.fromString(entry.getKey());
      BsonValue val = entry.getValue();
      if (val.isNull()) {
        values[i] = null;
      } else {
        values[i] = convertValue(fieldName + "." + entry.getKey(), valueType, val);
      }
      i++;
    }

    return new ArrayBasedMapData(new GenericArrayData(keys), new GenericArrayData(values));
  }

  // ─── String ────────────────────────────────────────────────────────

  private UTF8String convertToString(BsonValue bsonValue) {
    switch (bsonValue.getBsonType()) {
      case STRING:
        return UTF8String.fromString(bsonValue.asString().getValue());
      case OBJECT_ID:
        return UTF8String.fromString(bsonValue.asObjectId().getValue().toHexString());
      case BINARY:
        byte[] data = bsonValue.asBinary().getData();
        return UTF8String.fromString(Base64.getEncoder().encodeToString(data));
      case DATE_TIME:
        return UTF8String.fromString(formatDateTime(bsonValue.asDateTime().getValue()));
      case TIMESTAMP:
        long tsMillis = (long) bsonValue.asTimestamp().getTime() * 1000L;
        return UTF8String.fromString(formatDateTime(tsMillis));
      case DECIMAL128:
        return UTF8String.fromString(bsonValue.asDecimal128().getValue().toString());
      case BOOLEAN:
        return UTF8String.fromString(String.valueOf(bsonValue.asBoolean().getValue()));
      case INT32:
        return UTF8String.fromString(String.valueOf(bsonValue.asInt32().getValue()));
      case INT64:
        return UTF8String.fromString(String.valueOf(bsonValue.asInt64().getValue()));
      case DOUBLE:
        return UTF8String.fromString(String.valueOf(bsonValue.asDouble().getValue()));
      case REGULAR_EXPRESSION:
        return UTF8String.fromString(bsonValue.asRegularExpression().getPattern());
      case DB_POINTER:
        return UTF8String.fromString(
            bsonValue.asDBPointer().getNamespace()
                + ":"
                + bsonValue.asDBPointer().getId().toHexString());
      case DOCUMENT:
        return UTF8String.fromString(bsonValue.asDocument().toJson());
      case ARRAY:
        return UTF8String.fromString(new BsonDocument("v", bsonValue).toJson());
      default:
        return UTF8String.fromString(bsonValue.toString());
    }
  }

  // ─── Numeric ───────────────────────────────────────────────────────

  private int convertToInt(BsonValue bsonValue) {
    switch (bsonValue.getBsonType()) {
      case INT32:
        return bsonValue.asInt32().getValue();
      case INT64:
        return (int) bsonValue.asInt64().getValue();
      case DOUBLE:
        return (int) bsonValue.asDouble().getValue();
      case DECIMAL128:
        return bsonValue.asDecimal128().getValue().bigDecimalValue().intValue();
      case STRING:
        return Integer.parseInt(bsonValue.asString().getValue().trim());
      default:
        throw new BsonConversionException(
            "Cannot convert " + bsonValue.getBsonType() + " to integer");
    }
  }

  private long convertToLong(BsonValue bsonValue) {
    switch (bsonValue.getBsonType()) {
      case INT64:
        return bsonValue.asInt64().getValue();
      case INT32:
        return bsonValue.asInt32().getValue();
      case DOUBLE:
        return (long) bsonValue.asDouble().getValue();
      case DATE_TIME:
        return bsonValue.asDateTime().getValue();
      case TIMESTAMP:
        return (long) bsonValue.asTimestamp().getTime() * 1000L;
      case DECIMAL128:
        return bsonValue.asDecimal128().getValue().bigDecimalValue().longValue();
      case STRING:
        return Long.parseLong(bsonValue.asString().getValue().trim());
      default:
        throw new BsonConversionException("Cannot convert " + bsonValue.getBsonType() + " to long");
    }
  }

  private double convertToDouble(BsonValue bsonValue) {
    switch (bsonValue.getBsonType()) {
      case DOUBLE:
        return bsonValue.asDouble().getValue();
      case INT32:
        return bsonValue.asInt32().getValue();
      case INT64:
        return (double) bsonValue.asInt64().getValue();
      case DECIMAL128:
        return bsonValue.asDecimal128().getValue().bigDecimalValue().doubleValue();
      case STRING:
        return Double.parseDouble(bsonValue.asString().getValue().trim());
      default:
        throw new BsonConversionException(
            "Cannot convert " + bsonValue.getBsonType() + " to double");
    }
  }

  // ─── Boolean ───────────────────────────────────────────────────────

  private boolean convertToBoolean(BsonValue bsonValue) {
    if (bsonValue.isBoolean()) {
      return bsonValue.asBoolean().getValue();
    }
    throw new BsonConversionException("Cannot convert " + bsonValue.getBsonType() + " to boolean");
  }

  // ─── Binary ────────────────────────────────────────────────────────

  private byte[] convertToBinary(BsonValue bsonValue) {
    switch (bsonValue.getBsonType()) {
      case BINARY:
        return bsonValue.asBinary().getData();
      case STRING:
        return bsonValue.asString().getValue().getBytes(StandardCharsets.UTF_8);
      default:
        throw new BsonConversionException(
            "Cannot convert " + bsonValue.getBsonType() + " to binary");
    }
  }

  // ─── Timestamp (epoch microseconds) ────────────────────────────────

  private long convertToTimestamp(BsonValue bsonValue) {
    switch (bsonValue.getBsonType()) {
      case DATE_TIME:
        return bsonValue.asDateTime().getValue() * 1000L;
      case TIMESTAMP:
        return (long) bsonValue.asTimestamp().getTime() * 1000000L;
      case INT64:
        return bsonValue.asInt64().getValue() * 1000L;
      case STRING:
        Instant instant = Instant.parse(bsonValue.asString().getValue());
        return instant.getEpochSecond() * 1000000L + instant.getNano() / 1000L;
      default:
        throw new BsonConversionException(
            "Cannot convert " + bsonValue.getBsonType() + " to timestamp");
    }
  }

  // ─── Date (epoch days) ─────────────────────────────────────────────

  private int convertToDate(BsonValue bsonValue) {
    switch (bsonValue.getBsonType()) {
      case DATE_TIME:
        long millis = bsonValue.asDateTime().getValue();
        return (int) (millis / (24L * 3600L * 1000L));
      case TIMESTAMP:
        long tsSeconds = bsonValue.asTimestamp().getTime();
        return (int) (tsSeconds / (24L * 3600L));
      case STRING:
        return (int) java.time.LocalDate.parse(bsonValue.asString().getValue()).toEpochDay();
      default:
        throw new BsonConversionException("Cannot convert " + bsonValue.getBsonType() + " to date");
    }
  }

  // ─── Decimal ───────────────────────────────────────────────────────

  private org.apache.spark.sql.types.Decimal convertToDecimal(
      BsonValue bsonValue, DecimalType decimalType) {
    BigDecimal bigDecimal;
    switch (bsonValue.getBsonType()) {
      case DECIMAL128:
        bigDecimal = bsonValue.asDecimal128().getValue().bigDecimalValue();
        break;
      case DOUBLE:
        bigDecimal = BigDecimal.valueOf(bsonValue.asDouble().getValue());
        break;
      case INT32:
        bigDecimal = BigDecimal.valueOf(bsonValue.asInt32().getValue());
        break;
      case INT64:
        bigDecimal = BigDecimal.valueOf(bsonValue.asInt64().getValue());
        break;
      case STRING:
        bigDecimal = new BigDecimal(bsonValue.asString().getValue().trim());
        break;
      default:
        throw new BsonConversionException(
            "Cannot convert " + bsonValue.getBsonType() + " to decimal");
    }
    return org.apache.spark.sql.types.Decimal.apply(
        bigDecimal, decimalType.precision(), decimalType.scale());
  }

  // ─── Corrupt record handling ───────────────────────────────────────

  private InternalRow createCorruptRecordRow(BsonDocument document) {
    StructField[] fields = schema.fields();
    Object[] values = new Object[fields.length];
    Arrays.fill(values, null);

    for (int i = 0; i < fields.length; i++) {
      if (fields[i].name().equals(corruptRecordColumn)) {
        values[i] = UTF8String.fromString(document.toJson());
        break;
      }
    }
    return new GenericInternalRow(values);
  }

  // ─── DateTime formatting ───────────────────────────────────────────

  private static String formatDateTime(long epochMillis) {
    ZonedDateTime zdt = Instant.ofEpochMilli(epochMillis).atZone(ZoneOffset.UTC);
    String formatted = DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(zdt);
    if (formatted.endsWith("+00:00")) {
      formatted = formatted.substring(0, formatted.length() - 6) + "Z";
    }
    return formatted;
  }
}
