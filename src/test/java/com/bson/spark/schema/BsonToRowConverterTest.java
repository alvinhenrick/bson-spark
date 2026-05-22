package com.bson.spark.schema;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.apache.spark.unsafe.types.UTF8String;
import org.bson.BsonDocument;
import org.junit.jupiter.api.Test;

import com.bson.spark.options.BsonReadOptions;

import java.util.Collections;

class BsonToRowConverterTest {

    private static BsonReadOptions defaultOptions() {
        return new BsonReadOptions(new CaseInsensitiveStringMap(Collections.emptyMap()));
    }

    @Test
    void convertsSimpleEjsonDocument() {
        // This is what Atlas Data Federation actually outputs (line from a .json file)
        String ejson = "{"
                + "\"_id\":{\"$oid\":\"62b196c82283a3d22fddf32b\"},"
                + "\"resourceType\":\"Account\","
                + "\"id\":\"bwell-J52EeTf\","
                + "\"_sourceAssigningAuthority\":\"medstar\","
                + "\"_uuid\":\"ce4d849e-3eee-580c-9d79-d67b3cb8030f\","
                + "\"_sourceId\":\"bwell-J52EeTf\""
                + "}";

        StructType schema = new StructType(new StructField[]{
                new StructField("_id", DataTypes.StringType, false, Metadata.empty()),
                new StructField("resourceType", DataTypes.StringType, true, Metadata.empty()),
                new StructField("id", DataTypes.StringType, true, Metadata.empty()),
                new StructField("_sourceAssigningAuthority", DataTypes.StringType, true, Metadata.empty()),
                new StructField("_uuid", DataTypes.StringType, true, Metadata.empty()),
                new StructField("_sourceId", DataTypes.StringType, true, Metadata.empty()),
        });

        BsonToRowConverter converter = new BsonToRowConverter(schema, defaultOptions());
        BsonDocument document = BsonDocument.parse(ejson);
        InternalRow row = converter.convert(document);

        assertThat(row).isNotNull();
        // ObjectId becomes hex string
        assertThat(row.getUTF8String(0)).isEqualTo(UTF8String.fromString("62b196c82283a3d22fddf32b"));
        assertThat(row.getUTF8String(1)).isEqualTo(UTF8String.fromString("Account"));
        assertThat(row.getUTF8String(2)).isEqualTo(UTF8String.fromString("bwell-J52EeTf"));
        assertThat(row.getUTF8String(3)).isEqualTo(UTF8String.fromString("medstar"));
        assertThat(row.getUTF8String(4)).isEqualTo(UTF8String.fromString("ce4d849e-3eee-580c-9d79-d67b3cb8030f"));
        assertThat(row.getUTF8String(5)).isEqualTo(UTF8String.fromString("bwell-J52EeTf"));
    }

    @Test
    void convertsDateTimeFields() {
        String ejson = "{"
                + "\"_id\":{\"$oid\":\"62b196c82283a3d22fddf32b\"},"
                + "\"lastUpdated\":{\"$date\":{\"$numberLong\":\"1721469638000\"}}"
                + "}";

        StructType schema = new StructType(new StructField[]{
                new StructField("_id", DataTypes.StringType, false, Metadata.empty()),
                new StructField("lastUpdated", DataTypes.TimestampType, true, Metadata.empty()),
        });

        BsonToRowConverter converter = new BsonToRowConverter(schema, defaultOptions());
        BsonDocument document = BsonDocument.parse(ejson);
        InternalRow row = converter.convert(document);

        assertThat(row).isNotNull();
        // Timestamp stored as epoch microseconds
        long expectedMicros = 1721469638000L * 1000L;
        assertThat(row.getLong(1)).isEqualTo(expectedMicros);
    }

    @Test
    void convertsDateTimeAsStringWhenSchemaIsString() {
        String ejson = "{"
                + "\"_id\":{\"$oid\":\"62b196c82283a3d22fddf32b\"},"
                + "\"lastUpdated\":{\"$date\":{\"$numberLong\":\"1721469638000\"}}"
                + "}";

        StructType schema = new StructType(new StructField[]{
                new StructField("_id", DataTypes.StringType, false, Metadata.empty()),
                new StructField("lastUpdated", DataTypes.StringType, true, Metadata.empty()),
        });

        BsonToRowConverter converter = new BsonToRowConverter(schema, defaultOptions());
        BsonDocument document = BsonDocument.parse(ejson);
        InternalRow row = converter.convert(document);

        assertThat(row).isNotNull();
        // DateTime rendered as ISO string when schema declares StringType
        String dateStr = row.getUTF8String(1).toString();
        assertThat(dateStr).contains("2024-07-20");
        assertThat(dateStr).endsWith("Z");
    }

    @Test
    void convertsNumberIntField() {
        String ejson = "{"
                + "\"_id\":{\"$oid\":\"62b196c82283a3d22fddf32b\"},"
                + "\"count\":{\"$numberInt\":\"42\"}"
                + "}";

        StructType schema = new StructType(new StructField[]{
                new StructField("_id", DataTypes.StringType, false, Metadata.empty()),
                new StructField("count", DataTypes.IntegerType, true, Metadata.empty()),
        });

        BsonToRowConverter converter = new BsonToRowConverter(schema, defaultOptions());
        BsonDocument document = BsonDocument.parse(ejson);
        InternalRow row = converter.convert(document);

        assertThat(row).isNotNull();
        assertThat(row.getInt(1)).isEqualTo(42);
    }

    @Test
    void convertsMapField() {
        // This matches _access field from your federation output: {"medstar": {"$numberInt": "1"}}
        String ejson = "{"
                + "\"_id\":{\"$oid\":\"62b196c82283a3d22fddf32b\"},"
                + "\"_access\":{\"medstar\":{\"$numberInt\":\"1\"}}"
                + "}";

        StructType schema = new StructType(new StructField[]{
                new StructField("_id", DataTypes.StringType, false, Metadata.empty()),
                new StructField("_access",
                        DataTypes.createMapType(DataTypes.StringType, DataTypes.IntegerType, true),
                        true, Metadata.empty()),
        });

        BsonToRowConverter converter = new BsonToRowConverter(schema, defaultOptions());
        BsonDocument document = BsonDocument.parse(ejson);
        InternalRow row = converter.convert(document);

        assertThat(row).isNotNull();
        assertThat(row.isNullAt(1)).isFalse();
    }

    @Test
    void handlesNullableFieldsMissing() {
        String ejson = "{"
                + "\"_id\":{\"$oid\":\"62b196c82283a3d22fddf32b\"}"
                + "}";

        StructType schema = new StructType(new StructField[]{
                new StructField("_id", DataTypes.StringType, false, Metadata.empty()),
                new StructField("name", DataTypes.StringType, true, Metadata.empty()),
                new StructField("status", DataTypes.StringType, true, Metadata.empty()),
        });

        BsonToRowConverter converter = new BsonToRowConverter(schema, defaultOptions());
        BsonDocument document = BsonDocument.parse(ejson);
        InternalRow row = converter.convert(document);

        assertThat(row).isNotNull();
        assertThat(row.getUTF8String(0)).isEqualTo(UTF8String.fromString("62b196c82283a3d22fddf32b"));
        assertThat(row.isNullAt(1)).isTrue();
        assertThat(row.isNullAt(2)).isTrue();
    }

    @Test
    void corruptRecordCapturedOnError() {
        String ejson = "{"
                + "\"_id\":{\"$oid\":\"62b196c82283a3d22fddf32b\"},"
                + "\"count\":\"not_a_number\""
                + "}";

        StructType schema = new StructType(new StructField[]{
                new StructField("_id", DataTypes.StringType, false, Metadata.empty()),
                new StructField("count", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("_error", DataTypes.StringType, true, Metadata.empty()),
        });

        java.util.Map<String, String> opts = new java.util.HashMap<>();
        opts.put("columnNameOfCorruptRecord", "_error");
        BsonReadOptions options = new BsonReadOptions(new CaseInsensitiveStringMap(opts));

        BsonToRowConverter converter = new BsonToRowConverter(schema, options);
        BsonDocument document = BsonDocument.parse(ejson);
        InternalRow row = converter.convert(document);

        assertThat(row).isNotNull();
        // The corrupt record column should contain the original document JSON
        assertThat(row.isNullAt(2)).isFalse();
        String errorContent = row.getUTF8String(2).toString();
        assertThat(errorContent).contains("62b196c82283a3d22fddf32b");
    }

    @Test
    void convertsNestedStruct() {
        String ejson = "{"
                + "\"_id\":{\"$oid\":\"62b196c82283a3d22fddf32b\"},"
                + "\"meta\":{\"versionId\":\"2\",\"lastUpdated\":{\"$date\":{\"$numberLong\":\"1721469638000\"}}}"
                + "}";

        StructType metaSchema = new StructType(new StructField[]{
                new StructField("versionId", DataTypes.StringType, true, Metadata.empty()),
                new StructField("lastUpdated", DataTypes.StringType, true, Metadata.empty()),
        });

        StructType schema = new StructType(new StructField[]{
                new StructField("_id", DataTypes.StringType, false, Metadata.empty()),
                new StructField("meta", metaSchema, true, Metadata.empty()),
        });

        BsonToRowConverter converter = new BsonToRowConverter(schema, defaultOptions());
        BsonDocument document = BsonDocument.parse(ejson);
        InternalRow row = converter.convert(document);

        assertThat(row).isNotNull();
        InternalRow metaRow = row.getStruct(1, 2);
        assertThat(metaRow.getUTF8String(0)).isEqualTo(UTF8String.fromString("2"));
        // lastUpdated is StringType in schema, so DateTime becomes ISO string
        String lastUpdated = metaRow.getUTF8String(1).toString();
        assertThat(lastUpdated).contains("2024-07-20");
    }

    @Test
    void convertsArrayOfStructs() {
        String ejson = "{"
                + "\"_id\":{\"$oid\":\"62b196c82283a3d22fddf32b\"},"
                + "\"identifier\":[{\"system\":\"https://www.icanbwell.com/sourceId\",\"value\":\"bwell-J52EeTf\"}]"
                + "}";

        StructType identifierSchema = new StructType(new StructField[]{
                new StructField("system", DataTypes.StringType, true, Metadata.empty()),
                new StructField("value", DataTypes.StringType, true, Metadata.empty()),
        });

        StructType schema = new StructType(new StructField[]{
                new StructField("_id", DataTypes.StringType, false, Metadata.empty()),
                new StructField("identifier",
                        DataTypes.createArrayType(identifierSchema, true),
                        true, Metadata.empty()),
        });

        BsonToRowConverter converter = new BsonToRowConverter(schema, defaultOptions());
        BsonDocument document = BsonDocument.parse(ejson);
        InternalRow row = converter.convert(document);

        assertThat(row).isNotNull();
        assertThat(row.isNullAt(1)).isFalse();
    }
}
