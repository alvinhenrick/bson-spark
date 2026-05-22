package com.bson.spark.schema;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.util.ArrayBasedMapData;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.apache.spark.unsafe.types.UTF8String;
import org.bson.BsonDocument;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import com.bson.spark.options.BsonReadOptions;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

class BsonToRowConverterTest {

    private static BsonReadOptions defaultOptions() {
        return new BsonReadOptions(new CaseInsensitiveStringMap(Collections.emptyMap()));
    }

    private static BsonReadOptions optionsWithCorruptColumn() {
        Map<String, String> opts = new HashMap<>();
        opts.put("columnNameOfCorruptRecord", "_error");
        return new BsonReadOptions(new CaseInsensitiveStringMap(opts));
    }

    // ─── Real EJSON from Atlas Data Federation ─────────────────────────

    @Nested
    class RealFederationOutput {

        @Test
        void convertsActualFederationDocument() {
            // Actual line from /Volumes/bronze/fhir_lake/atlas_data_federation/Account_4_0_0/
            String ejson = "{"
                    + "\"_id\":{\"$oid\":\"62b196c82283a3d22fddf32b\"},"
                    + "\"resourceType\":\"Account\","
                    + "\"id\":\"bwell-J52EeTf\","
                    + "\"meta\":{\"versionId\":\"2\",\"lastUpdated\":{\"$date\":{\"$numberLong\":\"1721469638000\"}},"
                    + "\"source\":\"https://www.icanbwell.com/platform\","
                    + "\"security\":[{\"system\":\"https://www.icanbwell.com/access\",\"code\":\"medstar\"}]},"
                    + "\"status\":\"active\","
                    + "\"_access\":{\"medstar\":{\"$numberInt\":\"1\"}},"
                    + "\"_sourceAssigningAuthority\":\"medstar\","
                    + "\"_uuid\":\"ce4d849e-3eee-580c-9d79-d67b3cb8030f\","
                    + "\"_sourceId\":\"bwell-J52EeTf\""
                    + "}";

            StructType securitySchema = new StructType(new StructField[]{
                    new StructField("system", DataTypes.StringType, true, Metadata.empty()),
                    new StructField("code", DataTypes.StringType, true, Metadata.empty()),
            });

            StructType metaSchema = new StructType(new StructField[]{
                    new StructField("versionId", DataTypes.StringType, true, Metadata.empty()),
                    new StructField("lastUpdated", DataTypes.StringType, true, Metadata.empty()),
                    new StructField("source", DataTypes.StringType, true, Metadata.empty()),
                    new StructField("security", DataTypes.createArrayType(securitySchema, true), true, Metadata.empty()),
            });

            StructType schema = new StructType(new StructField[]{
                    new StructField("_id", DataTypes.StringType, false, Metadata.empty()),
                    new StructField("resourceType", DataTypes.StringType, true, Metadata.empty()),
                    new StructField("id", DataTypes.StringType, true, Metadata.empty()),
                    new StructField("meta", metaSchema, true, Metadata.empty()),
                    new StructField("status", DataTypes.StringType, true, Metadata.empty()),
                    new StructField("_access", DataTypes.createMapType(DataTypes.StringType, DataTypes.IntegerType, true), true, Metadata.empty()),
                    new StructField("_sourceAssigningAuthority", DataTypes.StringType, true, Metadata.empty()),
                    new StructField("_uuid", DataTypes.StringType, true, Metadata.empty()),
                    new StructField("_sourceId", DataTypes.StringType, true, Metadata.empty()),
            });

            BsonToRowConverter converter = new BsonToRowConverter(schema, defaultOptions());
            BsonDocument document = BsonDocument.parse(ejson);
            InternalRow row = converter.convert(document);

            assertThat(row).isNotNull();
            assertThat(row.getUTF8String(0)).isEqualTo(UTF8String.fromString("62b196c82283a3d22fddf32b"));
            assertThat(row.getUTF8String(1)).isEqualTo(UTF8String.fromString("Account"));
            assertThat(row.getUTF8String(2)).isEqualTo(UTF8String.fromString("bwell-J52EeTf"));
            assertThat(row.getUTF8String(4)).isEqualTo(UTF8String.fromString("active"));
            assertThat(row.getUTF8String(6)).isEqualTo(UTF8String.fromString("medstar"));
            assertThat(row.getUTF8String(7)).isEqualTo(UTF8String.fromString("ce4d849e-3eee-580c-9d79-d67b3cb8030f"));
            assertThat(row.getUTF8String(8)).isEqualTo(UTF8String.fromString("bwell-J52EeTf"));

            // Verify nested meta struct
            InternalRow metaRow = row.getStruct(3, 4);
            assertThat(metaRow.getUTF8String(0)).isEqualTo(UTF8String.fromString("2"));
            String lastUpdated = metaRow.getUTF8String(1).toString();
            assertThat(lastUpdated).contains("2024-07-20");
            assertThat(lastUpdated).endsWith("Z");
            assertThat(metaRow.getUTF8String(2)).isEqualTo(UTF8String.fromString("https://www.icanbwell.com/platform"));

            // Verify security array
            ArrayData securityArray = metaRow.getArray(3);
            assertThat(securityArray.numElements()).isEqualTo(1);
            InternalRow securityRow = securityArray.getStruct(0, 2);
            assertThat(securityRow.getUTF8String(0)).isEqualTo(UTF8String.fromString("https://www.icanbwell.com/access"));
            assertThat(securityRow.getUTF8String(1)).isEqualTo(UTF8String.fromString("medstar"));

            // Verify _access map
            assertThat(row.isNullAt(5)).isFalse();
        }

        @Test
        void handlesDocumentWithExtensionArray() {
            String ejson = "{"
                    + "\"_id\":{\"$oid\":\"62b196c82283a3d22fddf32b\"},"
                    + "\"extension\":[{\"valueDateTime\":\"2022-06-20T17:18:38.704967Z\"}]"
                    + "}";

            StructType extensionSchema = new StructType(new StructField[]{
                    new StructField("valueDateTime", DataTypes.StringType, true, Metadata.empty()),
            });

            StructType schema = new StructType(new StructField[]{
                    new StructField("_id", DataTypes.StringType, false, Metadata.empty()),
                    new StructField("extension", DataTypes.createArrayType(extensionSchema, true), true, Metadata.empty()),
            });

            BsonToRowConverter converter = new BsonToRowConverter(schema, defaultOptions());
            InternalRow row = converter.convert(BsonDocument.parse(ejson));

            assertThat(row).isNotNull();
            ArrayData extensions = row.getArray(1);
            assertThat(extensions.numElements()).isEqualTo(1);
            InternalRow ext = extensions.getStruct(0, 1);
            assertThat(ext.getUTF8String(0)).isEqualTo(UTF8String.fromString("2022-06-20T17:18:38.704967Z"));
        }

        @Test
        void handlesSubjectArrayWithNestedReferences() {
            String ejson = "{"
                    + "\"_id\":{\"$oid\":\"62b196c82283a3d22fddf32b\"},"
                    + "\"subject\":[{"
                    + "\"reference\":\"Patient/000129538\","
                    + "\"_sourceAssigningAuthority\":\"medstar\","
                    + "\"_uuid\":\"Patient/6917646a-941d-5a75-b229-202eae005a99\","
                    + "\"_sourceId\":\"Patient/000129538\""
                    + "}]"
                    + "}";

            StructType subjectSchema = new StructType(new StructField[]{
                    new StructField("reference", DataTypes.StringType, true, Metadata.empty()),
                    new StructField("_sourceAssigningAuthority", DataTypes.StringType, true, Metadata.empty()),
                    new StructField("_uuid", DataTypes.StringType, true, Metadata.empty()),
                    new StructField("_sourceId", DataTypes.StringType, true, Metadata.empty()),
            });

            StructType schema = new StructType(new StructField[]{
                    new StructField("_id", DataTypes.StringType, false, Metadata.empty()),
                    new StructField("subject", DataTypes.createArrayType(subjectSchema, true), true, Metadata.empty()),
            });

            BsonToRowConverter converter = new BsonToRowConverter(schema, defaultOptions());
            InternalRow row = converter.convert(BsonDocument.parse(ejson));

            assertThat(row).isNotNull();
            ArrayData subjects = row.getArray(1);
            assertThat(subjects.numElements()).isEqualTo(1);
            InternalRow subject = subjects.getStruct(0, 4);
            assertThat(subject.getUTF8String(0)).isEqualTo(UTF8String.fromString("Patient/000129538"));
            assertThat(subject.getUTF8String(1)).isEqualTo(UTF8String.fromString("medstar"));
            assertThat(subject.getUTF8String(2)).isEqualTo(UTF8String.fromString("Patient/6917646a-941d-5a75-b229-202eae005a99"));
            assertThat(subject.getUTF8String(3)).isEqualTo(UTF8String.fromString("Patient/000129538"));
        }
    }

    // ─── EJSON Type Conversions ────────────────────────────────────────

    @Nested
    class EjsonTypeConversions {

        @Test
        void convertsObjectId() {
            String ejson = "{\"_id\":{\"$oid\":\"507f1f77bcf86cd799439011\"}}";
            StructType schema = new StructType(new StructField[]{
                    new StructField("_id", DataTypes.StringType, false, Metadata.empty()),
            });

            InternalRow row = new BsonToRowConverter(schema, defaultOptions())
                    .convert(BsonDocument.parse(ejson));

            assertThat(row).isNotNull();
            assertThat(row.getUTF8String(0)).isEqualTo(UTF8String.fromString("507f1f77bcf86cd799439011"));
        }

        @Test
        void convertsNumberInt() {
            String ejson = "{\"count\":{\"$numberInt\":\"42\"}}";
            StructType schema = new StructType(new StructField[]{
                    new StructField("count", DataTypes.IntegerType, true, Metadata.empty()),
            });

            InternalRow row = new BsonToRowConverter(schema, defaultOptions())
                    .convert(BsonDocument.parse(ejson));

            assertThat(row).isNotNull();
            assertThat(row.getInt(0)).isEqualTo(42);
        }

        @Test
        void convertsNumberLong() {
            String ejson = "{\"ts\":{\"$numberLong\":\"9876543210\"}}";
            StructType schema = new StructType(new StructField[]{
                    new StructField("ts", DataTypes.LongType, true, Metadata.empty()),
            });

            InternalRow row = new BsonToRowConverter(schema, defaultOptions())
                    .convert(BsonDocument.parse(ejson));

            assertThat(row).isNotNull();
            assertThat(row.getLong(0)).isEqualTo(9876543210L);
        }

        @Test
        void convertsNumberDouble() {
            String ejson = "{\"score\":{\"$numberDouble\":\"3.14\"}}";
            StructType schema = new StructType(new StructField[]{
                    new StructField("score", DataTypes.DoubleType, true, Metadata.empty()),
            });

            InternalRow row = new BsonToRowConverter(schema, defaultOptions())
                    .convert(BsonDocument.parse(ejson));

            assertThat(row).isNotNull();
            assertThat(row.getDouble(0)).isEqualTo(3.14);
        }

        @Test
        void convertsDecimal128() {
            String ejson = "{\"amount\":{\"$numberDecimal\":\"123.456\"}}";
            StructType schema = new StructType(new StructField[]{
                    new StructField("amount", DataTypes.StringType, true, Metadata.empty()),
            });

            InternalRow row = new BsonToRowConverter(schema, defaultOptions())
                    .convert(BsonDocument.parse(ejson));

            assertThat(row).isNotNull();
            assertThat(row.getUTF8String(0)).isEqualTo(UTF8String.fromString("123.456"));
        }

        @Test
        void convertsDecimal128ToDecimalType() {
            String ejson = "{\"amount\":{\"$numberDecimal\":\"123.456\"}}";
            StructType schema = new StructType(new StructField[]{
                    new StructField("amount", DataTypes.createDecimalType(10, 3), true, Metadata.empty()),
            });

            InternalRow row = new BsonToRowConverter(schema, defaultOptions())
                    .convert(BsonDocument.parse(ejson));

            assertThat(row).isNotNull();
            assertThat(row.getDecimal(0, 10, 3).toBigDecimal().bigDecimal())
                    .isEqualByComparingTo("123.456");
        }

        @Test
        void convertsDateTime() {
            String ejson = "{\"created\":{\"$date\":{\"$numberLong\":\"1609459200000\"}}}";
            StructType schema = new StructType(new StructField[]{
                    new StructField("created", DataTypes.TimestampType, true, Metadata.empty()),
            });

            InternalRow row = new BsonToRowConverter(schema, defaultOptions())
                    .convert(BsonDocument.parse(ejson));

            assertThat(row).isNotNull();
            // 1609459200000 ms = 1609459200000000 microseconds
            assertThat(row.getLong(0)).isEqualTo(1609459200000L * 1000L);
        }

        @Test
        void convertsDateTimeToString() {
            String ejson = "{\"created\":{\"$date\":{\"$numberLong\":\"1609459200000\"}}}";
            StructType schema = new StructType(new StructField[]{
                    new StructField("created", DataTypes.StringType, true, Metadata.empty()),
            });

            InternalRow row = new BsonToRowConverter(schema, defaultOptions())
                    .convert(BsonDocument.parse(ejson));

            assertThat(row).isNotNull();
            String dateStr = row.getUTF8String(0).toString();
            assertThat(dateStr).isEqualTo("2021-01-01T00:00:00Z");
        }

        @Test
        void convertsBooleanTrue() {
            String ejson = "{\"active\":true}";
            StructType schema = new StructType(new StructField[]{
                    new StructField("active", DataTypes.BooleanType, true, Metadata.empty()),
            });

            InternalRow row = new BsonToRowConverter(schema, defaultOptions())
                    .convert(BsonDocument.parse(ejson));

            assertThat(row).isNotNull();
            assertThat(row.getBoolean(0)).isTrue();
        }

        @Test
        void convertsBooleanFalse() {
            String ejson = "{\"active\":false}";
            StructType schema = new StructType(new StructField[]{
                    new StructField("active", DataTypes.BooleanType, true, Metadata.empty()),
            });

            InternalRow row = new BsonToRowConverter(schema, defaultOptions())
                    .convert(BsonDocument.parse(ejson));

            assertThat(row).isNotNull();
            assertThat(row.getBoolean(0)).isFalse();
        }

        @Test
        void convertsBinaryToBase64String() {
            // Binary with base64 "aGVsbG8=" = "hello"
            String ejson = "{\"data\":{\"$binary\":{\"base64\":\"aGVsbG8=\",\"subType\":\"00\"}}}";
            StructType schema = new StructType(new StructField[]{
                    new StructField("data", DataTypes.StringType, true, Metadata.empty()),
            });

            InternalRow row = new BsonToRowConverter(schema, defaultOptions())
                    .convert(BsonDocument.parse(ejson));

            assertThat(row).isNotNull();
            assertThat(row.getUTF8String(0)).isEqualTo(UTF8String.fromString("aGVsbG8="));
        }

        @Test
        void convertsBinaryToBinaryType() {
            String ejson = "{\"data\":{\"$binary\":{\"base64\":\"aGVsbG8=\",\"subType\":\"00\"}}}";
            StructType schema = new StructType(new StructField[]{
                    new StructField("data", DataTypes.BinaryType, true, Metadata.empty()),
            });

            InternalRow row = new BsonToRowConverter(schema, defaultOptions())
                    .convert(BsonDocument.parse(ejson));

            assertThat(row).isNotNull();
            assertThat(row.getBinary(0)).isEqualTo("hello".getBytes());
        }

        @Test
        void convertsRegex() {
            String ejson = "{\"pattern\":{\"$regularExpression\":{\"pattern\":\"^abc\",\"options\":\"i\"}}}";
            StructType schema = new StructType(new StructField[]{
                    new StructField("pattern", DataTypes.StringType, true, Metadata.empty()),
            });

            InternalRow row = new BsonToRowConverter(schema, defaultOptions())
                    .convert(BsonDocument.parse(ejson));

            assertThat(row).isNotNull();
            assertThat(row.getUTF8String(0)).isEqualTo(UTF8String.fromString("^abc"));
        }

        @ParameterizedTest
        @CsvSource({
                "0, 0",
                "42, 42",
                "-1, -1",
                "2147483647, 2147483647",
        })
        void convertsNumberIntVariants(String input, int expected) {
            String ejson = "{\"val\":{\"$numberInt\":\"" + input + "\"}}";
            StructType schema = new StructType(new StructField[]{
                    new StructField("val", DataTypes.IntegerType, true, Metadata.empty()),
            });

            InternalRow row = new BsonToRowConverter(schema, defaultOptions())
                    .convert(BsonDocument.parse(ejson));

            assertThat(row).isNotNull();
            assertThat(row.getInt(0)).isEqualTo(expected);
        }
    }

    // ─── Null and Missing Fields ───────────────────────────────────────

    @Nested
    class NullHandling {

        @Test
        void handlesExplicitNull() {
            String ejson = "{\"_id\":{\"$oid\":\"507f1f77bcf86cd799439011\"},\"name\":null}";
            StructType schema = new StructType(new StructField[]{
                    new StructField("_id", DataTypes.StringType, false, Metadata.empty()),
                    new StructField("name", DataTypes.StringType, true, Metadata.empty()),
            });

            InternalRow row = new BsonToRowConverter(schema, defaultOptions())
                    .convert(BsonDocument.parse(ejson));

            assertThat(row).isNotNull();
            assertThat(row.isNullAt(1)).isTrue();
        }

        @Test
        void handlesMissingNullableFields() {
            String ejson = "{\"_id\":{\"$oid\":\"507f1f77bcf86cd799439011\"}}";
            StructType schema = new StructType(new StructField[]{
                    new StructField("_id", DataTypes.StringType, false, Metadata.empty()),
                    new StructField("name", DataTypes.StringType, true, Metadata.empty()),
                    new StructField("status", DataTypes.StringType, true, Metadata.empty()),
                    new StructField("count", DataTypes.IntegerType, true, Metadata.empty()),
            });

            InternalRow row = new BsonToRowConverter(schema, defaultOptions())
                    .convert(BsonDocument.parse(ejson));

            assertThat(row).isNotNull();
            assertThat(row.getUTF8String(0)).isEqualTo(UTF8String.fromString("507f1f77bcf86cd799439011"));
            assertThat(row.isNullAt(1)).isTrue();
            assertThat(row.isNullAt(2)).isTrue();
            assertThat(row.isNullAt(3)).isTrue();
        }

        @Test
        void handlesNullInArray() {
            String ejson = "{\"items\":[\"a\",null,\"b\"]}";
            StructType schema = new StructType(new StructField[]{
                    new StructField("items", DataTypes.createArrayType(DataTypes.StringType, true), true, Metadata.empty()),
            });

            InternalRow row = new BsonToRowConverter(schema, defaultOptions())
                    .convert(BsonDocument.parse(ejson));

            assertThat(row).isNotNull();
            ArrayData arr = row.getArray(0);
            assertThat(arr.numElements()).isEqualTo(3);
            assertThat(arr.getUTF8String(0)).isEqualTo(UTF8String.fromString("a"));
            assertThat(arr.isNullAt(1)).isTrue();
            assertThat(arr.getUTF8String(2)).isEqualTo(UTF8String.fromString("b"));
        }

        @Test
        void handlesNullInMap() {
            String ejson = "{\"m\":{\"a\":{\"$numberInt\":\"1\"},\"b\":null}}";
            StructType schema = new StructType(new StructField[]{
                    new StructField("m", DataTypes.createMapType(DataTypes.StringType, DataTypes.IntegerType, true), true, Metadata.empty()),
            });

            InternalRow row = new BsonToRowConverter(schema, defaultOptions())
                    .convert(BsonDocument.parse(ejson));

            assertThat(row).isNotNull();
            assertThat(row.isNullAt(0)).isFalse();
        }

        @Test
        void handlesEmptyArray() {
            String ejson = "{\"items\":[]}";
            StructType schema = new StructType(new StructField[]{
                    new StructField("items", DataTypes.createArrayType(DataTypes.StringType, true), true, Metadata.empty()),
            });

            InternalRow row = new BsonToRowConverter(schema, defaultOptions())
                    .convert(BsonDocument.parse(ejson));

            assertThat(row).isNotNull();
            ArrayData arr = row.getArray(0);
            assertThat(arr.numElements()).isEqualTo(0);
        }

        @Test
        void handlesEmptyDocument() {
            String ejson = "{\"nested\":{}}";
            StructType nestedSchema = new StructType(new StructField[]{
                    new StructField("field1", DataTypes.StringType, true, Metadata.empty()),
            });
            StructType schema = new StructType(new StructField[]{
                    new StructField("nested", nestedSchema, true, Metadata.empty()),
            });

            InternalRow row = new BsonToRowConverter(schema, defaultOptions())
                    .convert(BsonDocument.parse(ejson));

            assertThat(row).isNotNull();
            InternalRow nested = row.getStruct(0, 1);
            assertThat(nested.isNullAt(0)).isTrue();
        }
    }

    // ─── Corrupt Record Handling ───────────────────────────────────────

    @Nested
    class CorruptRecordHandling {

        @Test
        void capturesCorruptDocumentInErrorColumn() {
            String ejson = "{\"_id\":{\"$oid\":\"507f1f77bcf86cd799439011\"},\"count\":\"not_a_number\"}";
            StructType schema = new StructType(new StructField[]{
                    new StructField("_id", DataTypes.StringType, false, Metadata.empty()),
                    new StructField("count", DataTypes.IntegerType, true, Metadata.empty()),
                    new StructField("_error", DataTypes.StringType, true, Metadata.empty()),
            });

            BsonToRowConverter converter = new BsonToRowConverter(schema, optionsWithCorruptColumn());
            InternalRow row = converter.convert(BsonDocument.parse(ejson));

            assertThat(row).isNotNull();
            assertThat(row.isNullAt(2)).isFalse();
            String errorContent = row.getUTF8String(2).toString();
            assertThat(errorContent).contains("507f1f77bcf86cd799439011");
            assertThat(errorContent).contains("not_a_number");
        }

        @Test
        void returnsNullWithoutCorruptColumn() {
            String ejson = "{\"_id\":{\"$oid\":\"507f1f77bcf86cd799439011\"},\"count\":\"bad\"}";
            StructType schema = new StructType(new StructField[]{
                    new StructField("_id", DataTypes.StringType, false, Metadata.empty()),
                    new StructField("count", DataTypes.IntegerType, true, Metadata.empty()),
            });

            BsonToRowConverter converter = new BsonToRowConverter(schema, defaultOptions());
            InternalRow row = converter.convert(BsonDocument.parse(ejson));

            assertThat(row).isNull();
        }

        @Test
        void normalRowsHaveNullErrorColumn() {
            String ejson = "{\"_id\":{\"$oid\":\"507f1f77bcf86cd799439011\"},\"count\":{\"$numberInt\":\"5\"}}";
            StructType schema = new StructType(new StructField[]{
                    new StructField("_id", DataTypes.StringType, false, Metadata.empty()),
                    new StructField("count", DataTypes.IntegerType, true, Metadata.empty()),
                    new StructField("_error", DataTypes.StringType, true, Metadata.empty()),
            });

            BsonToRowConverter converter = new BsonToRowConverter(schema, optionsWithCorruptColumn());
            InternalRow row = converter.convert(BsonDocument.parse(ejson));

            assertThat(row).isNotNull();
            assertThat(row.getInt(1)).isEqualTo(5);
            assertThat(row.isNullAt(2)).isTrue();
        }
    }

    // ─── Type Coercion Edge Cases ──────────────────────────────────────

    @Nested
    class TypeCoercion {

        @Test
        void intToLongPromotion() {
            String ejson = "{\"val\":{\"$numberInt\":\"42\"}}";
            StructType schema = new StructType(new StructField[]{
                    new StructField("val", DataTypes.LongType, true, Metadata.empty()),
            });

            InternalRow row = new BsonToRowConverter(schema, defaultOptions())
                    .convert(BsonDocument.parse(ejson));

            assertThat(row).isNotNull();
            assertThat(row.getLong(0)).isEqualTo(42L);
        }

        @Test
        void intToDoublePromotion() {
            String ejson = "{\"val\":{\"$numberInt\":\"42\"}}";
            StructType schema = new StructType(new StructField[]{
                    new StructField("val", DataTypes.DoubleType, true, Metadata.empty()),
            });

            InternalRow row = new BsonToRowConverter(schema, defaultOptions())
                    .convert(BsonDocument.parse(ejson));

            assertThat(row).isNotNull();
            assertThat(row.getDouble(0)).isEqualTo(42.0);
        }

        @Test
        void longToDoublePromotion() {
            String ejson = "{\"val\":{\"$numberLong\":\"9876543210\"}}";
            StructType schema = new StructType(new StructField[]{
                    new StructField("val", DataTypes.DoubleType, true, Metadata.empty()),
            });

            InternalRow row = new BsonToRowConverter(schema, defaultOptions())
                    .convert(BsonDocument.parse(ejson));

            assertThat(row).isNotNull();
            assertThat(row.getDouble(0)).isEqualTo(9876543210.0);
        }

        @Test
        void numberIntToString() {
            String ejson = "{\"val\":{\"$numberInt\":\"42\"}}";
            StructType schema = new StructType(new StructField[]{
                    new StructField("val", DataTypes.StringType, true, Metadata.empty()),
            });

            InternalRow row = new BsonToRowConverter(schema, defaultOptions())
                    .convert(BsonDocument.parse(ejson));

            assertThat(row).isNotNull();
            assertThat(row.getUTF8String(0)).isEqualTo(UTF8String.fromString("42"));
        }

        @Test
        void numberLongToString() {
            String ejson = "{\"val\":{\"$numberLong\":\"9876543210\"}}";
            StructType schema = new StructType(new StructField[]{
                    new StructField("val", DataTypes.StringType, true, Metadata.empty()),
            });

            InternalRow row = new BsonToRowConverter(schema, defaultOptions())
                    .convert(BsonDocument.parse(ejson));

            assertThat(row).isNotNull();
            assertThat(row.getUTF8String(0)).isEqualTo(UTF8String.fromString("9876543210"));
        }

        @Test
        void nestedDocumentAsString() {
            String ejson = "{\"data\":{\"key\":\"value\",\"num\":{\"$numberInt\":\"1\"}}}";
            StructType schema = new StructType(new StructField[]{
                    new StructField("data", DataTypes.StringType, true, Metadata.empty()),
            });

            InternalRow row = new BsonToRowConverter(schema, defaultOptions())
                    .convert(BsonDocument.parse(ejson));

            assertThat(row).isNotNull();
            String json = row.getUTF8String(0).toString();
            assertThat(json).contains("key");
            assertThat(json).contains("value");
        }
    }

    // ─── Map conversions ───────────────────────────────────────────────

    @Nested
    class MapConversions {

        @Test
        void convertsAccessMapWithIntValues() {
            String ejson = "{\"_access\":{\"medstar\":{\"$numberInt\":\"1\"},\"bwell\":{\"$numberInt\":\"2\"}}}";
            StructType schema = new StructType(new StructField[]{
                    new StructField("_access", DataTypes.createMapType(DataTypes.StringType, DataTypes.IntegerType, true), true, Metadata.empty()),
            });

            InternalRow row = new BsonToRowConverter(schema, defaultOptions())
                    .convert(BsonDocument.parse(ejson));

            assertThat(row).isNotNull();
            assertThat(row.isNullAt(0)).isFalse();
            ArrayBasedMapData map = (ArrayBasedMapData) row.getMap(0);
            assertThat(map.numElements()).isEqualTo(2);
        }

        @Test
        void convertsEmptyMap() {
            String ejson = "{\"_access\":{}}";
            StructType schema = new StructType(new StructField[]{
                    new StructField("_access", DataTypes.createMapType(DataTypes.StringType, DataTypes.IntegerType, true), true, Metadata.empty()),
            });

            InternalRow row = new BsonToRowConverter(schema, defaultOptions())
                    .convert(BsonDocument.parse(ejson));

            assertThat(row).isNotNull();
            ArrayBasedMapData map = (ArrayBasedMapData) row.getMap(0);
            assertThat(map.numElements()).isEqualTo(0);
        }
    }

    // ─── Deeply nested structures ──────────────────────────────────────

    @Nested
    class DeepNesting {

        @Test
        void handlesThreeLevelsDeep() {
            String ejson = "{\"a\":{\"b\":{\"c\":\"deep\"}}}";
            StructType cSchema = new StructType(new StructField[]{
                    new StructField("c", DataTypes.StringType, true, Metadata.empty()),
            });
            StructType bSchema = new StructType(new StructField[]{
                    new StructField("b", cSchema, true, Metadata.empty()),
            });
            StructType schema = new StructType(new StructField[]{
                    new StructField("a", bSchema, true, Metadata.empty()),
            });

            InternalRow row = new BsonToRowConverter(schema, defaultOptions())
                    .convert(BsonDocument.parse(ejson));

            assertThat(row).isNotNull();
            InternalRow a = row.getStruct(0, 1);
            InternalRow b = a.getStruct(0, 1);
            assertThat(b.getUTF8String(0)).isEqualTo(UTF8String.fromString("deep"));
        }

        @Test
        void handlesArrayOfArrays() {
            String ejson = "{\"matrix\":[[{\"$numberInt\":\"1\"},{\"$numberInt\":\"2\"}],[{\"$numberInt\":\"3\"}]]}";
            StructType schema = new StructType(new StructField[]{
                    new StructField("matrix",
                            DataTypes.createArrayType(
                                    DataTypes.createArrayType(DataTypes.IntegerType, true), true),
                            true, Metadata.empty()),
            });

            InternalRow row = new BsonToRowConverter(schema, defaultOptions())
                    .convert(BsonDocument.parse(ejson));

            assertThat(row).isNotNull();
            ArrayData outer = row.getArray(0);
            assertThat(outer.numElements()).isEqualTo(2);
            ArrayData inner0 = outer.getArray(0);
            assertThat(inner0.numElements()).isEqualTo(2);
            assertThat(inner0.getInt(0)).isEqualTo(1);
            assertThat(inner0.getInt(1)).isEqualTo(2);
        }
    }

    // ─── Extra fields in document not in schema (ignored) ──────────────

    @Nested
    class ExtraFields {

        @Test
        void ignoresFieldsNotInSchema() {
            String ejson = "{"
                    + "\"_id\":{\"$oid\":\"507f1f77bcf86cd799439011\"},"
                    + "\"known\":\"value\","
                    + "\"unknown_field\":\"should_be_ignored\","
                    + "\"another_extra\":{\"$numberInt\":\"99\"}"
                    + "}";
            StructType schema = new StructType(new StructField[]{
                    new StructField("_id", DataTypes.StringType, false, Metadata.empty()),
                    new StructField("known", DataTypes.StringType, true, Metadata.empty()),
            });

            InternalRow row = new BsonToRowConverter(schema, defaultOptions())
                    .convert(BsonDocument.parse(ejson));

            assertThat(row).isNotNull();
            assertThat(row.getUTF8String(0)).isEqualTo(UTF8String.fromString("507f1f77bcf86cd799439011"));
            assertThat(row.getUTF8String(1)).isEqualTo(UTF8String.fromString("value"));
        }
    }
}
