package com.bson.spark;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class BsonDataSourceEndToEndTest {

    private static SparkSession spark;

    @BeforeAll
    static void startSpark() {
        spark = SparkSession.builder()
                .master("local[*]")
                .appName("BsonDataSourceEndToEndTest")
                .config("spark.ui.enabled", "false")
                .config("spark.driver.bindAddress", "127.0.0.1")
                .getOrCreate();
    }

    @AfterAll
    static void stopSpark() {
        if (spark != null) {
            spark.stop();
        }
    }

    private static String testResource(String relativePath) {
        return BsonDataSourceEndToEndTest.class
                .getClassLoader()
                .getResource("ejson/" + relativePath)
                .getPath();
    }

    // ─── All BSON Types ───────────────────────────────────────────────

    @Nested
    class AllTypesTest {

        private Dataset<Row> loadAllTypes() {
            StructType nestedDocSchema = new StructType(new StructField[] {
                new StructField("name", DataTypes.StringType, true, Metadata.empty()),
                new StructField("value", DataTypes.IntegerType, true, Metadata.empty()),
            });

            StructType schema = new StructType(new StructField[] {
                new StructField("_id", DataTypes.StringType, false, Metadata.empty()),
                new StructField("string_field", DataTypes.StringType, true, Metadata.empty()),
                new StructField("int_field", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("long_field", DataTypes.LongType, true, Metadata.empty()),
                new StructField("double_field", DataTypes.DoubleType, true, Metadata.empty()),
                new StructField(
                        "decimal_field",
                        DataTypes.createDecimalType(10, 4),
                        true,
                        Metadata.empty()),
                new StructField("bool_true", DataTypes.BooleanType, true, Metadata.empty()),
                new StructField("bool_false", DataTypes.BooleanType, true, Metadata.empty()),
                new StructField("date_field", DataTypes.TimestampType, true, Metadata.empty()),
                new StructField("timestamp_field", DataTypes.StringType, true, Metadata.empty()),
                new StructField("binary_field", DataTypes.BinaryType, true, Metadata.empty()),
                new StructField("object_id_field", DataTypes.StringType, true, Metadata.empty()),
                new StructField("regex_field", DataTypes.StringType, true, Metadata.empty()),
                new StructField("null_field", DataTypes.StringType, true, Metadata.empty()),
                new StructField("nested_doc", nestedDocSchema, true, Metadata.empty()),
                new StructField(
                        "array_of_strings",
                        DataTypes.createArrayType(DataTypes.StringType, true),
                        true,
                        Metadata.empty()),
                new StructField(
                        "array_of_ints",
                        DataTypes.createArrayType(DataTypes.IntegerType, true),
                        true,
                        Metadata.empty()),
                new StructField(
                        "map_field",
                        DataTypes.createMapType(DataTypes.StringType, DataTypes.IntegerType, true),
                        true,
                        Metadata.empty()),
            });

            return spark.read().format("bson").schema(schema).load(testResource("all_types.json"));
        }

        @Test
        void readsCorrectRowCount() {
            Dataset<Row> df = loadAllTypes();
            assertThat(df.count()).isEqualTo(3);
        }

        @Test
        void readsStringFields() {
            List<Row> rows = loadAllTypes().select("_id", "string_field").collectAsList();

            assertThat(rows.get(0).getString(0)).isEqualTo("507f1f77bcf86cd799439011");
            assertThat(rows.get(0).getString(1)).isEqualTo("hello world");
            assertThat(rows.get(1).getString(1)).isEqualTo("second doc");
            assertThat(rows.get(2).getString(1)).isEqualTo("");
        }

        @Test
        void readsIntegerFields() {
            List<Row> rows = loadAllTypes().select("int_field").collectAsList();

            assertThat(rows.get(0).getInt(0)).isEqualTo(42);
            assertThat(rows.get(1).getInt(0)).isEqualTo(-1);
            assertThat(rows.get(2).getInt(0)).isEqualTo(Integer.MAX_VALUE);
        }

        @Test
        void readsLongFields() {
            List<Row> rows = loadAllTypes().select("long_field").collectAsList();

            assertThat(rows.get(0).getLong(0)).isEqualTo(9876543210L);
            assertThat(rows.get(1).getLong(0)).isEqualTo(0L);
            assertThat(rows.get(2).getLong(0)).isEqualTo(Long.MAX_VALUE);
        }

        @Test
        void readsDoubleFields() {
            List<Row> rows = loadAllTypes().select("double_field").collectAsList();

            assertThat(rows.get(0).getDouble(0)).isEqualTo(3.14159);
            assertThat(rows.get(1).getDouble(0)).isEqualTo(-273.15);
            assertThat(rows.get(2).getDouble(0)).isEqualTo(Double.MAX_VALUE);
        }

        @Test
        void readsDecimalFields() {
            List<Row> rows = loadAllTypes().select("decimal_field").collectAsList();

            assertThat(rows.get(0).getDecimal(0)).isEqualByComparingTo(new BigDecimal("12345.6789"));
            assertThat(rows.get(1).getDecimal(0)).isEqualByComparingTo(new BigDecimal("0.0001"));
            assertThat(rows.get(2).getDecimal(0)).isEqualByComparingTo(new BigDecimal("-99999.9900"));
        }

        @Test
        void readsBooleanFields() {
            List<Row> rows = loadAllTypes().select("bool_true", "bool_false").collectAsList();

            assertThat(rows.get(0).getBoolean(0)).isTrue();
            assertThat(rows.get(0).getBoolean(1)).isFalse();
            assertThat(rows.get(1).getBoolean(0)).isTrue();
            assertThat(rows.get(1).getBoolean(1)).isTrue();
            assertThat(rows.get(2).getBoolean(0)).isFalse();
            assertThat(rows.get(2).getBoolean(1)).isFalse();
        }

        @Test
        void readsDateTimeAsTimestamp() {
            List<Row> rows = loadAllTypes().select("date_field").collectAsList();

            Timestamp expected = Timestamp.from(Instant.ofEpochMilli(1609459200000L));
            assertThat(rows.get(0).getTimestamp(0)).isEqualTo(expected);

            Timestamp epoch = Timestamp.from(Instant.EPOCH);
            assertThat(rows.get(1).getTimestamp(0)).isEqualTo(epoch);
        }

        @Test
        void readsBsonTimestampAsString() {
            List<Row> rows = loadAllTypes().select("timestamp_field").collectAsList();

            assertThat(rows.get(0).getString(0)).contains("2021-01-01");
            assertThat(rows.get(1).getString(0)).contains("1970-01-01");
        }

        @Test
        void readsBinaryFields() {
            List<Row> rows = loadAllTypes().select("binary_field").collectAsList();

            assertThat(rows.get(0).<byte[]>getAs(0)).isEqualTo("hello world".getBytes());
            assertThat(rows.get(1).<byte[]>getAs(0)).isEqualTo(new byte[] {1, 2, 3, 4});
            assertThat(rows.get(2).<byte[]>getAs(0)).isEqualTo(new byte[0]);
        }

        @Test
        void readsObjectIdAsString() {
            List<Row> rows = loadAllTypes().select("object_id_field").collectAsList();

            assertThat(rows.get(0).getString(0)).isEqualTo("60d5ec9af682fbd12a892da3");
            assertThat(rows.get(1).getString(0)).isEqualTo("000000000000000000000000");
            assertThat(rows.get(2).getString(0)).isEqualTo("ffffffffffffffffffffffff");
        }

        @Test
        void readsRegexAsString() {
            List<Row> rows = loadAllTypes().select("regex_field").collectAsList();

            assertThat(rows.get(0).getString(0)).isEqualTo("^test.*$");
            assertThat(rows.get(1).getString(0)).isEqualTo("\\d+");
            assertThat(rows.get(2).getString(0)).isEqualTo("");
        }

        @Test
        void readsNullFields() {
            List<Row> rows = loadAllTypes().select("null_field").collectAsList();

            assertThat(rows.get(0).isNullAt(0)).isTrue();
            assertThat(rows.get(1).isNullAt(0)).isTrue();
            assertThat(rows.get(2).isNullAt(0)).isTrue();
        }

        @Test
        void readsNestedStructs() {
            List<Row> rows = loadAllTypes().select("nested_doc").collectAsList();

            Row nested0 = rows.get(0).getStruct(0);
            assertThat(nested0.getString(0)).isEqualTo("inner");
            assertThat(nested0.getInt(1)).isEqualTo(100);

            Row nested1 = rows.get(1).getStruct(0);
            assertThat(nested1.getString(0)).isEqualTo("second_inner");
            assertThat(nested1.getInt(1)).isEqualTo(200);

            Row nested2 = rows.get(2).getStruct(0);
            assertThat(nested2.getString(0)).isEqualTo("");
            assertThat(nested2.getInt(1)).isEqualTo(0);
        }

        @Test
        void readsArrayOfStrings() {
            List<Row> rows = loadAllTypes().select("array_of_strings").collectAsList();

            assertThat(rows.get(0).<String>getList(0)).containsExactly("alpha", "beta", "gamma");
            assertThat(rows.get(1).<String>getList(0)).isEmpty();
            assertThat(rows.get(2).<String>getList(0)).containsExactly("single");
        }

        @Test
        void readsArrayOfInts() {
            List<Row> rows = loadAllTypes().select("array_of_ints").collectAsList();

            assertThat(rows.get(0).<Integer>getList(0)).containsExactly(1, 2, 3);
            assertThat(rows.get(1).<Integer>getList(0)).containsExactly(99);
            assertThat(rows.get(2).<Integer>getList(0))
                    .containsExactly(Integer.MIN_VALUE, 0, Integer.MAX_VALUE);
        }

        @Test
        @SuppressWarnings("unchecked")
        void readsMapFields() {
            List<Row> rows = loadAllTypes().select("map_field").collectAsList();

            Map<String, Integer> map0 = rows.get(0).getJavaMap(0);
            assertThat(map0).containsEntry("key1", 10);
            assertThat(map0).containsEntry("key2", 20);
            assertThat(map0).containsEntry("key3", 30);
            assertThat(map0).hasSize(3);

            Map<String, Integer> map1 = rows.get(1).getJavaMap(0);
            assertThat(map1).containsEntry("only_key", 1);
            assertThat(map1).hasSize(1);

            Map<String, Integer> map2 = rows.get(2).getJavaMap(0);
            assertThat(map2).isEmpty();
        }
    }

    // ─── Sparse/Missing Fields ────────────────────────────────────────

    @Nested
    class SparseFieldsTest {

        @Test
        void handlesMissingFieldsAsNull() {
            StructType schema = new StructType(new StructField[] {
                new StructField("_id", DataTypes.StringType, false, Metadata.empty()),
                new StructField("resourceType", DataTypes.StringType, true, Metadata.empty()),
                new StructField("name", DataTypes.StringType, true, Metadata.empty()),
                new StructField("age", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("code", DataTypes.StringType, true, Metadata.empty()),
                new StructField("value", DataTypes.DoubleType, true, Metadata.empty()),
            });

            Dataset<Row> df = spark.read()
                    .format("bson")
                    .schema(schema)
                    .load(testResource("sparse_fields.json"));

            List<Row> rows = df.collectAsList();
            assertThat(rows).hasSize(4);

            Row alice = rows.get(0);
            assertThat(alice.getString(1)).isEqualTo("Patient");
            assertThat(alice.getString(2)).isEqualTo("Alice");
            assertThat(alice.getInt(3)).isEqualTo(30);
            assertThat(alice.isNullAt(4)).isTrue();
            assertThat(alice.isNullAt(5)).isTrue();

            Row bob = rows.get(1);
            assertThat(bob.getString(2)).isEqualTo("Bob");
            assertThat(bob.isNullAt(3)).isTrue();
            assertThat(bob.isNullAt(4)).isTrue();

            Row obs1 = rows.get(2);
            assertThat(obs1.getString(1)).isEqualTo("Observation");
            assertThat(obs1.isNullAt(2)).isTrue();
            assertThat(obs1.isNullAt(3)).isTrue();
            assertThat(obs1.getString(4)).isEqualTo("blood-pressure");
            assertThat(obs1.getDouble(5)).isEqualTo(120.5);
        }

        @Test
        void supportsSparkSqlQueriesOnSparseData() {
            StructType schema = new StructType(new StructField[] {
                new StructField("_id", DataTypes.StringType, false, Metadata.empty()),
                new StructField("resourceType", DataTypes.StringType, true, Metadata.empty()),
                new StructField("name", DataTypes.StringType, true, Metadata.empty()),
                new StructField("age", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("code", DataTypes.StringType, true, Metadata.empty()),
                new StructField("value", DataTypes.DoubleType, true, Metadata.empty()),
            });

            Dataset<Row> df = spark.read()
                    .format("bson")
                    .schema(schema)
                    .load(testResource("sparse_fields.json"));

            df.createOrReplaceTempView("sparse_data");

            List<Row> patients =
                    spark.sql("SELECT name FROM sparse_data WHERE resourceType = 'Patient'")
                            .collectAsList();
            assertThat(patients).hasSize(2);
            assertThat(patients.get(0).getString(0)).isEqualTo("Alice");
            assertThat(patients.get(1).getString(0)).isEqualTo("Bob");

            List<Row> withAge = spark.sql(
                            "SELECT name, age FROM sparse_data WHERE age IS NOT NULL ORDER BY age")
                    .collectAsList();
            assertThat(withAge).hasSize(2);
            assertThat(withAge.get(0).getString(0)).isEqualTo("Alice");
            assertThat(withAge.get(0).getInt(1)).isEqualTo(30);
        }
    }

    // ─── Nested Federation Data ───────────────────────────────────────

    @Nested
    class NestedFederationTest {

        private Dataset<Row> loadFederationData() {
            StructType securitySchema = new StructType(new StructField[] {
                new StructField("system", DataTypes.StringType, true, Metadata.empty()),
                new StructField("code", DataTypes.StringType, true, Metadata.empty()),
            });

            StructType metaSchema = new StructType(new StructField[] {
                new StructField("versionId", DataTypes.StringType, true, Metadata.empty()),
                new StructField("lastUpdated", DataTypes.StringType, true, Metadata.empty()),
                new StructField("source", DataTypes.StringType, true, Metadata.empty()),
                new StructField(
                        "security",
                        DataTypes.createArrayType(securitySchema, true),
                        true,
                        Metadata.empty()),
            });

            StructType schema = new StructType(new StructField[] {
                new StructField("_id", DataTypes.StringType, false, Metadata.empty()),
                new StructField("resourceType", DataTypes.StringType, true, Metadata.empty()),
                new StructField("id", DataTypes.StringType, true, Metadata.empty()),
                new StructField("meta", metaSchema, true, Metadata.empty()),
                new StructField("status", DataTypes.StringType, true, Metadata.empty()),
                new StructField(
                        "_access",
                        DataTypes.createMapType(DataTypes.StringType, DataTypes.IntegerType, true),
                        true,
                        Metadata.empty()),
                new StructField(
                        "_sourceAssigningAuthority", DataTypes.StringType, true, Metadata.empty()),
                new StructField("_uuid", DataTypes.StringType, true, Metadata.empty()),
                new StructField("_sourceId", DataTypes.StringType, true, Metadata.empty()),
            });

            return spark.read()
                    .format("bson")
                    .schema(schema)
                    .load(testResource("nested_federation.json"));
        }

        @Test
        void readsNestedMetaStruct() {
            List<Row> rows = loadFederationData().select("meta").collectAsList();

            Row meta0 = rows.get(0).getStruct(0);
            assertThat(meta0.getString(0)).isEqualTo("2");
            assertThat(meta0.getString(1)).contains("2024-07-20");
            assertThat(meta0.getString(2)).isEqualTo("https://example.com/platform");
        }

        @Test
        void readsArrayOfStructsInsideStruct() {
            List<Row> rows = loadFederationData().select("meta").collectAsList();

            Row meta0 = rows.get(0).getStruct(0);
            List<Row> security0 = meta0.getList(3);
            assertThat(security0).hasSize(1);
            assertThat(security0.get(0).getString(0)).isEqualTo("https://example.com/access");
            assertThat(security0.get(0).getString(1)).isEqualTo("org2");

            Row meta1 = rows.get(1).getStruct(0);
            List<Row> security1 = meta1.getList(3);
            assertThat(security1).hasSize(2);
            assertThat(security1.get(0).getString(1)).isEqualTo("org1");
            assertThat(security1.get(1).getString(1)).isEqualTo("org1");
        }

        @Test
        @SuppressWarnings("unchecked")
        void readsAccessMapField() {
            List<Row> rows = loadFederationData().select("_access").collectAsList();

            Map<String, Integer> access0 = rows.get(0).getJavaMap(0);
            assertThat(access0).containsEntry("org2", 1);
            assertThat(access0).hasSize(1);

            Map<String, Integer> access1 = rows.get(1).getJavaMap(0);
            assertThat(access1).containsEntry("org1", 1);
            assertThat(access1).containsEntry("org2", 1);
            assertThat(access1).hasSize(2);
        }

        @Test
        void supportsNestedFieldProjection() {
            Dataset<Row> df = loadFederationData();
            df.createOrReplaceTempView("federation_data");

            List<Row> results = spark.sql(
                            "SELECT id, meta.versionId, _sourceAssigningAuthority FROM federation_data")
                    .collectAsList();

            assertThat(results).hasSize(2);
            assertThat(results.get(0).getString(0)).isEqualTo("acct-J52EeTf");
            assertThat(results.get(0).getString(1)).isEqualTo("2");
            assertThat(results.get(0).getString(2)).isEqualTo("org2");
        }

        @Test
        void supportsFilterPushdownOnTopLevelFields() {
            Dataset<Row> df = loadFederationData();

            List<Row> filtered = df.filter("resourceType = 'Patient'").collectAsList();
            assertThat(filtered).hasSize(1);
            assertThat((String) filtered.get(0).getAs("id")).isEqualTo("patient-001");
        }
    }

    // ─── Format Alias ─────────────────────────────────────────────────

    @Nested
    class FormatAliasTest {

        @Test
        void ejsonFormatAliasWorks() {
            StructType schema = new StructType(new StructField[] {
                new StructField("_id", DataTypes.StringType, false, Metadata.empty()),
                new StructField("resourceType", DataTypes.StringType, true, Metadata.empty()),
            });

            Dataset<Row> df = spark.read()
                    .format("ejson")
                    .schema(schema)
                    .load(testResource("sparse_fields.json"));

            assertThat(df.count()).isEqualTo(4);
        }
    }

    // ─── Type Coercion via DataSource ─────────────────────────────────

    @Nested
    class TypeCoercionEndToEnd {

        @Test
        void intFieldReadAsLong() {
            StructType schema = new StructType(new StructField[] {
                new StructField("_id", DataTypes.StringType, false, Metadata.empty()),
                new StructField("int_field", DataTypes.LongType, true, Metadata.empty()),
            });

            Dataset<Row> df =
                    spark.read().format("bson").schema(schema).load(testResource("all_types.json"));
            List<Row> rows = df.collectAsList();

            assertThat(rows.get(0).getLong(1)).isEqualTo(42L);
            assertThat(rows.get(1).getLong(1)).isEqualTo(-1L);
        }

        @Test
        void intFieldReadAsDouble() {
            StructType schema = new StructType(new StructField[] {
                new StructField("_id", DataTypes.StringType, false, Metadata.empty()),
                new StructField("int_field", DataTypes.DoubleType, true, Metadata.empty()),
            });

            Dataset<Row> df =
                    spark.read().format("bson").schema(schema).load(testResource("all_types.json"));
            List<Row> rows = df.collectAsList();

            assertThat(rows.get(0).getDouble(1)).isEqualTo(42.0);
        }

        @Test
        void intFieldReadAsString() {
            StructType schema = new StructType(new StructField[] {
                new StructField("_id", DataTypes.StringType, false, Metadata.empty()),
                new StructField("int_field", DataTypes.StringType, true, Metadata.empty()),
            });

            Dataset<Row> df =
                    spark.read().format("bson").schema(schema).load(testResource("all_types.json"));
            List<Row> rows = df.collectAsList();

            assertThat(rows.get(0).getString(1)).isEqualTo("42");
        }

        @Test
        void dateTimeFieldReadAsString() {
            StructType schema = new StructType(new StructField[] {
                new StructField("_id", DataTypes.StringType, false, Metadata.empty()),
                new StructField("date_field", DataTypes.StringType, true, Metadata.empty()),
            });

            Dataset<Row> df =
                    spark.read().format("bson").schema(schema).load(testResource("all_types.json"));
            List<Row> rows = df.collectAsList();

            assertThat(rows.get(0).getString(1)).isEqualTo("2021-01-01T00:00:00Z");
            assertThat(rows.get(1).getString(1)).isEqualTo("1970-01-01T00:00:00Z");
        }
    }
}
