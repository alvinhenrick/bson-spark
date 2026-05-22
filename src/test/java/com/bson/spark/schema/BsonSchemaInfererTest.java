package com.bson.spark.schema;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.bson.BsonDocument;
import org.junit.jupiter.api.Test;

class BsonSchemaInfererTest {

    @Test
    void mergesSchemasWithNewFields() {
        StructType left = new StructType(new StructField[]{
                DataTypes.createStructField("_id", DataTypes.StringType, true),
                DataTypes.createStructField("name", DataTypes.StringType, true),
        });

        StructType right = new StructType(new StructField[]{
                DataTypes.createStructField("_id", DataTypes.StringType, true),
                DataTypes.createStructField("status", DataTypes.StringType, true),
        });

        StructType merged = BsonSchemaInferer.mergeSchemas(left, right);

        assertThat(merged.fieldNames()).containsExactly("_id", "name", "status");
    }

    @Test
    void widensIntToLong() {
        StructType left = new StructType(new StructField[]{
                DataTypes.createStructField("count", DataTypes.IntegerType, true),
        });

        StructType right = new StructType(new StructField[]{
                DataTypes.createStructField("count", DataTypes.LongType, true),
        });

        StructType merged = BsonSchemaInferer.mergeSchemas(left, right);

        assertThat(merged.fields()[0].dataType()).isEqualTo(DataTypes.LongType);
    }

    @Test
    void widensNumericToDouble() {
        StructType left = new StructType(new StructField[]{
                DataTypes.createStructField("value", DataTypes.IntegerType, true),
        });

        StructType right = new StructType(new StructField[]{
                DataTypes.createStructField("value", DataTypes.DoubleType, true),
        });

        StructType merged = BsonSchemaInferer.mergeSchemas(left, right);

        assertThat(merged.fields()[0].dataType()).isEqualTo(DataTypes.DoubleType);
    }

    @Test
    void makesFieldNullableIfMissingInOneSchema() {
        StructType left = new StructType(new StructField[]{
                DataTypes.createStructField("_id", DataTypes.StringType, false),
                DataTypes.createStructField("name", DataTypes.StringType, false),
        });

        StructType right = new StructType(new StructField[]{
                DataTypes.createStructField("_id", DataTypes.StringType, false),
        });

        StructType merged = BsonSchemaInferer.mergeSchemas(left, right);

        // "name" is missing from right → should become nullable
        StructField nameField = merged.fields()[1];
        assertThat(nameField.name()).isEqualTo("name");
        assertThat(nameField.nullable()).isTrue();
    }
}
