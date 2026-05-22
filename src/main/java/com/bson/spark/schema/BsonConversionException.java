package com.bson.spark.schema;

/**
 * Exception thrown when a BSON value cannot be converted to the
 * expected Spark data type.
 */
public final class BsonConversionException extends RuntimeException {

    public BsonConversionException(String message) {
        super(message);
    }
}
