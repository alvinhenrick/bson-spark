package com.bson.spark.interop;

import java.util.List;
import java.util.Map;

import scala.collection.JavaConverters;

/**
 * Scala 2.12 collection interop utilities.
 */
public final class JavaScala {

    private JavaScala() {}

    @SuppressWarnings("deprecation")
    public static <K, V> scala.collection.immutable.Map<K, V> asScalaImmutable(Map<K, V> data) {
        scala.collection.mutable.Map<K, V> mutableMap =
                JavaConverters.mapAsScalaMapConverter(data).asScala();
        return mutableMap.toMap(scala.Predef.<scala.Tuple2<K, V>>conforms());
    }

    @SuppressWarnings("deprecation")
    public static <K, V> Map<K, V> asJava(scala.collection.Map<K, V> data) {
        return JavaConverters.mapAsJavaMapConverter(data).asJava();
    }

    @SuppressWarnings("deprecation")
    public static <A> List<A> asJava(scala.collection.Seq<A> data) {
        return JavaConverters.seqAsJavaListConverter(data).asJava();
    }
}
