package com.bson.spark.interop;

import java.util.List;
import java.util.Map;
import scala.jdk.CollectionConverters;

/** Scala 2.13 collection interop utilities. */
public final class JavaScala {

  private JavaScala() {}

  public static <K, V> scala.collection.Map<K, V> asScala(Map<K, V> data) {
    return CollectionConverters.MapHasAsScala(data).asScala();
  }

  public static <K, V> scala.collection.immutable.Map<K, V> asScalaImmutable(Map<K, V> data) {
    return scala.collection.immutable.Map.from(asScala(data));
  }

  public static <A> scala.collection.Seq<A> asScala(List<A> data) {
    return CollectionConverters.ListHasAsScala(data).asScala();
  }

  public static <K, V> Map<K, V> asJava(scala.collection.Map<K, V> data) {
    return CollectionConverters.MapHasAsJava(data).asJava();
  }

  public static <A> List<A> asJava(scala.collection.Seq<A> data) {
    return CollectionConverters.SeqHasAsJava(data).asJava();
  }
}
