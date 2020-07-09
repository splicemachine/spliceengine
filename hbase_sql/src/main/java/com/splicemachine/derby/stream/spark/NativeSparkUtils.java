package com.splicemachine.derby.stream.spark;

import org.apache.log4j.Logger;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.WithColumns;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.util.List;

public class NativeSparkUtils {
    private static <T> Seq<T> toSeq(List<T> list) {
        return JavaConverters.asScalaIteratorConverter(list.iterator()).asScala().toSeq();
    }

    public static Dataset<Row> withColumns(List<String> names, List<Column> columns, Dataset<Row> ds) {
        return WithColumns.withColumns(NativeSparkUtils.<String>toSeq(names), NativeSparkUtils.<Column>toSeq(columns), ds);
    }
}