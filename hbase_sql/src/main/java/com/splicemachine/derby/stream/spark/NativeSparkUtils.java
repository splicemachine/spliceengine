package com.splicemachine.derby.stream.spark;

import org.apache.log4j.Logger;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.lang.reflect.Method;
import java.util.Iterator;
import java.util.List;

public class NativeSparkUtils {
    private static final Logger LOG = Logger.getLogger(NativeSparkUtils.class);
    private static Method WITH_COLUMNS_METHOD;

    static {
        try {
            WITH_COLUMNS_METHOD = Dataset.class.getDeclaredMethod("withColumns", Seq.class, Seq.class);
        } catch (NoSuchMethodException e) {
            LOG.info("Spark version doesn't have 'withColumns' method, will use 'withColumn'");
        }
    }

    private static <T> Seq<T> toSeq(List<T> list) {
        return JavaConverters.asScalaIteratorConverter(list.iterator()).asScala().toSeq();
    }

    public static Dataset<Row> withColumns(List<String> names, List<Column> columns, Dataset<Row> ds) {
        if (WITH_COLUMNS_METHOD != null) {
            try {
                return (Dataset<Row>) WITH_COLUMNS_METHOD.invoke(ds, toSeq(names), toSeq(columns));
            } catch (Exception e) {
                LOG.warn("Unexpected exception, falling back to 'withColumn'");
            }
        }
        Iterator<String> name = names.iterator();
        Iterator<Column> column = columns.iterator();
        while (name.hasNext()) {
            ds = ds.withColumn(name.next(), column.next());
        }
        return ds;
    }

    public static void main(String[] args) {
        System.out.println("test");
    }
}
