package com.splicemachine.derby.stream.spark;

import com.splicemachine.spark.splicemachine.ShuffleUtils;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import scala.Tuple2;

public class SparkExtractKey implements Function<Tuple2<Row, Row>, Tuple2<Row, Tuple2<Row, Row>>> {

    @Override
    public Tuple2<Row, Tuple2<Row, Row>> call(Tuple2<Row, Row> tuple) throws Exception {
        return new Tuple2<>(tuple._1(), tuple);
    }
}
