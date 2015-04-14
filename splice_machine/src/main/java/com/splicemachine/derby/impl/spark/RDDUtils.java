package com.splicemachine.derby.impl.spark;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.derby.impl.sql.execute.operations.SparkRow;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

import java.util.Iterator;

public class RDDUtils {
    public static final Logger LOG = Logger.getLogger(RDDUtils.class);

    public static JavaPairRDD<ExecRow, SparkRow> getKeyedRDD(JavaRDD<SparkRow> rdd, final int[] keyColumns)
            throws StandardException {
        JavaPairRDD<ExecRow, SparkRow> keyed = rdd.keyBy(new Keyer(keyColumns));
        return keyed;
    }

    private static void printRDD(String title, @SuppressWarnings("rawtypes") Iterable it) {
        StringBuilder sb = new StringBuilder(title);
        sb.append(": ");
        boolean first = true;
        for (Object o : it) {
            if (!first) {
                sb.append(",");
            }
            sb.append(o);
            first = false;
        }
        LOG.debug(sb);
    }

    public static void printRDD(String title, JavaRDD<ExecRow> rdd) {
        if (LOG.isDebugEnabled()) {
            printRDD(title, rdd.collect());
        }
    }

    public static void printRDD(String title, JavaPairRDD<ExecRow, ExecRow> rdd) {
        if (LOG.isDebugEnabled()) {
            printRDD(title, rdd.collect());
        }
    }

    public static ExecRow getKey(ExecRow row, int[] keyColumns) throws StandardException {
        ValueRow key = new ValueRow(keyColumns.length);
        int position = 1;
        for (int keyColumn : keyColumns) {
            key.setColumn(position++, row.getColumn(keyColumn + 1));
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("Added key, returning (%s, %s) key hash %d", key, row, key.hashCode()));
        }
        return key;
    }

    public static JavaRDD<SparkRow> toSparkRows(JavaRDD<ExecRow> execRows) {
        return execRows.map(new Function<ExecRow, SparkRow>() {
            @Override
            public SparkRow call(ExecRow execRow) throws Exception {
                return new SparkRow(execRow);
            }
        });
    }

    public static Iterator<ExecRow> toExecRowsIterator(final Iterator<SparkRow> sparkRowsIterator) {
        return new Iterator<ExecRow>() {
            @Override
            public boolean hasNext() {
                return sparkRowsIterator.hasNext();
            }

            @Override
            public ExecRow next() {
                return sparkRowsIterator.next().getRow();
            }

            @Override
            public void remove() {
                sparkRowsIterator.remove();
            }
        };
    }

    public static Iterable<SparkRow> toSparkRowsIterable(Iterable<ExecRow> execRows) {
        return new SparkRowsIterable(execRows);
    }

    public static class SparkRowsIterable implements Iterable<SparkRow>, Iterator<SparkRow> {
        private Iterator<ExecRow> execRows;

        public SparkRowsIterable(Iterable<ExecRow> execRows) {
            this.execRows = execRows.iterator();
        }

        @Override
        public Iterator<SparkRow> iterator() {
            return this;
        }

        @Override
        public boolean hasNext() {
            return execRows.hasNext();
        }

        @Override
        public SparkRow next() {
            return new SparkRow(execRows.next());
        }

        @Override
        public void remove() {
            execRows.remove();
        }
    }

    public static class Keyer implements Function<SparkRow, ExecRow> {

        private static final long serialVersionUID = 3988079974858059941L;
        private int[] keyColumns;

        public Keyer() {
        }

        public Keyer(int[] keyColumns) {
            this.keyColumns = keyColumns;
        }

        @Override
        public ExecRow call(SparkRow row) throws Exception {
            return RDDUtils.getKey(row.getRow(), keyColumns);
        }
    }
}
