package com.splicemachine.derby.stream.spark;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;

import java.util.Iterator;

public class RDDUtils {
    public static final Logger LOG = Logger.getLogger(RDDUtils.class);

    public static JavaPairRDD<ExecRow, LocatedRow> getKeyedRDD(JavaRDD<LocatedRow> rdd, final int[] keyColumns)
            throws StandardException {
        JavaPairRDD<ExecRow, LocatedRow> keyed = rdd.keyBy(new Keyer(keyColumns));
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

    public static JavaRDD<LocatedRow> toSparkRows(JavaRDD<ExecRow> execRows) {
        return execRows.map(new Function<ExecRow, LocatedRow>() {
            @Override
            public LocatedRow call(ExecRow execRow) throws Exception {
                return new LocatedRow(execRow);
            }
        });
    }

    public static Iterator<ExecRow> toExecRowsIterator(final Iterator<LocatedRow> sparkRowsIterator) {
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

    public static Iterable<LocatedRow> toSparkRowsIterable(Iterable<ExecRow> execRows) {
        return new SparkRowsIterable(execRows);
    }
    
    @SuppressWarnings("rawtypes")
    public static void setAncestorRDDNames(JavaPairRDD rdd, int levels, String[] names) {
        assert levels > 0;
        setAncestorRDDNames(rdd.rdd(), levels, names);
    }

    @SuppressWarnings("rawtypes")
    public static void setAncestorRDDNames(JavaRDD rdd, int levels, String[] names) {
        assert levels > 0;
        setAncestorRDDNames(rdd.rdd(), levels, names);
    }

    @SuppressWarnings("rawtypes")
    // TODO (wjk): remove this when we have a better way to change name of RDDs implicitly created within spark
    private static void setAncestorRDDNames(org.apache.spark.rdd.RDD rdd, int levels, String[] names) {
        assert levels > 0;
        org.apache.spark.rdd.RDD currentRDD = rdd;
        for (int i = 0; i < levels && currentRDD != null; i++) {
            org.apache.spark.rdd.RDD rddAnc =
                ((org.apache.spark.OneToOneDependency)currentRDD.dependencies().head()).rdd();
            if (rddAnc != null) rddAnc.setName(names[i]);
            currentRDD = rddAnc;
        }
    }

    public static class SparkRowsIterable implements Iterable<LocatedRow>, Iterator<LocatedRow> {
        private Iterator<ExecRow> execRows;

        public SparkRowsIterable(Iterable<ExecRow> execRows) {
            this.execRows = execRows.iterator();
        }

        @Override
        public Iterator<LocatedRow> iterator() {
            return this;
        }

        @Override
        public boolean hasNext() {
            return execRows.hasNext();
        }

        @Override
        public LocatedRow next() {
            return new LocatedRow(execRows.next());
        }

        @Override
        public void remove() {
            execRows.remove();
        }
    }

    public static class Keyer implements Function<LocatedRow, ExecRow> {

        private static final long serialVersionUID = 3988079974858059941L;
        private int[] keyColumns;

        public Keyer() {
        }

        public Keyer(int[] keyColumns) {
            this.keyColumns = keyColumns;
        }

        @Override
        public ExecRow call(LocatedRow row) throws Exception {
            return RDDUtils.getKey(row.getRow(), keyColumns);
        }
    }
}
