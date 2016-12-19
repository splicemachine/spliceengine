/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.derby.stream.spark;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
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
    public static void setAncestorRDDNames(JavaPairRDD rdd, int levels, String[] newNames, String[] checkNames) {
        assert levels > 0;
        setAncestorRDDNames(rdd.rdd(), levels, newNames, checkNames);
    }

    @SuppressWarnings("rawtypes")
    public static void setAncestorRDDNames(JavaRDD rdd, int levels, String[] newNames, String[] checkNames) {
        assert levels > 0;
        setAncestorRDDNames(rdd.rdd(), levels, newNames, checkNames);
    }

    @SuppressWarnings("rawtypes")
    // TODO (wjk): remove this when we have a better way to change name of RDDs implicitly created within spark
    private static void setAncestorRDDNames(org.apache.spark.rdd.RDD rdd, int levels, String[] newNames, String[] checkNames) {
        assert levels > 0;
        org.apache.spark.rdd.RDD currentRDD = rdd;
        for (int i = 0; i < levels && currentRDD != null; i++) {
            org.apache.spark.rdd.RDD rddAnc =
                ((org.apache.spark.Dependency)currentRDD.dependencies().head()).rdd();
            if (rddAnc != null) {
                if (checkNames == null || checkNames[i] == null)
                    rddAnc.setName(newNames[i]);
                else if (rddAnc.name().equals(checkNames[i]))
                    rddAnc.setName(newNames[i]);
            }
            currentRDD = rddAnc;
        }
    }

    public static int getPartitions(JavaRDDLike<?,?> rdd, int defaultPartitions) {
        int rddPartitions = rdd.getNumPartitions();
        return Math.max(rddPartitions, defaultPartitions);
    }

    public static int getPartitions(JavaRDDLike<?,?> rdd1, JavaRDDLike<?,?> rdd2, int defaultPartitions) {
        int rddPartitions1 = rdd1.getNumPartitions();
        int rddPartitions2 = rdd2.getNumPartitions();
        int max = Math.max(rddPartitions1, rddPartitions2);
        return Math.max(max, defaultPartitions);
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
