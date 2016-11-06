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

import com.splicemachine.EngineDriver;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.ResultColumnDescriptor;
import com.splicemachine.db.iapi.sql.ResultDescription;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.store.access.ColumnOrdering;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.impl.jdbc.EmbedResultSet40;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.derby.impl.SpliceSpark;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;

import com.splicemachine.derby.impl.sql.execute.operations.SpliceBaseOperation;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.collection.JavaConversions;

import java.sql.ResultSet;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.asc;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.desc;

public class SparkUtils {
    public static final Logger LOG = Logger.getLogger(SparkUtils.class);

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
            return SparkUtils.getKey(row.getRow(), keyColumns);
        }
    }

    public static Dataset<Row> resultSetToDF(ResultSet rs) throws StandardException {
        EmbedResultSet40 ers = (EmbedResultSet40) rs;
        com.splicemachine.db.iapi.sql.ResultSet serverSideRs = ers.getUnderlyingResultSet();
        JavaRDD<LocatedRow> rdd = ((SparkDataSet) ((SpliceBaseOperation) serverSideRs).getDataSet(EngineDriver.driver().processorFactory().distributedProcessor())).rdd;
        // The schema is encoded in a string
        ResultDescription rd = serverSideRs.getResultDescription();
        final ResultColumnDescriptor[] columns = rd.getColumnInfo();

        // Generate the schema based on the string of schema
        List<StructField> fields = new ArrayList<>();
        int i = 0;
        for (ResultColumnDescriptor column : columns) {
            StructField field = DataTypes.createStructField(column.getName(), convertResultColumnDescriptorToSparkType(column), true);
            fields.add(field);
            i++;
        }
        StructType schema = DataTypes.createStructType(fields);

        // Convert records of the RDD to Rows
        JavaRDD<Row> rowRDD = rdd.map(new Function<LocatedRow, Row>() {
            @Override
            public Row call(LocatedRow record) throws Exception {
                Object[] values = new Object[columns.length];
                int i=0;
                for(DataValueDescriptor dvd : record.getRow().getRowArray()){
                    values[i] = dvd.getObject();
                    ++i;
                }
                return RowFactory.create(values);
            }
        });

        /* Apply the schema to the RDD
           Maybe we have to dispatcg an OLAP query because this creation is resource intense
           Staying simple for now
        */
        SparkSession s = SpliceSpark.getSession();
        Dataset<Row> df = s.createDataFrame(rowRDD, schema);
        return df;
    }

//TODO: (MZ) Will make a DataType ResultColumnDescriptor.getSparkType() method to make this cleaner
    public static DataType convertResultColumnDescriptorToSparkType(ResultColumnDescriptor rcd){
        int position = rcd.getColumnPosition();
        DataTypeDescriptor type = rcd.getType();
        switch(type.getJDBCTypeId()) {
            case Types.BIGINT:
                return DataTypes.LongType;
            case Types.INTEGER:
                return DataTypes.IntegerType;
            case Types.SMALLINT:
                return DataTypes.ShortType;
            case Types.TINYINT:
                return DataTypes.ShortType;
            case Types.DECIMAL:
                return DataTypes.FloatType;
            case Types.NUMERIC:
                return DataTypes.DoubleType;
            case Types.DOUBLE:
                return DataTypes.DoubleType;
            case Types.FLOAT:
                return DataTypes.FloatType;
            case Types.CHAR:
                return DataTypes.StringType;
            case Types.VARCHAR:
                return DataTypes.StringType;
            case Types.DATE:
                return DataTypes.DateType;
            case Types.TIMESTAMP:
                return DataTypes.TimestampType;
            case Types.TIME:
                return DataTypes.TimestampType; /* TODO: (MZ) Not sure if this is the right conversion */
            case Types.NULL:
                return DataTypes.NullType;
            default:
                return DataTypes.NullType;
        }
    }

    /**
     * Convert Sort Columns, convert to 0-based index
     * @param sortColumns
     * @return
     */


    public static scala.collection.mutable.Buffer<Column> convertSortColumns(ColumnOrdering[] sortColumns){
        return Arrays
                .stream(sortColumns)
                .map(column -> column.getIsAscending() ? asc(ValueRow.getNamedColumn(column.getColumnId()-1)) :
                        desc(ValueRow.getNamedColumn(column.getColumnId()-1)))
                .collect(Collectors.collectingAndThen(Collectors.toList(), JavaConversions::asScalaBuffer));
    }

    /**
     * Convert partition to Spark dataset columns
     * Ignoring partition
     * @param sortColumns
     * @return
     */

    public static scala.collection.mutable.Buffer<Column> convertPartitions(ColumnOrdering[] sortColumns){
        return Arrays
                .stream(sortColumns)
                .map(column -> col(ValueRow.getNamedColumn(column.getColumnId()-1)))
                .collect(Collectors.collectingAndThen(Collectors.toList(), JavaConversions::asScalaBuffer));
    }


}
