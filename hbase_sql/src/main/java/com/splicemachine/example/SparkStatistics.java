/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.example;

import splice.com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.ResultColumnDescriptor;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.iapi.types.SQLDouble;
import com.splicemachine.db.iapi.types.SQLLongint;
import com.splicemachine.db.iapi.types.SQLVarchar;
import com.splicemachine.db.impl.jdbc.EmbedConnection;
import com.splicemachine.db.impl.jdbc.EmbedResultSet40;
import com.splicemachine.db.impl.sql.GenericColumnDescriptor;
import com.splicemachine.db.impl.sql.execute.IteratorNoPutResultSet;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.stat.MultivariateStatisticalSummary;
import org.apache.spark.mllib.stat.Statistics;
import java.sql.*;
import java.util.List;

public class SparkStatistics {

    private static final ResultColumnDescriptor[] STATEMENT_STATS_OUTPUT_COLUMNS = new GenericColumnDescriptor[]{
            new GenericColumnDescriptor("COLUMN_NAME", DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR)),
            new GenericColumnDescriptor("MIN", DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.DOUBLE)),
            new GenericColumnDescriptor("MAX", DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.DOUBLE)),
            new GenericColumnDescriptor("NUM_NONZEROS", DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.DOUBLE)),
            new GenericColumnDescriptor("VARIANCE", DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.DOUBLE)),
            new GenericColumnDescriptor("MEAN", DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.DOUBLE)),
            new GenericColumnDescriptor("NORML1", DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.DOUBLE)),
            new GenericColumnDescriptor("MORML2", DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.DOUBLE)),
            new GenericColumnDescriptor("COUNT", DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BIGINT)),
    };


    @SuppressFBWarnings(value = "OBL_UNSATISFIED_OBLIGATION_EXCEPTION_EDGE", justification = "DB-9844")
    public static void getStatementStatistics(String statement, ResultSet[] resultSets) throws SQLException {
        try {
            // Run sql statement
            Connection con = DriverManager.getConnection("jdbc:default:connection");
            PreparedStatement ps = con.prepareStatement(statement);
            ResultSet rs = ps.executeQuery();

            // Convert result set to Java RDD
            JavaRDD<ExecRow> resultSetRDD = ResultSetToRDD(rs);

            // Collect column statistics
            int[] fieldsToConvert = getFieldsToConvert(ps);
            MultivariateStatisticalSummary summary = getColumnStatisticsSummary(resultSetRDD, fieldsToConvert);

            IteratorNoPutResultSet resultsToWrap = wrapResults((EmbedConnection) con, getColumnStatistics(ps, summary, fieldsToConvert));
            resultSets[0] = new EmbedResultSet40((EmbedConnection)con, resultsToWrap, false, null, true);
       } catch (StandardException e) {
            throw new SQLException(Throwables.getRootCause(e));
        }
    }

    private static MultivariateStatisticalSummary getColumnStatisticsSummary(JavaRDD<ExecRow> resultSetRDD,
                                                                     int[] fieldsToConvert) throws StandardException{
        JavaRDD<Vector> vectorJavaRDD = SparkMLibUtils.locatedRowRDDToVectorRDD(resultSetRDD, fieldsToConvert);
        return Statistics.colStats(vectorJavaRDD.rdd());
    }


    /*
     * Convert a ResultSet to JavaRDD
     */
    private static JavaRDD<ExecRow> ResultSetToRDD (ResultSet resultSet) throws StandardException{
        EmbedResultSet40 ers = (EmbedResultSet40)resultSet;

        com.splicemachine.db.iapi.sql.ResultSet rs = ers.getUnderlyingResultSet();

        return SparkMLibUtils.resultSetToRDD(rs);
    }


    private static int[] getFieldsToConvert(PreparedStatement ps) throws SQLException{
        ResultSetMetaData metaData = ps.getMetaData();
        int columnCount = metaData.getColumnCount();
        int[] fieldsToConvert = new int[columnCount];
        for (int i = 0; i < columnCount; ++i) {
            fieldsToConvert[i] = i+1;
        }
        return fieldsToConvert;
    }

    /*
     * Convert column statistics to an iterable row source
     */
    private static Iterable<ExecRow> getColumnStatistics(PreparedStatement ps,
                                                         MultivariateStatisticalSummary summary,
                                                         int[] fieldsToConvert) throws StandardException {
        try {

            List<ExecRow> rows = Lists.newArrayList();
            ResultSetMetaData metaData = ps.getMetaData();

            double[] min = summary.min().toArray();
            double[] max = summary.max().toArray();
            double[] mean = summary.mean().toArray();
            double[] nonZeros = summary.numNonzeros().toArray();
            double[] variance = summary.variance().toArray();
            double[] normL1 = summary.normL1().toArray();
            double[] normL2 = summary.normL2().toArray();
            long count = summary.count();

            for (int i= 0; i < fieldsToConvert.length; ++i) {
                int columnPosition = fieldsToConvert[i];
                String columnName = metaData.getColumnName(columnPosition);
                ExecRow row = new ValueRow(9);
                row.setColumn(1, new SQLVarchar(columnName));
                row.setColumn(2, new SQLDouble(min[columnPosition-1]));
                row.setColumn(3, new SQLDouble(max[columnPosition-1]));
                row.setColumn(4, new SQLDouble(nonZeros[columnPosition-1]));
                row.setColumn(5, new SQLDouble(variance[columnPosition-1]));
                row.setColumn(6, new SQLDouble(mean[columnPosition-1]));
                row.setColumn(7, new SQLDouble(normL1[columnPosition-1]));
                row.setColumn(8, new SQLDouble(normL2[columnPosition-1]));
                row.setColumn(9, new SQLLongint(count));
                rows.add(row);
            }
            return rows;
        }
        catch (Exception e) {
            throw StandardException.newException(e.getLocalizedMessage());
        }
    }

    private static IteratorNoPutResultSet wrapResults(EmbedConnection conn, Iterable<ExecRow> rows) throws
            StandardException {
        Activation lastActivation = conn.getLanguageConnection().getLastActivation();
        IteratorNoPutResultSet resultsToWrap = new IteratorNoPutResultSet(rows, STATEMENT_STATS_OUTPUT_COLUMNS,
                lastActivation);
        resultsToWrap.openCore();
        return resultsToWrap;
    }
}
