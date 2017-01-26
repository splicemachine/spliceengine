/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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

package com.splicemachine.mrio.api.core;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.ResultColumnDescriptor;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.iapi.types.SQLBoolean;
import com.splicemachine.db.iapi.types.SQLLongint;
import com.splicemachine.db.impl.jdbc.EmbedConnection;
import com.splicemachine.db.impl.jdbc.EmbedResultSet40;
import com.splicemachine.db.impl.sql.GenericColumnDescriptor;
import com.splicemachine.db.impl.sql.execute.IteratorNoPutResultSet;
import com.splicemachine.db.impl.sql.execute.ValueRow;

import com.splicemachine.derby.stream.spark.SparkUtils;
import com.splicemachine.derby.test.framework.*;

import org.apache.log4j.Logger;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;

import java.awt.*;
import java.sql.*;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Types;
import java.util.*;
import java.util.List;


import static org.junit.Assert.assertEquals;




/**
 * Created by mzweben on 8/25/16.
 */
public class DataFrameIT extends SpliceUnitTest {

    private static Logger LOG = Logger.getLogger(DataFrameIT.class);
    public static final String CLASS_NAME = DataFrameIT.class.getSimpleName().toUpperCase();
    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher(CLASS_NAME);
    public static final String TABLE_NAME_1 = "FOOD";
    public static final String TABLE_NAME_2 = "PERSON";
    public static final String TABLE_NAME_3 = "BOOL_TABLE";
    public static final String TABLE_NAME_4 = "A";
    public static final String TABLE_NAME_5 = "B";
    public static final String TABLE_NAME_6 = "C";

    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);
    protected static SpliceTableWatcher spliceTableWatcher1 = new SpliceTableWatcher(TABLE_NAME_1,CLASS_NAME,"(name varchar(255),value1 varchar(255),value2 varchar(255))");
    protected static SpliceTableWatcher spliceTableWatcher2 = new SpliceTableWatcher(TABLE_NAME_2,CLASS_NAME,"(name varchar(255), age float,created_time timestamp)");
    protected static SpliceTableWatcher spliceTableWatcher3 = new SpliceTableWatcher(TABLE_NAME_3,CLASS_NAME,"(col boolean)");
    protected static SpliceTableWatcher spliceTableWatcher4 = new SpliceTableWatcher(TABLE_NAME_4,CLASS_NAME,"(id int, text char(20))");
    protected static SpliceTableWatcher spliceTableWatcher5 = new SpliceTableWatcher(TABLE_NAME_5,CLASS_NAME,"(id int)");
    protected static SpliceTableWatcher spliceTableWatcher6 = new SpliceTableWatcher(TABLE_NAME_6,CLASS_NAME,"(id int)");

    private static final ResultColumnDescriptor[] DATAFRAME_COUNT_STORED_PROCEDURE_COLUMN_DECSRIPTOR = new GenericColumnDescriptor[]{
            new GenericColumnDescriptor("COUNT", DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BIGINT))
    };

    private static final ResultColumnDescriptor[] DATAFRAME_NTH_STORED_PROCEDURE_COLUMN_DECSRIPTOR = new GenericColumnDescriptor[]{
            new GenericColumnDescriptor("SAME", DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BOOLEAN))
    };
    private static long startTime;

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher)
            .around(spliceTableWatcher1)
            .around(spliceTableWatcher2)
            .around(spliceTableWatcher3)
            .around(spliceTableWatcher4)
            .around(spliceTableWatcher5)
            .around(spliceTableWatcher6)
            .around(new SpliceDataWatcher(){
                @Override
                protected void starting(Description description) {
                    try {
                        spliceClassWatcher.setAutoCommit(true);

                        // Stored procedure returns size of dataframe
                        String proc = format("CREATE PROCEDURE Splice.testResultSetToDF(statement VARCHAR(1024))" +
                                "   PARAMETER STYLE JAVA " +
                                "   LANGUAGE JAVA " +
                                "   READS SQL DATA " +
                                "   DYNAMIC RESULT SETS 1 " +
                                "   EXTERNAL NAME 'com.splicemachine.mrio.api.core.DataFrameIT.testResultSetToDF'");
                        PreparedStatement ps = spliceClassWatcher.prepareStatement(proc);
                        ps.execute();

                        // Stored procedure returns true if nth element of dataframe
                        String proc1 = format("CREATE PROCEDURE Splice.testNth(tableName VARCHAR(1024), type VARCHAR(100), nthRow INTEGER, nthCol INTEGER)" +
                                "   PARAMETER STYLE JAVA " +
                                "   LANGUAGE JAVA " +
                                "   READS SQL DATA " +
                                "   DYNAMIC RESULT SETS 1 " +
                                "   EXTERNAL NAME 'com.splicemachine.mrio.api.core.DataFrameIT.testNth'");
                        ps = spliceClassWatcher.prepareStatement(proc1);
                        ps.execute();

                        spliceClassWatcher.executeUpdate(format("insert into %s.%s values (1,'dw')",CLASS_NAME,TABLE_NAME_4));
                        spliceClassWatcher.executeUpdate(format("insert into %s.%s values (1,'d2w')",CLASS_NAME,TABLE_NAME_4));
                        spliceClassWatcher.executeUpdate(format("insert into %s.%s values (2,'d2w')",CLASS_NAME,TABLE_NAME_4));
                        spliceClassWatcher.executeUpdate(format("insert into %s.%s(id) values (3)",CLASS_NAME,TABLE_NAME_4));
                        spliceClassWatcher.executeUpdate(format("insert into %s.%s(id) values (4)",CLASS_NAME,TABLE_NAME_4));
                        spliceClassWatcher.executeUpdate(format("insert into %s.%s(text) values ('4')",CLASS_NAME,TABLE_NAME_4));
                        spliceClassWatcher.executeUpdate(format("insert into %s.%s(text) values ('45')",CLASS_NAME,TABLE_NAME_4));


                        Triplet triple = new Triplet("jzhang","pickles","greens");
                        ps = spliceClassWatcher.prepareStatement(format("insert into %s.%s (name,value1,value2) values (?,?,?)",CLASS_NAME,TABLE_NAME_1));
                        ps.setString(1, triple.k1);
                        ps.setString(2, triple.k2);
                        ps.setString(3, triple.k3);
                        ps.executeUpdate();

                        Triplet t2 = new Triplet("sfines","turkey","apples");
                        ps.setString(1, t2.k1);
                        ps.setString(2, t2.k2);
                        ps.setString(3, t2.k3);
                        ps.executeUpdate();

                        Triplet t3 = new Triplet("jleach","roast beef","tacos");
                        ps.setString(1, t3.k1);
                        ps.setString(2, t3.k2);
                        ps.setString(3, t3.k3);
                        ps.executeUpdate();

                        // add row to person
                        startTime = System.currentTimeMillis();
                        ps = spliceClassWatcher.prepareStatement("insert into " + spliceTableWatcher2 + " values (?,?,?)");
                        ps.setString(1,"joe");
                        ps.setFloat(2, 5.5f);
                        ps.setTimestamp(3, new Timestamp(System.currentTimeMillis()));
                        ps.execute();

                        ps.setString(1, "bob");
                        ps.setFloat(2, 1.2f);
                        ps.setTimestamp(3, new Timestamp(System.currentTimeMillis()));
                        ps.execute();

                        ps.setString(1, "tom");
                        ps.setFloat(2, 13.4667f);
                        ps.setTimestamp(3, new Timestamp(System.currentTimeMillis()));
                        ps.execute();

                        ps = spliceClassWatcher.prepareStatement("insert into "+ spliceTableWatcher3+" values (?)");
                        ps.setObject(1, null);
                        ps.addBatch();
                        ps.setBoolean(1, true);
                        ps.addBatch();
                        ps.setBoolean(1, false);
                        ps.addBatch();
                        ps.executeBatch();

                        ps = spliceClassWatcher.prepareStatement("insert into "+ spliceTableWatcher5+" values (?)");
                        ps.setInt(1,1);  ps.execute();
                        ps.setInt(1,1);  ps.execute();
                        ps.setInt(1,10); ps.execute();
                        ps.setInt(1,5);  ps.execute();
                        ps.setInt(1,2);  ps.execute();
                        ps.setInt(1,7);  ps.execute();
                        ps.setInt(1,90); ps.execute();
                        ps.setInt(1,4);  ps.execute();


                        ps = spliceClassWatcher.prepareStatement("insert into "+ spliceTableWatcher6+" values (?)");
                        ps.setInt(1,1);  ps.execute();
                        ps.setInt(1,10); ps.execute();
                        ps.setInt(1,10); ps.execute();
                        ps.setInt(1,50); ps.execute();
                        ps.setInt(1,20); ps.execute();
                        ps.setInt(1,70); ps.execute();
                        ps.setInt(1,2);  ps.execute();
                        ps.setInt(1,40); ps.execute();

                        spliceClassWatcher.commit();

                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                    finally {
                        spliceClassWatcher.closeAll();
                    }
                }

            });

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(CLASS_NAME);



    @Test
    public void testDF() throws Exception {

        // Run
        String q1 = format("select count(*) from %s",this.getTableReference(TABLE_NAME_1));
        String q2 = format("select count(*) from %s",this.getTableReference(TABLE_NAME_2));
        String q3 = format("select count(*) from %s",this.getTableReference(TABLE_NAME_3));
        String q4 = format("select count(*) from %s",this.getTableReference(TABLE_NAME_4));
        String q5 = format("select count(*) from %s",this.getTableReference(TABLE_NAME_5));

        String qp1 = format("call splice.testResultSetToDF('%s')",this.getTableReference(TABLE_NAME_1));
        String qp2 = format("call splice.testResultSetToDF('%s')",this.getTableReference(TABLE_NAME_2));
        String qp3 = format("call splice.testResultSetToDF('%s')",this.getTableReference(TABLE_NAME_3));
        String qp4 = format("call splice.testResultSetToDF('%s')",this.getTableReference(TABLE_NAME_4));
        String qp5 = format("call splice.testResultSetToDF('%s')",this.getTableReference(TABLE_NAME_5));

        // Run client side count(*) queries
        ResultSet rsc1 = methodWatcher.executeQuery(q1);
        ResultSet rsc2 = methodWatcher.executeQuery(q2);
        ResultSet rsc3 = methodWatcher.executeQuery(q3);
        ResultSet rsc4 = methodWatcher.executeQuery(q4);
        ResultSet rsc5 = methodWatcher.executeQuery(q5);

        // Run server-side Stored Procedures converting to DataFrames and returning count of DataFrame
        ResultSet rss1 = methodWatcher.executeQuery(qp1);
        ResultSet rss2 = methodWatcher.executeQuery(qp2);
        ResultSet rss3 = methodWatcher.executeQuery(qp3);
        ResultSet rss4 = methodWatcher.executeQuery(qp4);
        ResultSet rss5 = methodWatcher.executeQuery(qp5);

        Assert.assertTrue(rsc1.next());
        Assert.assertTrue(rsc2.next());
        Assert.assertTrue(rsc3.next());
        Assert.assertTrue(rsc4.next());
        Assert.assertTrue(rsc5.next());

        Assert.assertTrue(rss1.next());
        Assert.assertTrue(rss2.next());
        Assert.assertTrue(rss3.next());
        Assert.assertTrue(rss4.next());
        Assert.assertTrue(rss5.next());

        assertEquals("q1: Incorrect counts!",rsc1.getInt(1), rss1.getLong(1));
        assertEquals("q2: Incorrect counts!",rsc2.getInt(1), rss2.getLong(1));
        assertEquals("q3: Incorrect counts!",rsc3.getInt(1), rss3.getLong(1));
        assertEquals("q4: Incorrect counts!",rsc4.getInt(1), rss4.getLong(1));
        assertEquals("q5: Incorrect counts!",rsc5.getInt(1), rss5.getLong(1));

    }


    @Test
    public void testDFNth() throws Exception {
        String[] tests = {
                format("call splice.testNth('%s','%s',%d,%d)", this.getTableReference(TABLE_NAME_1), "String", 0, 1),
                format("call splice.testNth('%s','%s',%d,%d)", this.getTableReference(TABLE_NAME_1), "String", 0, 2),
                format("call splice.testNth('%s','%s',%d,%d)", this.getTableReference(TABLE_NAME_1), "String", 0, 3),
                format("call splice.testNth('%s','%s',%d,%d)", this.getTableReference(TABLE_NAME_1), "String", 1, 1),
                format("call splice.testNth('%s','%s',%d,%d)", this.getTableReference(TABLE_NAME_1), "String", 1, 2),
                format("call splice.testNth('%s','%s',%d,%d)", this.getTableReference(TABLE_NAME_1), "String", 1, 3),
                format("call splice.testNth('%s','%s',%d,%d)", this.getTableReference(TABLE_NAME_1), "String", 2, 1),
                format("call splice.testNth('%s','%s',%d,%d)", this.getTableReference(TABLE_NAME_1), "String", 2, 2),
                format("call splice.testNth('%s','%s',%d,%d)", this.getTableReference(TABLE_NAME_1), "String", 2, 3),

                format("call splice.testNth('%s','%s',%d,%d)",this.getTableReference(TABLE_NAME_2), "String", 0, 1),
                format("call splice.testNth('%s','%s',%d,%d)",this.getTableReference(TABLE_NAME_2), "Double", 0, 2),
                format("call splice.testNth('%s','%s',%d,%d)",this.getTableReference(TABLE_NAME_2), "Timestamp", 0, 3),
                format("call splice.testNth('%s','%s',%d,%d)",this.getTableReference(TABLE_NAME_2), "String", 1, 1),
                format("call splice.testNth('%s','%s',%d,%d)",this.getTableReference(TABLE_NAME_2), "Double", 1, 2),
                format("call splice.testNth('%s','%s',%d,%d)",this.getTableReference(TABLE_NAME_2), "Timestamp", 1, 3),
                format("call splice.testNth('%s','%s',%d,%d)",this.getTableReference(TABLE_NAME_2), "String", 2, 1),
                format("call splice.testNth('%s','%s',%d,%d)",this.getTableReference(TABLE_NAME_2), "Double", 2, 2),
                format("call splice.testNth('%s','%s',%d,%d)",this.getTableReference(TABLE_NAME_2), "Timestamp", 2, 3),

                format("call splice.testNth('%s','%s',%d,%d)",this.getTableReference(TABLE_NAME_3), "Boolean", 0, 1),
                format("call splice.testNth('%s','%s',%d,%d)",this.getTableReference(TABLE_NAME_3), "Boolean", 1, 1),
                format("call splice.testNth('%s','%s',%d,%d)",this.getTableReference(TABLE_NAME_3), "Boolean", 2, 1),

                format("call splice.testNth('%s','%s',%d,%d)",this.getTableReference(TABLE_NAME_4), "Integer", 0, 1),
                format("call splice.testNth('%s','%s',%d,%d)",this.getTableReference(TABLE_NAME_4), "String", 0, 2),
                format("call splice.testNth('%s','%s',%d,%d)",this.getTableReference(TABLE_NAME_4), "Integer", 1, 1),
                format("call splice.testNth('%s','%s',%d,%d)",this.getTableReference(TABLE_NAME_4), "String", 1, 2),
                format("call splice.testNth('%s','%s',%d,%d)",this.getTableReference(TABLE_NAME_4), "Integer", 2, 1),
                format("call splice.testNth('%s','%s',%d,%d)",this.getTableReference(TABLE_NAME_4), "String", 2, 2),
                format("call splice.testNth('%s','%s',%d,%d)",this.getTableReference(TABLE_NAME_4), "Integer", 3, 1),
                format("call splice.testNth('%s','%s',%d,%d)",this.getTableReference(TABLE_NAME_4), "String", 3, 2),
                format("call splice.testNth('%s','%s',%d,%d)",this.getTableReference(TABLE_NAME_4), "Integer", 4, 1),
                format("call splice.testNth('%s','%s',%d,%d)",this.getTableReference(TABLE_NAME_4), "String", 4, 2),
                format("call splice.testNth('%s','%s',%d,%d)",this.getTableReference(TABLE_NAME_4), "Integer", 5, 1),
                format("call splice.testNth('%s','%s',%d,%d)",this.getTableReference(TABLE_NAME_4), "String", 5, 2),
                format("call splice.testNth('%s','%s',%d,%d)",this.getTableReference(TABLE_NAME_4), "Integer", 6, 1),
                format("call splice.testNth('%s','%s',%d,%d)",this.getTableReference(TABLE_NAME_4), "String", 6, 2),

                format("call splice.testNth('%s','%s',%d,%d)",this.getTableReference(TABLE_NAME_5), "Integer", 0, 1),
                format("call splice.testNth('%s','%s',%d,%d)",this.getTableReference(TABLE_NAME_5), "Integer", 1, 1),
                format("call splice.testNth('%s','%s',%d,%d)",this.getTableReference(TABLE_NAME_5), "Integer", 2, 1),
                format("call splice.testNth('%s','%s',%d,%d)",this.getTableReference(TABLE_NAME_5), "Integer", 3, 1),
                format("call splice.testNth('%s','%s',%d,%d)",this.getTableReference(TABLE_NAME_5), "Integer", 4, 1),
                format("call splice.testNth('%s','%s',%d,%d)",this.getTableReference(TABLE_NAME_5), "Integer", 7, 1)
        };

        for (int i=0; i < tests.length; ++i) {
            ResultSet rss = methodWatcher.executeQuery(tests[i]); // Call procedure
            Assert.assertTrue("Test: "+ tests[i]+" failed conversion to DF!",rss.next());
            Assert.assertTrue("Test: "+ tests[i]+" has values that do not match!", rss.getBoolean(1)); // assert that procedure validated elements were equal
        }
    }


    // Method for stored procedure that converts a given table into a DataFrame and returns count() of DataFrame
    public static void testResultSetToDF(String table, ResultSet[] resultSets) throws SQLException {

    try{
        Connection conn = DriverManager.getConnection("jdbc:default:connection");
        PreparedStatement pstmt = conn.prepareStatement("select * from " + table.toUpperCase());
        ResultSet res = pstmt.executeQuery();
        // Convert result set to Dataframe
        Dataset<Row> resultSetDF = SparkUtils.resultSetToDF(res);
        resultSets[0] = res;

            // Construct Stored Procedure Result
            List<ExecRow> rows = Lists.newArrayList();
            ExecRow row = new ValueRow(1);
            // System.out.println(resultSetDF.rdd().count());
            row.setColumn(1, new SQLLongint(resultSetDF.count()));
            rows.add(row);
            IteratorNoPutResultSet resultsToWrap = wrapResults((EmbedConnection) conn, rows, DATAFRAME_COUNT_STORED_PROCEDURE_COLUMN_DECSRIPTOR);
            resultSets[0] = new EmbedResultSet40((EmbedConnection)conn, resultsToWrap, false, null, true);

            conn.close();
        }
        catch (StandardException e) {
            throw new SQLException(Throwables.getRootCause(e));
        }
    }


    // Converts a table to a dataframe and tests a single specified cell to make sure the dataframe and result set have equal values
    // Uses ExecRow offsets where Rows start at 0, Columns start at 1
    // returns a Boolean value as a ResultSet indicating whether the cells match
    public static void testNth(String table, String type, Integer nthRow, Integer nthCol, ResultSet[] resultSets) throws SQLException {

        try{
            Connection conn = DriverManager.getConnection("jdbc:default:connection");
            PreparedStatement pstmt = conn.prepareStatement("select * from " + table.toUpperCase());
            ResultSet res = pstmt.executeQuery();

            // Convert result set to Dataframe
            Dataset<Row> resultSetDF = SparkUtils.resultSetToDF(res);
            //Retrieve nthRow of DataFrame
            org.apache.spark.sql.Row[] r = (Row[])resultSetDF.collect();


            //Retrieve nthRow of ResultSet
            int i = 0;
            Boolean equalsTest = false;
            while(res.next() && i<nthRow){
                i++;
            }
            //System.out.println("Type="+type+"nthrow="+nthRow+" nthcol="+nthCol+" rs="+res.getObject(nthCol)+" i="+i+" df="+r[i]);

            // if either null both have to be null
            if (res.getObject(nthCol) == null) {
                equalsTest = r[i].isNullAt(nthCol-1);
            }
            else if(r[i].isNullAt(nthCol-1)) {
                equalsTest = false;
            }
            else {

                // Test nth element of ResultSet
                switch (type.toLowerCase()){
                    case "string":
                            equalsTest = res.getString(nthCol).equals(r[i].getString(nthCol-1));
                            break;
                    case "integer":
                        equalsTest = res.getInt(nthCol) == r[i].getInt(nthCol-1);
                        break;
                    case "boolean":
                        equalsTest = res.getBoolean(nthCol) == r[i].getBoolean(nthCol-1);
                        break;
                    case "double":
                        equalsTest = res.getDouble(nthCol) == (r[i].getDouble(nthCol-1));
                        break;
                    case "timestamp":
                        equalsTest = res.getTimestamp(nthCol).equals(r[i].getTimestamp(nthCol-1));
                        break;
                    default: equalsTest = false;
                        break;
                }
              }

            // Construct Stored Procedure Result
            List<ExecRow> rows = Lists.newArrayList();
            ExecRow row = new ValueRow(1);
            row.setColumn(1, new SQLBoolean(equalsTest));
            rows.add(row);
            IteratorNoPutResultSet resultsToWrap = wrapResults((EmbedConnection) conn, rows, DATAFRAME_NTH_STORED_PROCEDURE_COLUMN_DECSRIPTOR);
            resultSets[0] = new EmbedResultSet40((EmbedConnection)conn, resultsToWrap, false, null, true);
            conn.close();
        }
        catch (Exception e) {
            throw new SQLException(Throwables.getRootCause(e));
        }
    }


    private static class Triplet implements Comparable<Triplet>{
        private final String k1;
        private final String k2;
        private final String k3;

        public Triplet(String k1, String k2,String k3){
            this.k1 = k1;
            this.k2 = k2;
            this.k3 = k3;
        }

        @Override
        public String toString() {
            return "("+k1+","+k2+","+k3+")";
        }

        @Override
        public int compareTo(Triplet other){
            int compare = k1.compareTo(other.k1);
            if(compare==0){
                compare = k2.compareTo(other.k2);
                if (compare ==0)
                    compare = k3.compareTo(other.k3);
            }
            return compare;

        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((k1 == null) ? 0 : k1.hashCode());
            result = prime * result + ((k2 == null) ? 0 : k2.hashCode());
            result = prime * result + ((k3 == null) ? 0 : k3.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (!(obj instanceof Triplet))
                return false;
            Triplet other = (Triplet) obj;
            if (k1 == null) {
                if (other.k1 != null)
                    return false;
            } else if (!k1.equals(other.k1))
                return false;
            if (k2 == null) {
                if (other.k2 != null)
                    return false;
            } else if (!k2.equals(other.k2))
                return false;
            if (k3 == null) {
                if (other.k3 != null)
                    return false;
            } else if (!k3.equals(other.k3))
                return false;
            return true;
        }
    }

    // Helper method to construct the return value of the Stored Procedure
    // Create a IteratorNoPutResultSet and insert an activation, column descriptor, and open the ResultSet
    private static IteratorNoPutResultSet wrapResults(EmbedConnection conn, Iterable<ExecRow> rows, ResultColumnDescriptor[] colDesc) throws
            StandardException {
        Activation lastActivation = conn.getLanguageConnection().getLastActivation();
        IteratorNoPutResultSet resultsToWrap = new IteratorNoPutResultSet(rows, colDesc,
                lastActivation);
        resultsToWrap.openCore();
        return resultsToWrap;
    }

}
