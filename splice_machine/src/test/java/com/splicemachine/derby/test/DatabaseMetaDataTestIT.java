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

package com.splicemachine.derby.test;

import com.splicemachine.db.shared.common.sql.Utils;
import com.splicemachine.derby.test.framework.*;
import com.splicemachine.homeless.TestUtils;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

public class DatabaseMetaDataTestIT {

    // This class exists because relying on SpliceProcedureWatcher to maintain the procedure (mainly drop-if-exists part) would
    // use the same code path we want to test which defeats the purpose of the test itself. Therefore we add this class with
    // custom drop-if-exists logic that doesn't access the code paths we want to test.
    static class SpliceProcedureWatcherWithCustomDrop extends SpliceProcedureWatcher {
        private static final Logger LOG = Logger.getLogger(SpliceProcedureWatcherWithCustomDrop.class);
        public SpliceProcedureWatcherWithCustomDrop(String procedureName, String schemaName, String createString) {
            super(procedureName, schemaName, createString);
        }

        public SpliceProcedureWatcherWithCustomDrop(String procedureName,String schemaName, String createString, String userName, String password) {
            super(procedureName, schemaName, createString, userName, password);
        }

        @Override
        protected void dropIfExists(Connection connection) throws SQLException {
            String metadataQuery = String.format("SELECT COUNT(*) FROM SYS.SYSALIASES A, SYSVW.SYSSCHEMASVIEW S WHERE A.ALIASTYPE = 'P' AND S.SCHEMANAME  LIKE '%s' AND A.ALIAS LIKE '%s' ESCAPE '\\' AND A.SCHEMAID = S.SCHEMAID", schemaName.toUpperCase(), functionName.toUpperCase());
            Statement statement = connection.createStatement();
            try (ResultSet rs = statement.executeQuery(metadataQuery)) {
                rs.next();
                boolean exists = rs.getInt(1) == 1;
                if (exists) {
                    executeDrop(schemaName, functionName);
                }
            } catch (Exception e) {
                LOG.error("error Dropping " + e.getMessage());
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }
    }

    private static final String CLASS_NAME = DatabaseMetaDataTestIT.class.getSimpleName().toUpperCase();
    private static final SpliceWatcher spliceClassWatcher = new SpliceWatcher(CLASS_NAME);
    private static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);
    private static SpliceTableWatcher spliceTableWatcher1 = new SpliceTableWatcher("t1test", spliceSchemaWatcher.schemaName, "(t1n numeric(10,2) default null, t1c char(10))");
    private static SpliceTableWatcher spliceTableWatcher2 = new SpliceTableWatcher("a_b", spliceSchemaWatcher.schemaName, "(correct int)");
    private static SpliceTableWatcher spliceTableWatcher3 = new SpliceTableWatcher("aXb", spliceSchemaWatcher.schemaName, "(incorrect int)");
    private static SpliceTableWatcher spliceTableWatcher4 = new SpliceTableWatcher("aYb", spliceSchemaWatcher.schemaName, "(incorrect int)");
    private static SpliceTableWatcher spliceTableWatcher5 = new SpliceTableWatcher("aZb", spliceSchemaWatcher.schemaName, "(incorrect int)");
    private static SpliceTableWatcher spliceTableWatcher6 = new SpliceTableWatcher("\"A%B\"", spliceSchemaWatcher.schemaName, "(incorrect int)");
    private static SpliceTableWatcher spliceTableWatcher7 = new SpliceTableWatcher("\"A\\B\"", spliceSchemaWatcher.schemaName, "(incorrect int)");
    private static SpliceTableWatcher spliceTableWatcher8  = new SpliceTableWatcher("\"TEMP  _TABLE\"", spliceSchemaWatcher.schemaName, "(R8 int)");
    private static SpliceTableWatcher spliceTableWatcher9  = new SpliceTableWatcher("\"TEMP ESCAPE '\\\\' TABLE\"", spliceSchemaWatcher.schemaName, "(R9 int)");
    private static SpliceTableWatcher spliceTableWatcher10 = new SpliceTableWatcher("\"\\'TEMPTABLE\\'\"", spliceSchemaWatcher.schemaName, "(R10 int)");
    private static SpliceTableWatcher spliceTableWatcher11 = new SpliceTableWatcher("\"TEMP\\TABLE\"", spliceSchemaWatcher.schemaName, "(R11 int)");
    private static SpliceTableWatcher spliceTableWatcher12 = new SpliceTableWatcher("\"TEE%SSS\"", spliceSchemaWatcher.schemaName, "(R13 int)");
    private static SpliceTableWatcher spliceTableWatcher13 = new SpliceTableWatcher("\"TEE%SSSS\"", spliceSchemaWatcher.schemaName, "(R14 int)");
    private static SpliceTableWatcher spliceTableWatcher14 = new SpliceTableWatcher("coltest", spliceSchemaWatcher.schemaName, "(c1 decimal(10,2) default 3.14, c2 char(10), c3 decfloat not null, c4 varchar(4), primary key(c2, c4))");
    private static SpliceProcedureWatcher spliceProcedureWatcher1 = new SpliceProcedureWatcherWithCustomDrop("a_b", spliceSchemaWatcher.schemaName, "(correct varchar(2)) EXTERNAL NAME 'bla.returnsNothing' LANGUAGE JAVA PARAMETER STYLE JAVA");
    private static SpliceProcedureWatcher spliceProcedureWatcher2 = new SpliceProcedureWatcherWithCustomDrop("aXb", spliceSchemaWatcher.schemaName, "(incorrect varchar(2)) EXTERNAL NAME 'bla.returnsNothing' LANGUAGE JAVA PARAMETER STYLE JAVA");
    private static SpliceProcedureWatcher spliceProcedureWatcher3 = new SpliceProcedureWatcherWithCustomDrop("aYb", spliceSchemaWatcher.schemaName, "(incorrect varchar(2)) EXTERNAL NAME 'bla.returnsNothing' LANGUAGE JAVA PARAMETER STYLE JAVA");
    private static SpliceProcedureWatcher spliceProcedureWatcher4 = new SpliceProcedureWatcherWithCustomDrop("aZb", spliceSchemaWatcher.schemaName, "(incorrect varchar(2)) EXTERNAL NAME 'bla.returnsNothing' LANGUAGE JAVA PARAMETER STYLE JAVA");
    private static SpliceProcedureWatcher spliceProcedureWatcher5 = new SpliceProcedureWatcherWithCustomDrop("\"A%B\"", spliceSchemaWatcher.schemaName, "(incorrect varchar(2)) EXTERNAL NAME 'bla.returnsNothing' LANGUAGE JAVA PARAMETER STYLE JAVA");
    private static SpliceProcedureWatcher spliceProcedureWatcher6 = new SpliceProcedureWatcherWithCustomDrop("\"A\\B\"", spliceSchemaWatcher.schemaName, "(incorrect varchar(2)) EXTERNAL NAME 'bla.returnsNothing' LANGUAGE JAVA PARAMETER STYLE JAVA");

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher)
            .around(spliceTableWatcher1)
            .around(spliceTableWatcher2)
            .around(spliceTableWatcher3)
            .around(spliceTableWatcher4)
            .around(spliceTableWatcher5)
            .around(spliceTableWatcher6)
            .around(spliceTableWatcher7)
            .around(spliceTableWatcher8)
            .around(spliceTableWatcher9)
            .around(spliceTableWatcher10)
            .around(spliceTableWatcher11)
            .around(spliceTableWatcher12)
            .around(spliceTableWatcher13)
            .around(spliceTableWatcher14)
            .around(spliceProcedureWatcher1)
            .around(spliceProcedureWatcher2)
            .around(spliceProcedureWatcher3)
            .around(spliceProcedureWatcher4)
            .around(spliceProcedureWatcher5)
            .around(spliceProcedureWatcher6);

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher();

    @Test
    public void testVersionAndProductName() throws Exception {
        DatabaseMetaData dmd = methodWatcher.getOrCreateConnection().getMetaData();
        Assert.assertEquals("Splice Machine", dmd.getDatabaseProductName());
        Assert.assertEquals("10.9.2.2 - (1)", dmd.getDatabaseProductVersion());
    }

    @Test
    public void testGetSchemasCorrect() throws Exception{
        String schemaName = "TEST_SCHEMA123456";
        TestConnection conn=methodWatcher.getOrCreateConnection();
        conn.setAutoCommit(false);
        try{
            try(Statement s=conn.createStatement()){
                s.execute("create schema "+schemaName);
                conn.setSchema(schemaName);
                s.execute("create table t (a int, b int)");
            }

            DatabaseMetaData dmd=conn.getMetaData();
            try(ResultSet rs=dmd.getSchemas(null,schemaName)){
                Assert.assertTrue("Did not find sys schema!",rs.next());
                String tableSchem=rs.getString("TABLE_SCHEM");
                Assert.assertEquals("Incorrect table schema!",schemaName,tableSchem);
                Assert.assertNull("Incorrect catalog!",rs.getString("TABLE_CATALOG"));
                Assert.assertFalse("Found more than one schema for specified schema value!",rs.next());
            }
        }finally{
            conn.rollback();
        }
    }

    @Test
    public void testNullDecimalInUserTypeInDictionaryMeta() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("select * from sys.syscolumns where columnname = 'T1N'");
        Assert.assertTrue("Query Did not return, decimal serde issue",rs.next());
    }

    @Test
    public void testSysCatColumnsView() throws Exception {
        try (ResultSet rs = methodWatcher.executeQuery("select * from syscat.columns where tabname = 'COLTEST'")) {
            String expected = "TABSCHEMA       | TABNAME | COLNAME | COLNO |TYPENAME  |LENGTH | SCALE | DEFAULT | NULLS |CODEPAGE |KEYSEQ |\n" +
                    "-------------------------------------------------------------------------------------------------------------------\n" +
                    "DATABASEMETADATATESTIT | COLTEST |   C1    |   0   | DECIMAL  |  10   |   2   |  3.14   |   Y   |    0    | NULL  |\n" +
                    "DATABASEMETADATATESTIT | COLTEST |   C2    |   1   |CHARACTER |  10   |   0   |  NULL   |   N   |  1208   |   1   |\n" +
                    "DATABASEMETADATATESTIT | COLTEST |   C3    |   2   |DECFLOAT  |  16   |   0   |  NULL   |   N   |    0    | NULL  |\n" +
                    "DATABASEMETADATATESTIT | COLTEST |   C4    |   3   | VARCHAR  |   4   |   0   |  NULL   |   N   |  1208   |   2   |";
            Assert.assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        }
    }

    @Test
    public void testDescribeTable() throws Exception {
        TestConnection conn=methodWatcher.getOrCreateConnection();
        DatabaseMetaData dmd=conn.getMetaData();
        try(ResultSet rs = dmd.getColumns(null, spliceSchemaWatcher.schemaName, "A\\_B" /* simulating what ij.jj would do */, null)) {
            Assert.assertTrue(rs.next());
            Assert.assertEquals("CORRECT", rs.getString(4));
            Assert.assertFalse(rs.next());
        }
    }

    @Test
    public void testDescribeTableWithQuoting() throws Exception {
        Map<String, String> columns  = new HashMap();
        columns.put("TEMP  _TABLE", "R8");
        columns.put("TEMP ESCAPE '\\\\' TABLE", "R9");
        columns.put("\\'TEMPTABLE\\'", "R10");
        columns.put("TEMP\\TABLE", "R11");
        columns.put("TEE%SSS", "R13");
        columns.put("TEE%SSSS", "R14");

        for(Map.Entry<String, String> entry : columns.entrySet()) {
            verifyColumn(entry.getKey(), entry.getValue());
        }
    }

    private void verifyColumn(String tableName, String columnName) throws SQLException {
        TestConnection conn=methodWatcher.getOrCreateConnection();
        DatabaseMetaData dmd=conn.getMetaData();
        try(ResultSet rs = dmd.getColumns(null, spliceSchemaWatcher.schemaName, Utils.escape(tableName) /* simulating what ij.jj would do */, null)) {
            Assert.assertTrue(rs.next());
            Assert.assertEquals(columnName, rs.getString(4));
            Assert.assertFalse(rs.next());
        }
    }

    @Test
    public void testDescribeProcedure() throws Exception {
        TestConnection conn=methodWatcher.getOrCreateConnection();
        DatabaseMetaData dmd=conn.getMetaData();
        try(ResultSet rs = dmd.getProcedureColumns(null, spliceSchemaWatcher.schemaName, "A\\_B" /* simulating what ij.jj would do */, null)) {
            Assert.assertTrue(rs.next());
            Assert.assertEquals("CORRECT", rs.getString(4));
            Assert.assertFalse(rs.next());
        }
    }
}
