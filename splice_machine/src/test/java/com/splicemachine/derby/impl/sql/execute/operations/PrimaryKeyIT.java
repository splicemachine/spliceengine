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

package com.splicemachine.derby.impl.sql.execute.operations;

import static java.lang.String.format;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.*;
import java.util.List;

import com.splicemachine.derby.test.framework.TestConnection;
import com.splicemachine.pipeline.ErrorState;
import org.spark_project.guava.collect.Lists;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import com.splicemachine.derby.test.framework.SQLClosures;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;

public class PrimaryKeyIT {

    private static final String SCHEMA = PrimaryKeyIT.class.getSimpleName().toUpperCase();

    private static SpliceWatcher spliceClassWatcher = new SpliceWatcher(SCHEMA);

    @ClassRule
    public static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(SCHEMA);

    @BeforeClass
    public static void insertTestData() throws Exception {
        spliceClassWatcher.executeUpdate("create table A (name varchar(50),val int, CONSTRAINT FOO PRIMARY KEY(name))");
        spliceClassWatcher.executeUpdate("create table AB (name varchar(50),val int, CONSTRAINT AB_PK PRIMARY KEY(val, name))");

        spliceClassWatcher.executeUpdate("insert into A values ('sfines',1),('jleach',2),('mzweben',3),('gdavis',4),('dgf',5)");
        spliceClassWatcher.executeUpdate("insert into AB values ('dfg',1)");
    }

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(SCHEMA);

    // DB-3315: Updating row with primary key does not fail when it should.
    @Test
    public void updatePrimaryKeyOnRow() throws Exception {
        methodWatcher.executeUpdate("create table X(a varchar(9) primary key)");
        methodWatcher.executeUpdate("insert into X values('AAA')");
        methodWatcher.executeUpdate("insert into X values('MMM')");
        // should be a PK violation
        try {
            methodWatcher.executeUpdate("update X set a ='AAA' where a='MMM'");
            fail("Did not throw an exception on duplicate records on primary key");
        } catch (SQLException e) {
            assertEquals("Incorrect error returned.", "23505", e.getSQLState());
            assertEquals("Expected 2 rows in table X", 2L, (long)methodWatcher.query("select count(*) from X"));
        }
    }

    @Test
    public void testUpdateOverPrimaryKeyInSingleTransactionWorks() throws Exception{
        TestConnection conn = methodWatcher.getOrCreateConnection();
        conn.setAutoCommit(false);
        conn.setSchema(SCHEMA); //just in case the schema wasn't set already
        try(Statement s = conn.createStatement()){
            s.execute("create table T (a int, b int, CONSTRAINT PK_T_1 PRIMARY KEY(a))");
            int rowsModified = s.executeUpdate("insert into T(a,b) values (1,1),(2,1),(3,2)");
            Assert.assertEquals("Did not insert rows!",3,rowsModified);
            try{
                s.executeUpdate("update T set a = a+1 where b=1");
                Assert.fail("Did not throw a duplicate key exception");
            }catch(SQLException se){
                Assert.assertEquals("Incorrect error code!",
                        ErrorState.LANG_DUPLICATE_KEY_CONSTRAINT.getSqlState(),
                        se.getSQLState());
                Assert.assertTrue("Incorrect error message: Does not contain constraint name!",se.getMessage().contains("'PK_T_1'"));
                Assert.assertTrue("Incorrect error message: Does not contain table name!",se.getMessage().contains("'T'"));
            }
        }finally{
            conn.rollback();
            conn.reset();
        }
    }

    // DB-3013: Updating row with primary key and unique index fails.
    @Test
    public void updatePrimaryKeyOnRowWithUniqueIndex() throws Exception {
        methodWatcher.executeUpdate("create table UU(a int primary key, b int unique)");
        methodWatcher.executeUpdate("insert into UU values(1,1)");
        methodWatcher.executeUpdate("update UU set a=-10 where a=1");
        assertEquals(1L, (long)methodWatcher.query("select count(*) from UU where a=-10"));
        assertEquals(0L, (long)methodWatcher.query("select count(*) from UU where a=1"));
    }

    @Test
    public void testCannotInsertDuplicatePks() throws Exception {
        try {
            methodWatcher.executeUpdate("insert into A values ('sfines',1)");
            fail("Did not throw an exception on duplicate records on primary key");
        } catch (SQLException e) {
            assertEquals("Incorrect error returned.", "23505", e.getSQLState());
            assertEquals(1L, (long)methodWatcher.query("select count(*) from A where name = 'sfines'"));
        }
    }

    @Test
    public void testDeleteAndInsertInSameTransaction() throws Exception {
        Connection conn = methodWatcher.getOrCreateConnection();
        conn.setAutoCommit(false);
        try {
            assertEquals(1L, (long)methodWatcher.query("select count(*) from A where name='sfines'"));

            methodWatcher.executeUpdate("delete from A");
            methodWatcher.executeUpdate("insert into A (name,val) values ('sfines',2)");

            assertEquals(1L, (long)methodWatcher.query("select count(*) from A where name='sfines'"));
        } finally {
            conn.rollback();
        }
    }

    @Test
    public void testInsertAndDeleteInSameTransaction() throws Exception {
        Connection conn = methodWatcher.getOrCreateConnection();
        conn.setAutoCommit(false);
        try {
            assertEquals(0L, (long)methodWatcher.query("select count(*) from A where name='other'"));

            methodWatcher.executeUpdate("insert into A (name, val) values ('other',2)");
            assertEquals(1L, (long)methodWatcher.query("select count(*) from A where name='other'"));

            methodWatcher.executeUpdate("delete from A where name = 'other'");

            assertEquals(0L, (long)methodWatcher.query("select count(*) from A where name='other'"));
        } finally {
            conn.rollback();
        }
    }

    /* Regression test for Bug 419 */
    @Test(expected = SQLException.class, timeout = 10000)
    public void testDuplicateInsertFromSameTable() throws Exception {
        try {
            methodWatcher.executeUpdate("insert into A select * from A");
        } catch (SQLException sql) {
            assertEquals("Incorrect error returned.", "23505", sql.getSQLState());
            throw sql;
        }
    }

    @Test(timeout = 10000)
    public void testUpdateKeyColumn() throws Exception {
        Connection conn = methodWatcher.getOrCreateConnection();
        conn.setAutoCommit(false);
        try {
            methodWatcher.executeUpdate("update A set name = 'jzhang' where name = 'jleach'");

            SQLClosures.prepareExecute(conn, "select * from A where name = ?", new SQLClosures.SQLAction<PreparedStatement>() {
                @Override
                public void execute(PreparedStatement validator) throws Exception {
                    validator.setString(1, "jleach");
                    ResultSet rs = validator.executeQuery();
                    try {
                        while (rs.next()) {
                            fail("Should have returned nothing");
                        }
                    } finally {
                        rs.close();
                    }
                    validator.setString(1, "jzhang");
                    rs = validator.executeQuery();
                    try {
                        int matchCount = 0;
                        while (rs.next()) {
                            if ("jzhang".equalsIgnoreCase(rs.getString(1))) {
                                matchCount++;
                                assertEquals("Column incorrect!", 2, rs.getInt(2));
                            }
                        }
                        assertEquals("Incorrect number of updated rows!", 1, matchCount);
                    } finally {
                        rs.close();
                    }
                }
            });

        } finally {
            conn.rollback();
        }
    }

    // Test for DB-1112
    @Test(timeout = 10000)
    public void testUpdateKeyColumnWithSameValue() throws Exception {
        Connection conn = methodWatcher.getOrCreateConnection();
        conn.setAutoCommit(false);
        try {
            methodWatcher.executeUpdate("update A set name = 'dgf' where name = 'dgf'");
            methodWatcher.executeUpdate("update A set name = 'dgf' where name = 'dgf'");
            methodWatcher.executeUpdate("update A set name = 'dgf' where name = 'dgf'");
            methodWatcher.executeUpdate("update A set name = 'dgf' where name = 'dgf'");
            methodWatcher.executeUpdate("update A set name = 'dgf' where name = 'dgf'");

            PreparedStatement validator = conn.prepareStatement("select * from A where name = ?");
            validator.setString(1, "dgf");
            try (ResultSet rs = validator.executeQuery()) {
                int matchCount = 0;
                while (rs.next()) {
                    if ("dgf".equalsIgnoreCase(rs.getString(1))) {
                        matchCount++;
                        assertEquals("Column incorrect!", 5, rs.getInt(2));
                    }
                }
                assertEquals("Incorrect number of updated rows!", 1, matchCount);
            } finally {
                validator.close();
            }
        } finally {
            conn.rollback();
        }
    }

    @Test(timeout = 10000)
    public void testUpdateNonKeyColumn() throws Exception {
        Connection conn = methodWatcher.getOrCreateConnection();
        conn.setAutoCommit(false);
        try {
            methodWatcher.executeUpdate("update A set val = 20 where name = 'mzweben'");

            SQLClosures.prepareExecute(conn, "select * from A where name = ?", new SQLClosures
                .SQLAction<PreparedStatement>() {
                @Override
                public void execute(PreparedStatement validator) throws Exception {
                    validator.setString(1, "mzweben");
                    try (ResultSet rs = validator.executeQuery()) {
                        int matchCount = 0;
                        while (rs.next()) {
                            if ("mzweben".equalsIgnoreCase(rs.getString(1))) {
                                matchCount++;
                                int val = rs.getInt(2);
                                assertEquals("Column incorrect!", 20, val);
                            }
                        }
                        assertEquals("Incorrect number of updated rows!", 1, matchCount);
                    }

                }
            });
        } finally {
            conn.rollback();
        }
    }

    @Test(timeout = 10000)
    public void testScanningPrimaryKeyTableWithBaseRowLookup() throws Exception {
        ResultSet rs = methodWatcher.prepareStatement("select * from A where name = 'sfines'").executeQuery();
        assertTrue("Cannot lookup sfines by primary key", rs.next());
    }

    @Test(timeout = 10000)
    public void testScanningPrimaryKeyTableByPkOnly() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("select name from A where name = 'sfines'");
        assertTrue("Cannot lookup sfines by primary key ", rs.next());
    }

    @Test(timeout = 10000)
    public void testCanRetrievePrimaryKeysFromMetadata() throws Exception {
        doCanRetrievePrimaryKeysFromMetadataLowerCase(null, SCHEMA, "A");
    }

    @Test(timeout = 10000)
    public void testCanRetrievePrimaryKeysFromMetadataLowerCase() throws Exception {
        doCanRetrievePrimaryKeysFromMetadataLowerCase(null, SCHEMA.toLowerCase(), "a");
    }

    // DB-3528: creating non-unique index over a PK causes PK constraint to become non-affective.
    @Test
    public void testIndexOverPrimaryKey() throws Exception {
        methodWatcher.executeUpdate("create table j (a varchar(5) primary key)");
        methodWatcher.executeUpdate("insert into j values ('1'), ('2'), ('3')");
        methodWatcher.executeUpdate("create index ij on j (A)");
        try {
            methodWatcher.executeUpdate("update j set A='3' where A='2'");
            fail("Expected PK violation");
        } catch (Exception e) {
            // expected constraint violation
            assertTrue(e instanceof SQLException);
            assertEquals("23505", ((SQLException)e).getSQLState());
        }
        assertEquals(Lists.newArrayList("1","2","3"), methodWatcher.queryList("select * from j"));
        assertEquals(Lists.newArrayList("1","2","3"), methodWatcher.queryList("select * from j --SPLICE-PROPERTIES index=ij"));
    }

    // Called by the two tests above to handle variations in schemaName and tableName like upper/lower case
    private void doCanRetrievePrimaryKeysFromMetadataLowerCase(String catalogName, String schemaName, String tblName) throws Exception {
        ResultSet rs = methodWatcher.getOrCreateConnection().getMetaData().getPrimaryKeys(catalogName, schemaName, tblName);
        List<String> results = Lists.newArrayList();
        while (rs.next()) {
            String tableCat = rs.getString(1);
            String tableSchem = rs.getString(2);
            String tableName = rs.getString(3);
            String colName = rs.getString(4);
            short keySeq = rs.getShort(5);
            String pkName = rs.getString(6);
            Assert.assertNotNull("No Table name returned", tableName);
            Assert.assertNotNull("No Column name returned", colName);
            Assert.assertNotNull("No Pk Name returned", pkName);
            results.add(format("cat:%s,schema:%s,table:%s,column:%s,pk:%s,seqNum:%d",
                    tableCat, tableSchem, tableName, colName, pkName, keySeq));
        }
        assertTrue("No Pks returned!", results.size() > 0);
    }

}
