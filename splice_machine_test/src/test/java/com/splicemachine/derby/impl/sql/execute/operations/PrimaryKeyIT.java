package com.splicemachine.derby.impl.sql.execute.operations;

import com.google.common.collect.Lists;
import com.splicemachine.derby.test.framework.SQLClosures;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import org.junit.*;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import static java.lang.String.format;
import static org.junit.Assert.*;

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

    // DB-3013: Updating row with primary key and unique index fails.
    @Test
    public void updatePrimaryKeyOnRowWithUniqueIndex() throws Exception {
        methodWatcher.executeUpdate("create table UU(a int primary key, b int unique)");
        methodWatcher.executeUpdate("insert into UU values(1,1)");
        methodWatcher.executeUpdate("update UU set a=-10 where a=1");
        assertEquals(1L, methodWatcher.query("select count(*) from UU where a=-10"));
        assertEquals(0L, methodWatcher.query("select count(*) from UU where a=1"));
    }

    @Test
    public void testCannotInsertDuplicatePks() throws Exception {
        try {
            methodWatcher.executeUpdate("insert into A values ('sfines',1)");
            fail("Did not throw an exception on duplicate records on primary key");
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("identified by 'FOO' defined on 'A'"));
            assertEquals(1L, methodWatcher.query("select count(*) from A where name = 'sfines'"));
        }
    }

    @Test
    public void testDeleteAndInsertInSameTransaction() throws Exception {
        Connection conn = methodWatcher.getOrCreateConnection();
        conn.setAutoCommit(false);
        try {
            assertEquals(1L, methodWatcher.query("select count(*) from A where name='sfines'"));

            methodWatcher.executeUpdate("delete from A");
            methodWatcher.executeUpdate("insert into A (name,val) values ('sfines',2)");

            assertEquals(1L, methodWatcher.query("select count(*) from A where name='sfines'"));
        } finally {
            conn.rollback();
        }
    }

    @Test
    public void testInsertAndDeleteInSameTransaction() throws Exception {
        Connection conn = methodWatcher.getOrCreateConnection();
        conn.setAutoCommit(false);
        try {
            assertEquals(0L, methodWatcher.query("select count(*) from A where name='other'"));

            methodWatcher.executeUpdate("insert into A (name, val) values ('other',2)");
            assertEquals(1L, methodWatcher.query("select count(*) from A where name='other'"));

            methodWatcher.executeUpdate("delete from A where name = 'other'");

            assertEquals(0L, methodWatcher.query("select count(*) from A where name='other'"));
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
            assertTrue("Incorrect error returned!", sql.getSQLState().contains("23505"));
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
            ResultSet rs = validator.executeQuery();
            try {
                int matchCount = 0;
                while (rs.next()) {
                    if ("dgf".equalsIgnoreCase(rs.getString(1))) {
                        matchCount++;
                        assertEquals("Column incorrect!", 5, rs.getInt(2));
                    }
                }
                assertEquals("Incorrect number of updated rows!", 1, matchCount);
            } finally {
                rs.close();
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

            SQLClosures.prepareExecute(conn, "select * from A where name = ?", new SQLClosures.SQLAction<PreparedStatement>() {
                @Override
                public void execute(PreparedStatement validator) throws Exception {
                    validator.setString(1, "mzweben");
                    ResultSet rs = validator.executeQuery();
                    try {
                        int matchCount = 0;
                        while (rs.next()) {
                            if ("mzweben".equalsIgnoreCase(rs.getString(1))) {
                                matchCount++;
                                int val = rs.getInt(2);
                                assertEquals("Column incorrect!", 20, val);
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
