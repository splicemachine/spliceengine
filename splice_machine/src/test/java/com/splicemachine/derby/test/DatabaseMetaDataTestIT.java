package com.splicemachine.derby.test;

import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.derby.test.framework.TestConnection;
import org.junit.*;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.Statement;

public class DatabaseMetaDataTestIT {

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
}