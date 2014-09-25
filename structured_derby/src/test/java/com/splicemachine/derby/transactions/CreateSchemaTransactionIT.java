package com.splicemachine.derby.transactions;

import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.derby.test.framework.TestConnection;
import com.splicemachine.derby.utils.ErrorState;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @author Scott Fines
 * Date: 9/25/14
 */
public class CreateSchemaTransactionIT {
    public static final SpliceWatcher classWatcher = new SpliceWatcher();
    @ClassRule
    public static TestRule chain = RuleChain.outerRule(classWatcher);

    private static TestConnection conn1;
    private static TestConnection conn2;


    @BeforeClass
    public static void setUpClass() throws Exception {
        conn1 = classWatcher.getOrCreateConnection();
        conn2 = classWatcher.createConnection();
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
        conn1.close();
        conn2.close();
    }

    @After
    public void tearDown() throws Exception {
        conn1.rollback();
        conn1.reset();
        conn2.rollback();
        conn2.reset();
    }

    @Before
    public void setUp() throws Exception {
        conn1.setAutoCommit(false);
        conn2.setAutoCommit(false);
    }

    @Test
    public void testCreateSchemaNotRecognizedUntilCommit() throws Exception {
        String schemaName = "schema1".toUpperCase();
        conn1.createStatement().execute(String.format("create schema %s",schemaName));

        ResultSet schemas = conn2.getMetaData().getSchemas(null, schemaName);
        Assert.assertFalse("schema is visible outside of transaction!",schemas.next());

        conn1.commit();
        conn2.commit();
        try{
            //the data should be clear
            schemas = conn2.getMetaData().getSchemas(null, schemaName);
            Assert.assertTrue("schema is visible outside of transaction!", schemas.next());
        }finally{
            /*
             * Try and keep the schemaspace clear. Of course, if DropSchema doesn't work
             * properly, then this won't work and you may get contamination between different
             * test runs.
             */
            conn1.createStatement().execute(String.format("drop schema %s restrict",schemaName));
            conn1.commit();
        }
    }

    @Test
    public void testCreateSchemaRollback() throws Exception {
        String schemaName = "schema2".toUpperCase();
        conn1.createStatement().execute(String.format("create schema %s",schemaName));

        ResultSet schemas = conn1.getMetaData().getSchemas(null, schemaName);
        Assert.assertTrue("schema is not visible within the same transaction!", schemas.next());
        conn1.rollback();

        schemas = conn1.getMetaData().getSchemas(null, schemaName);
        Assert.assertFalse("schema is visible after rollback!",schemas.next());
    }

    @Test
    public void testCreateSchemaWithSameNameCausesWriteConflict() throws Exception {
        String schemaName = "schema3".toUpperCase();
        conn1.createStatement().execute(String.format("create schema %s",schemaName));

        try{
            conn2.createStatement().execute(String.format("create schema %s",schemaName));
        }catch(SQLException se){
            Assert.assertEquals("Incorrect error message:"+se.getMessage(),
                    ErrorState.WRITE_WRITE_CONFLICT.getSqlState(),se.getSQLState());
        }
    }
}
