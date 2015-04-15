package com.splicemachine.derby.transactions;

import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.derby.test.framework.TestConnection;
import com.splicemachine.pipeline.exception.ErrorState;

import com.splicemachine.test.Transactions;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @author Scott Fines
 * Date: 9/25/14
 */
@Category({Transactions.class})
public class SchemaTransactionIT {
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

    @Test
    public void testDropSchemaNotRecognizedUntilCommit() throws Exception {
        String schemaName = "schema4".toUpperCase();
        try{
            conn1.createStatement().execute(String.format("create schema %s",schemaName));
        }catch(SQLException se){
            /*
             * It's possible that the schema was leftover from a previous failure. In that case,
             * we will get a specific error--when we see that error, we will drop the schema,
             * then try the test over again
             */
            if(ErrorState.LANG_OBJECT_ALREADY_EXISTS.getSqlState().equals(se.getSQLState())){
                conn1.createStatement().execute(String.format("drop schema %s",schemaName));
                conn1.commit();
                conn2.commit(); //make sure the other transaction rolls forward
                testDropSchemaNotRecognizedUntilCommit(); //re-start the test
            }
        }
        conn1.commit();
        conn2.commit();

        ResultSet schemas = conn2.getMetaData().getSchemas(null, schemaName);
        Assert.assertTrue("schema is not visible after commit!", schemas.next());
        schemas.close();

        //now drop the schema
        conn1.createStatement().execute(String.format("drop schema %s restrict",schemaName));

        schemas = conn2.getMetaData().getSchemas(null, schemaName);
        Assert.assertTrue("drop schema is visible outside of transaction!",schemas.next());
        schemas.close();

        conn1.commit();
        conn2.commit();

        schemas = conn2.getMetaData().getSchemas(null, schemaName);
        Assert.assertFalse("drop schema is still visible after commit!", schemas.next());
        schemas.close();
    }

    @Test
    public void testDropSchemaRollback() throws Exception {
        String schemaName = "schema5".toUpperCase();
        conn1.createStatement().execute(String.format("create schema %s",schemaName));
        conn1.commit();

        ResultSet schemas = conn1.getMetaData().getSchemas(null, schemaName);
        Assert.assertTrue("schema is not visible before drop!", schemas.next());

        conn1.createStatement().execute(String.format("drop schema %s restrict",schemaName));
        schemas = conn1.getMetaData().getSchemas(null, schemaName);
        Assert.assertFalse("drop schema is not visible inside same transaction!",schemas.next());

        conn1.rollback();

        schemas = conn1.getMetaData().getSchemas(null, schemaName);
        Assert.assertTrue("schema is not visible after rollback!",schemas.next());
        //actually drop it to make sure it clears
        conn1.createStatement().execute(String.format("drop schema %s restrict",schemaName));
        conn1.commit();
    }
}
