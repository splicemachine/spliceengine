package com.splicemachine.derby.transactions;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.derby.test.framework.TestConnection;
import com.splicemachine.pipeline.exception.ErrorState;
import com.splicemachine.test.SerialTest;

import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @author Scott Fines
 *         Date: 9/4/14
 */
//@Category(SerialTest.class) //serial until DB-1777 is resolved
public class DropTableTransactionIT {
    public static final SpliceSchemaWatcher schemaWatcher = new SpliceSchemaWatcher(DropTableTransactionIT.class.getSimpleName().toUpperCase());

    public static final SpliceTableWatcher table = new SpliceTableWatcher("A",schemaWatcher.schemaName,"(a int, b int)");

    public static final SpliceWatcher classWatcher = new SpliceWatcher();
    @ClassRule
    public static TestRule chain = RuleChain.outerRule(classWatcher)
            .around(schemaWatcher);

    private static TestConnection conn1;
    private static TestConnection conn2;

    private long conn1Txn;
    private long conn2Txn;

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
        conn1Txn = conn1.getCurrentTransactionId();
        conn2Txn = conn2.getCurrentTransactionId();
    }

    @Test
    public void testDropTableIgnoredByOtherTransactions() throws Exception {
        //create the table here to make sure that it exists
        table.start();
        //roll forward the transactions in case they were created before the creation
        tearDown();
        setUp();

        //issue the drop statement
        conn1.createStatement().execute("drop table " + table);

        //now confirm that you can keep writing and reading data from the table from conn1
        int aInt = 1;
        int bInt = 1;
        PreparedStatement ps = conn2.prepareStatement("insert into " + table+"(a,b) values (?,?)");
        ps.setInt(1,aInt);ps.setInt(2,bInt);ps.execute();

        long count = conn2.count("select * from "+ table+" where a="+aInt);
        Assert.assertEquals("Unable to read data from dropped table!",1l,count);

        //commit conn1
        conn1.commit();
        //confirm we still can write and read from it
        ps = conn2.prepareStatement("insert into " + table+"(a,b) values (?,?)");
        ps.setInt(1,aInt);ps.setInt(2,bInt);ps.execute();

        count = conn2.count("select * from "+ table+" where a="+aInt);
        Assert.assertEquals("Unable to read data from dropped table!",2l,count);

        //commit conn2. Table should be dropped to conn2 now
        conn2.commit();

        //we shouldn't be able to see the table now
        try{
            ps = conn2.prepareStatement("insert into " + table+"(a,b) values (?,?)");
        }catch(SQLException se){
            System.out.printf("%s:%s%n",se.getSQLState(),se.getMessage());
            Assert.assertEquals("Incorrect error message!", ErrorState.LANG_TABLE_NOT_FOUND.getSqlState(),se.getSQLState());
        }
    }

    @Test
    public void testDropTableRollback() throws Exception {
        //create the table here to make sure that it exists
        table.start();
        //roll forward the transactions in case they were created before the creation
        tearDown();
        setUp();

        conn1.createStatement().execute("drop table "+ table);

        conn1.rollback();

        //confirm that the table is still visible
        int aInt = 2;
        int bInt = 2;
        PreparedStatement ps = conn1.prepareStatement("insert into " + table+"(a,b) values (?,?)");
        ps.setInt(1,aInt);ps.setInt(2,bInt);ps.execute();

        long count = conn1.count("select * from "+ table+" where a="+aInt);
        Assert.assertEquals("Unable to read data from dropped table!",1l,count);
    }

    @Test
    public void testCanDropUnrelatedTablesConcurrently() throws Exception {
        new SpliceTableWatcher("t",schemaWatcher.schemaName,"(a int unique not null, b int)").start();
        new SpliceTableWatcher("t2",schemaWatcher.schemaName,"(a int unique not null, b int)").start();
        conn1.commit();
        conn2.commit(); //roll both connections forward to ensure visibility

        /*
         * Now try and drop both tables, one table in each transaction, and make sure that they do not conflict
         * with each other
         */
        conn1.createStatement().execute("drop table "+ schemaWatcher+".t");

        conn2.createStatement().execute("drop table "+ schemaWatcher+".t2");

        //commit the two transactions to make sure that the tables no longer exist
        conn1.commit();
        conn2.commit();
    }

    @Test
    public void testDroppingSameTableGivesWriteWriteConflict() throws Exception {
        new SpliceTableWatcher("t3",schemaWatcher.schemaName,"(a int unique not null, b int)").start();
        conn1.commit();
        conn2.commit(); //roll both connections forward to ensure visibility

        /*
         * Now try and drop both tables, one table in each transaction, and make sure that they do not conflict
         * with each other
         */
        conn1.createStatement().execute("drop table "+ schemaWatcher+".t3");
        try{
            conn2.createStatement().execute("drop table "+ schemaWatcher+".t3");
            Assert.fail("Did not throw a Write/Write conflict");
        }catch(SQLException se){
           Assert.assertEquals("Incorrect error message!",ErrorState.WRITE_WRITE_CONFLICT.getSqlState(),se.getSQLState());
        }finally{
            //commit the two transactions to make sure that the tables no longer exist
            conn1.commit();
        }
    }
}
