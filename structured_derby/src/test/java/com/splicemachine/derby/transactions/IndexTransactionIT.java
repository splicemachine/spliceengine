package com.splicemachine.derby.transactions;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.derby.test.framework.TestConnection;
import com.splicemachine.derby.utils.ErrorState;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @author Scott Fines
 * Date: 8/27/14
 */
public class IndexTransactionIT {

    public static final SpliceSchemaWatcher schemaWatcher = new SpliceSchemaWatcher(InsertInsertTransactionIT.class.getSimpleName().toUpperCase());

    public static final SpliceTableWatcher table = new SpliceTableWatcher("A",schemaWatcher.schemaName,"(a int, b int, primary key (a))");

    public static final SpliceWatcher classWatcher = new SpliceWatcher();
    @ClassRule
    public static TestRule chain = RuleChain.outerRule(classWatcher)
            .around(schemaWatcher)
            .around(table);

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

    @Test(expected = SQLException.class)
    public void testCannotCreateIfActiveWritesOutstanding() throws Exception {
        int a = 1;
        int b = 1;
        PreparedStatement preparedStatement = conn1.prepareStatement("insert into " + table + " (a,b) values(?,?)");
        preparedStatement.setInt(1,a);
        preparedStatement.setInt(2,b);

        preparedStatement.executeUpdate();

        preparedStatement = conn2.prepareStatement("create index t_idx on " + table + "(a)");
        try{
            preparedStatement.execute();
        }catch(SQLException se){
            Assert.assertEquals("Incorrect error message!", ErrorState.DDL_ACTIVE_TRANSACTIONS.getSqlState(),se.getSQLState());
            String errorMessage = se.getMessage();
            Assert.assertTrue("Error message does not refer to table!",errorMessage.contains("T_IDX")||errorMessage.contains("t_idx"));
            throw se;
        }
    }
}
