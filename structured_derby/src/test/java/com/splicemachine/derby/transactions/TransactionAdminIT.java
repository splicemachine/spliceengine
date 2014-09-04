package com.splicemachine.derby.transactions;

import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.derby.test.framework.TestConnection;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

/**
 * Tests around the maintenance of TransactionAdmin tools.
 *
 * @author Scott Fines
 * Date: 9/4/14
 */
public class TransactionAdminIT {

    public static final SpliceWatcher classWatcher = new SpliceWatcher();
    @ClassRule
    public static TestRule chain = RuleChain.outerRule(classWatcher);

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

    @Test(timeout=20000)
    public void testKillTransactionKillsTransaction() throws Exception {
        conn1.createStatement().execute("call SYSCS_UTIL.SYSCS_KILL_TRANSACTION("+conn2Txn+")");
        //vacuum must wait for conn2 to complete, unless it's been killed
        conn1.createStatement().execute("call SYSCS_UTIL.SYSCS_VACUUM()");
    }
}
