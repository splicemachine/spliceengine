package com.splicemachine.derby.impl.sql.execute.actions;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.derby.test.framework.TestConnection;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.Savepoint;

/**
 *
 * @author Jeff Cunningham
 *         Date: 7/17/13
 */
public class SavepointConstantOperationIT { 
    public static final String CLASS_NAME = SavepointConstantOperationIT.class.getSimpleName().toUpperCase();
    protected static SpliceWatcher classWatcher = new SpliceWatcher();
    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);

    public static final String TABLE_NAME_1 = "B";

    private static String tableDef = "(TaskId INT NOT NULL)";
    protected static SpliceTableWatcher spliceTableWatcher1 = new SpliceTableWatcher(TABLE_NAME_1, CLASS_NAME, tableDef);

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(classWatcher)
            .around(spliceSchemaWatcher)
            .around(spliceTableWatcher1);

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
    public void testCanSetAndReleaseASavepoint() throws Exception {
        Savepoint savepoint = conn1.setSavepoint("test");
        int value = 1;
        conn1.execute(String.format("insert into %s (taskid) values (%d)",spliceTableWatcher1,value));
        conn1.releaseSavepoint(savepoint);
        long count = conn1.count(String.format("select * from %s where taskid=%d",spliceTableWatcher1,value));
        Assert.assertEquals("Incorrect count after savepoint release!",1l,count);
    }

    @Test
    public void testReleasingASavepointDoesNotCommitData() throws Exception {
        Savepoint savepoint = conn1.setSavepoint("test");
        int value = 6;
        conn1.execute(String.format("insert into %s (taskid) values (%d)",spliceTableWatcher1,value));

        long count = conn2.count(String.format("select * from %s where taskid=%d",spliceTableWatcher1,value));
        Assert.assertEquals("Data is visible to another transaction!",0l,count);

        conn1.releaseSavepoint(savepoint);
        count = conn2.count(String.format("select * from %s where taskid=%d",spliceTableWatcher1,value));
        Assert.assertEquals("Data was committed during savepoint release!",0l,count);
    }

    @Test
    public void testRollingBackASavepointMakesDataInvisibleToMyself() throws Exception {
        Savepoint savepoint = conn1.setSavepoint("test");
        int value = 2;
        conn1.execute(String.format("insert into %s (taskid) values (%d)",spliceTableWatcher1,value));
        long count = conn1.count(String.format("select * from %s where taskid=%d",spliceTableWatcher1,value));
        Assert.assertEquals("Incorrect count after savepoint release!",1l,count);

        conn1.rollback(savepoint);
        count = conn1.count(String.format("select * from %s where taskid=%d",spliceTableWatcher1,value));
        Assert.assertEquals("Incorrect count after savepoint release!",0l,count);
    }

    @Test
    public void testCanReleaseNonImmediateSavepoint() throws Exception {
        Savepoint s1 = conn1.setSavepoint("test");
        int value = 3;
        conn1.execute(String.format("insert into %s (taskid) values (%d)", spliceTableWatcher1, value));

        Savepoint s2 = conn1.setSavepoint("test2");
        conn1.execute(String.format("insert into %s (taskid) values (%d)", spliceTableWatcher1, value));

        //try releasing the first savepoint without first releasing the second, and make sure that it still works
        conn1.releaseSavepoint(s1);
        long count = conn1.count(String.format("select * from %s where taskid=%d",spliceTableWatcher1,value));
        Assert.assertEquals("Incorrect count after savepoint release!", 2l, count);
    }

    @Test
    public void testRollingBackANonImmediateSavepointMakesDataInvisible() throws Exception {
        Savepoint s1 = conn1.setSavepoint("test");
        int value = 4;
        conn1.execute(String.format("insert into %s (taskid) values (%d)", spliceTableWatcher1, value));

        Savepoint s2 = conn1.setSavepoint("test2");
        conn1.execute(String.format("insert into %s (taskid) values (%d)", spliceTableWatcher1, value));

        //make sure data looks like what we expect
        long count = conn1.count(String.format("select * from %s where taskid=%d",spliceTableWatcher1,value));
        Assert.assertEquals("Incorrect count after savepoint release!", 2l, count);

        //rollback s1 and make sure that all data is invisible
        conn1.rollback(s1);
        count = conn1.count(String.format("select * from %s where taskid=%d",spliceTableWatcher1,value));
        Assert.assertEquals("Incorrect count after savepoint release!", 0l, count);
    }

    @Test
    public void testRollingBackANonImmediateSavepointMakesDataInvisibleEvenIfOtherSavepointIsReleased() throws Exception {
        Savepoint s1 = conn1.setSavepoint("test");
        int value = 4;
        conn1.execute(String.format("insert into %s (taskid) values (%d)", spliceTableWatcher1, value));

        Savepoint s2 = conn1.setSavepoint("test2");
        conn1.execute(String.format("insert into %s (taskid) values (%d)", spliceTableWatcher1, value));

        conn1.releaseSavepoint(s2);
        //make sure data looks like what we expect
        long count = conn1.count(String.format("select * from %s where taskid=%d",spliceTableWatcher1,value));
        Assert.assertEquals("Incorrect count after savepoint release!", 2l, count);

        //rollback s1 and make sure that all data is invisible
        conn1.rollback(s1);
        count = conn1.count(String.format("select * from %s where taskid=%d",spliceTableWatcher1,value));
        Assert.assertEquals("Incorrect count after savepoint release!", 0l, count);
    }

    @Test
    public void testCanRollbackThenReleaseASavepointAndDataIsCorrect() throws Exception {
        Savepoint savepoint = conn1.setSavepoint("test");
        int value = 7;
        conn1.execute(String.format("insert into %s (taskid) values (%d)",spliceTableWatcher1,value));
        long count = conn1.count(String.format("select * from %s where taskid=%d",spliceTableWatcher1,value));
        Assert.assertEquals("Incorrect count after savepoint release!",1l,count);

        conn1.rollback(savepoint);
        count = conn1.count(String.format("select * from %s where taskid=%d",spliceTableWatcher1,value));
        Assert.assertEquals("Incorrect count after savepoint release!",0l,count);

        //insert some data again
        conn1.execute(String.format("insert into %s (taskid) values (%d)",spliceTableWatcher1,value));

        //now release the savepoint
        conn1.releaseSavepoint(savepoint);
        count = conn1.count(String.format("select * from %s where taskid=%d",spliceTableWatcher1,value));
        Assert.assertEquals("Incorrect count after savepoint release!",1l,count);


    }
}
