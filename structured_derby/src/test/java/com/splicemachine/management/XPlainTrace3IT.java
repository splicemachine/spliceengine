package com.splicemachine.management;

import com.splicemachine.derby.management.XPlainTreeNode;
import com.splicemachine.derby.test.framework.*;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;

import java.sql.PreparedStatement;

/**
 * Created by jyuan on 7/15/14.
 */
public class XPlainTrace3IT {
    private static SpliceXPlainTrace xPlainTrace = new SpliceXPlainTrace();
    private static TestConnection baseConnection;
    private static int nrows = 10;

    public XPlainTrace3IT() {

    }

    public static final String CLASS_NAME = XPlainTrace3IT.class.getSimpleName().toUpperCase();
    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
    public static final String TABLE1 = "T1";

    public static final String TABLE2 = "T2";
    public static final String TABLE3 = "T3";
    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);

    private static String tableDef = "(I INT)";
    protected static SpliceTableWatcher spliceTableWatcher1 = new SpliceTableWatcher(TABLE1,CLASS_NAME, tableDef);
    protected static SpliceTableWatcher spliceTableWatcher2 = new SpliceTableWatcher(TABLE2,CLASS_NAME, tableDef);
    protected static SpliceTableWatcher spliceTableWatcher3 = new SpliceTableWatcher(TABLE3,CLASS_NAME, tableDef);
    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher();

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher)
            .around(spliceTableWatcher1).around(new SpliceDataWatcher() {
                @Override
                protected void starting(Description description) {
                    PreparedStatement ps;
                    try {
                        ps = spliceClassWatcher.prepareStatement(
                                String.format("insert into %s (i) values (?)", spliceTableWatcher1));
                        for (int i = 0; i < nrows; i++) {
                            ps.setInt(1, i);
                            ps.execute();
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            })
            .around(spliceTableWatcher2)
            .around(spliceTableWatcher3).around(new SpliceDataWatcher() {
                    @Override
                    protected void starting(Description description) {
                        PreparedStatement ps;
                        try {
                            ps = spliceClassWatcher.prepareStatement(
                                    String.format("insert into %s (i) values (?)", spliceTableWatcher3));
                            for (int i = 0; i < nrows; i++) {
                                ps.setInt(1, i);
                                ps.execute();
                            }
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }
            });

    @BeforeClass
    public static void setUpClass() throws Exception {
        baseConnection = new TestConnection(SpliceNetConnection.getConnection());
        xPlainTrace.setConnection(baseConnection);
    }

    @AfterClass
    public static void tearDown() throws Exception {
        baseConnection.close();
    }

    @Test
    public void testInsertAndCreateIndex() throws Exception {
        xPlainTrace.turnOnTrace();
        String sql = "insert into " + CLASS_NAME + "." + TABLE2 + " values 1, 2";
        boolean success = xPlainTrace.execute(sql);
        long statementId = baseConnection.getLastStatementId();
        xPlainTrace.turnOffTrace();

        XPlainTreeNode operation = xPlainTrace.getOperationTree(statementId);
        Assert.assertEquals(0,operation.getOperationType().compareToIgnoreCase(SpliceXPlainTrace.INSERT));
        Assert.assertEquals(operation.getInputRows(), 2);
        Assert.assertEquals(2, operation.getWriteRows());

        operation = operation.getChildren().getFirst();
        Assert.assertEquals(0,operation.getOperationType().compareToIgnoreCase(SpliceXPlainTrace.UNION));
        Assert.assertEquals(2, operation.getInputRows());

        for(XPlainTreeNode child:operation.getChildren()) {
            Assert.assertEquals(0,child.getOperationType().compareToIgnoreCase(SpliceXPlainTrace.ROW));
        }

        xPlainTrace.turnOnTrace();
        String ddl = "create index ti on " + CLASS_NAME + "." + TABLE2 + "(i)";
        xPlainTrace.execute(ddl);
        statementId = baseConnection.getLastStatementId();
        xPlainTrace.turnOffTrace();

        operation = xPlainTrace.getOperationTree(statementId);
        String operationType = operation.getOperationType();
        System.out.println(operationType);
        Assert.assertEquals(0, operationType.compareToIgnoreCase(SpliceXPlainTrace.POPULATEINDEX));
        Assert.assertEquals(2, operation.getOutputRows());
        Assert.assertEquals(2, operation.getWriteRows());
        //Assert.assertEquals(2, operation.getLocalScanRows());
    }

    @Test
    public void testUpdate() throws Exception {
        xPlainTrace.turnOnTrace();
        String sql = "update " + CLASS_NAME + "." + TABLE3 + " set i=i+1";
        boolean success = xPlainTrace.execute(sql);
        long statementId = baseConnection.getLastStatementId();
        xPlainTrace.turnOffTrace();

        XPlainTreeNode operation = xPlainTrace.getOperationTree(statementId);
        Assert.assertTrue(operation.getOperationType().compareToIgnoreCase(SpliceXPlainTrace.UPDATE)==0);
        Assert.assertEquals(nrows, operation.getWriteRows());
        Assert.assertEquals(nrows, operation.getInputRows());
    }

    @Test
    public void testDelete() throws Exception {
        xPlainTrace.turnOnTrace();
        String sql = "delete from " + CLASS_NAME + "." + TABLE1;
        boolean success = xPlainTrace.execute(sql);
        long statementId = baseConnection.getLastStatementId();
        xPlainTrace.turnOffTrace();

        XPlainTreeNode operation = xPlainTrace.getOperationTree(statementId);
        Assert.assertTrue(operation.getOperationType().compareToIgnoreCase(SpliceXPlainTrace.DELETE)==0);
        Assert.assertEquals(nrows, operation.getWriteRows());
        Assert.assertEquals(nrows, operation.getInputRows());
    }
}
