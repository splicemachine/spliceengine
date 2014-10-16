package com.splicemachine.management;

import com.splicemachine.derby.management.XPlainTreeNode;
import com.splicemachine.derby.test.framework.*;
import com.splicemachine.test.SerialTest;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Deque;

/**
 * Tests for XPLAIN trace
 *
 * Created by jyuan on 7/7/14.
 */
@Category(SerialTest.class) //in serial category because of trying to get the correct statement id
public class XPlainTrace1IT extends BaseXplainIT {

    public static final String CLASS_NAME = XPlainTrace1IT.class.getSimpleName().toUpperCase();
    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
    public static final String TABLE1 = "TAB1";
    public static final String TABLE2 = "TAB2";
    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);

    private static String tableDef = "(I INT)";
    protected static SpliceTableWatcher spliceTableWatcher1 = new SpliceTableWatcher(TABLE1,CLASS_NAME, tableDef);
    protected static SpliceTableWatcher spliceTableWatcher2 = new SpliceTableWatcher(TABLE2,CLASS_NAME, tableDef);
    private static final int nrows = 10;


    public XPlainTrace1IT() {
        super();
    }


    @Rule public SpliceWatcher methodWatcher = new SpliceWatcher();


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
                        for(int i=0;i<nrows;i++){
                            ps.setInt(1,i);
                            ps.execute();
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            })
            .around(spliceTableWatcher2).around(new SpliceDataWatcher() {
                @Override
                protected void starting(Description description) {
                    PreparedStatement ps;
                    try {
                        ps = spliceClassWatcher.prepareStatement(
                                String.format("insert into %s (i) values (?)", spliceTableWatcher2));
                        for(int i=0;i<nrows;i++){
                            ps.setInt(1,i);
                            ps.execute();
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            })
            ;



    @Test
    public void testBroadcastJoin() throws Exception {
        xPlainTrace.turnOnTrace();

        String sql = "select * from " +
                CLASS_NAME + "." + TABLE1 + " t1, " + CLASS_NAME + "." + TABLE2 + " t2 " +
                "where t1.i = t2.i";
        long count = baseConnection.count(sql);
        Assert.assertEquals("Incorrect row count with XPLAIN enabled",nrows,count);
        xPlainTrace.turnOffTrace();

        long statementId = getLastStatementId();

        XPlainTreeNode operation = xPlainTrace.getOperationTree(statementId);
        String operationType = operation.getOperationType();
        System.out.println(operationType);
        Assert.assertEquals(0,operationType.compareToIgnoreCase(SpliceXPlainTrace.BROADCASTJOIN));
        Assert.assertEquals(operation.getInputRows(), 10);
        Assert.assertEquals(operation.getOutputRows(), 10);
        Assert.assertEquals(operation.getWriteRows(), 0);
        Assert.assertEquals(operation.getRemoteScanRows(), 10);
        Assert.assertTrue(operation.getInfo().contains("Join Condition:(T1.I[4:1] = T2.I[4:2])"));
    }

    @Test
    public void testMergeSortJoin() throws Exception {
        xPlainTrace.turnOnTrace();
        String sql = "select * from \n" +
                CLASS_NAME + "." + TABLE1 + " t1, " + CLASS_NAME + "." + TABLE2 + " t2 --SPLICE-PROPERTIES joinStrategy=SORTMERGE\n" +
                "where t1.i = t2.i";
        long count = baseConnection.count(sql);
        Assert.assertEquals("Incorrect row count with XPLAIN enabled",count,nrows);
        xPlainTrace.turnOffTrace();

        long statementId = getLastStatementId();
        XPlainTreeNode operation = xPlainTrace.getOperationTree(statementId);
        String operationType = operation.getOperationType();
        System.out.println(operationType);
        Assert.assertEquals(0,operationType.compareToIgnoreCase(SpliceXPlainTrace.MERGESORTJOIN));
        Assert.assertTrue(operation.getInfo().contains("Join Condition:(T1.I[4:1] = T2.I[4:2])"));
        Assert.assertEquals(nrows,operation.getInputRows());
        Assert.assertEquals(nrows,operation.getOutputRows());
        Assert.assertEquals(2*nrows,operation.getWriteRows());
        Assert.assertEquals(2*nrows,operation.getRemoteScanRows());
    }

    @Test
    public void testXPlainTraceOnOff() throws Exception {
        xPlainTrace.turnOnTrace();
        String sql = "select * from " + CLASS_NAME + "." + TABLE1;
        long count = baseConnection.count(sql);
        Assert.assertEquals("Incorrect base query with xplain enabled",nrows,count);
        xPlainTrace.turnOffTrace();

        ResultSet statementLine = getStatementsForTxn();
        Assert.assertTrue("Count not find statement line for explain query!", statementLine.next());
        long statementId = statementLine.getLong("STATEMENTID");
        Assert.assertFalse("No statement id found!",statementLine.wasNull());

        XPlainTreeNode operation = xPlainTrace.getOperationTree(statementId);
        Assert.assertTrue(operation!=null);

        // Execute the same statement. It should not be traced
        count = baseConnection.count(sql);
        Assert.assertEquals("Incorrect base query with xplain disabled",nrows,count);

        long statementRows = baseConnection.count("select * from SYS.SYSSTATEMENTHISTORY where transactionid = " + txnId);
        Assert.assertEquals("Unexpected number of entries for this transaction in SYSSTATEMENTHISTORY",1l,statementRows);

        // Turn on xplain trace and run the same sql statement. It should be traced
        xPlainTrace.turnOnTrace();
        count = baseConnection.count(sql);
        Assert.assertEquals("Incorrect base query with xplain enabled",nrows,count);
        xPlainTrace.turnOffTrace();

        statementLine = getStatementsForTxn();
        int numLines = 0;
        boolean foundOld = false;
        while(statementLine.next()){
            numLines++;
            long sId = statementLine.getLong("STATEMENTID");
            Assert.assertFalse("No statement id found!",statementLine.wasNull());
            if(sId==statementId)
                foundOld=true;
        }
        Assert.assertTrue("Old statement id was not found!", foundOld);
        Assert.assertEquals("incorrect number of statement rows!",2,numLines);
    }


    @Test
    public void testTableScan() throws Exception {
        xPlainTrace.turnOnTrace();
        String sql = "select * from " + CLASS_NAME + "." + TABLE1 + " where i > 0";
        long count =  baseConnection.count(sql);
        Assert.assertEquals("Incorrect query with XPLAIN enabled",nrows-1,count);
        xPlainTrace.turnOffTrace();

        //get the last statement id
        ResultSet statementLine = getStatementsForTxn();
        Assert.assertTrue("Count not find statement line for explain query!", statementLine.next());
        long statementId = statementLine.getLong("STATEMENTID");
        Assert.assertFalse("No statement id found!", statementLine.wasNull());

        XPlainTreeNode topOperation = xPlainTrace.getOperationTree(statementId);
        String operationType = topOperation.getOperationType();

        Assert.assertTrue("No table scan found!",operationType.contains(SpliceXPlainTrace.TABLESCAN));
        Assert.assertEquals(topOperation.getLocalScanRows(), nrows);
        String info = topOperation.getInfo();
        System.out.println(info);
        Assert.assertTrue(info.contains("Scan filter:(I[0:1] > 0), table:"));
    }

    @Test
    public void testCountStar() throws Exception {
        xPlainTrace.turnOnTrace();
        String sql = "select count(*) from " + CLASS_NAME + "." + TABLE1;
        long count =  baseConnection.getCount(sql);
        Assert.assertEquals("Incorrect count with XPLAIN enabled",nrows,count);
        xPlainTrace.turnOffTrace();
        //get the last statement id
        ResultSet statementLine = getStatementsForTxn();
        Assert.assertTrue("Count not find statement line for explain query!", statementLine.next());
        long statementId = statementLine.getLong("STATEMENTID");
        Assert.assertFalse("No statement id found!",statementLine.wasNull());

        XPlainTreeNode topOperation = xPlainTrace.getOperationTree(statementId);

        String operationType = topOperation.getOperationType();
        Assert.assertEquals(0,operationType.compareTo(SpliceXPlainTrace.PROJECTRESTRICT));
        Assert.assertEquals(topOperation.getInputRows(), 1);

        // Should be ScalarAggregate
        Deque<XPlainTreeNode> children = topOperation.getChildren();
        Assert.assertEquals(children.size(), 1);

        XPlainTreeNode child = children.getFirst();
        Assert.assertEquals(0,child.getOperationType().compareTo(SpliceXPlainTrace.SCALARAGGREGATE));
        Assert.assertEquals(child.getInputRows(), 10);
        Assert.assertEquals(child.getOutputRows(), 1);
        Assert.assertEquals(child.getWriteRows(), 1);
    }

    @Test
    @Ignore
    public void testRepeatedNestedLoopJoin() throws Exception {
        for(int i=0;i<1000;i++){
            testNestedLoopJoin();
        }
    }

    @Test
    @Ignore
    public void testRepeatedMSJThenNLJ() throws Exception {
        for(int i=0;i<1000;i++){
            System.out.println("----MergeSortJoin");
            setUp();
            testMergeSortJoin();
            tearDown();

            System.out.println("----NestedLoopJoin");
            setUp();
            testNestedLoopJoin();
            tearDown();
        }
    }

    @Test
    @Ignore("Ignored for now, since the numbers still add up physically, but the test requires some work")
    public void testNestedLoopJoin() throws Exception {
        xPlainTrace.turnOnTrace();

        String sql = "select t1.i from --SPLICE-PROPERTIES joinOrder=FIXED\n" +
                      CLASS_NAME + "." + TABLE1 + " t1, " + CLASS_NAME + "." + TABLE2 + " t2 --SPLICE-PROPERTIES joinStrategy=NESTEDLOOP\n" +
                      "where t1.i = t2.i*2";
        long count = baseConnection.count(sql);
        Assert.assertEquals(5,count);
        long statementId = getLastStatementId();
        xPlainTrace.turnOffTrace();

        XPlainTreeNode operation = xPlainTrace.getOperationTree(statementId);
        String operationType = operation.getOperationType();
        System.out.println(operationType);
        Assert.assertEquals(0,operationType.compareTo(SpliceXPlainTrace.PROJECTRESTRICT));
        Assert.assertEquals(count ,operation.getInputRows());
        Assert.assertEquals(count ,operation.getOutputRows());

        Assert.assertEquals(1,operation.getChildren().size());
        operation = operation.getChildren().getFirst();
        operationType = operation.getOperationType();
        Assert.assertEquals(0,operationType.compareTo(SpliceXPlainTrace.NESTEDLOOPJOIN));
        Assert.assertEquals(operation.getInputRows(), nrows);
        Assert.assertEquals(operation.getRemoteScanRows(), count);
        Assert.assertEquals(operation.getOutputRows(), count);

        // will have 1 child for each row on the left, plus one extra
        Assert.assertEquals(11,operation.getChildren().size());

        // First child should be a bulk table scan operation
        XPlainTreeNode child = operation.getChildren().getLast();
        operationType = child.getOperationType();
        Assert.assertEquals(0,operationType.compareTo(SpliceXPlainTrace.BULKTABLESCAN));
        Assert.assertEquals(child.getLocalScanRows(), nrows);
        Assert.assertEquals(child.getOutputRows(), nrows);

        // right child should be a project restrict operation
        child = operation.getChildren().getFirst();
        operationType = child.getOperationType();
        System.out.println(operationType);
        Assert.assertEquals(0,operationType.trim().compareToIgnoreCase(SpliceXPlainTrace.PROJECTRESTRICT));

        child = child.getChildren().getLast();
        operationType = child.getOperationType();
        System.out.println(operationType);
        Assert.assertTrue("No table scan found!",operationType.contains(SpliceXPlainTrace.TABLESCAN));
        Assert.assertEquals(child.getLocalScanRows(), nrows * nrows);
        Assert.assertEquals(child.getOutputRows(), nrows*nrows);
        Assert.assertEquals(child.getIterations(), nrows);
        String info = child.getInfo();
        System.out.println(info);
        Assert.assertTrue(info.compareToIgnoreCase("table:XPLAINTRACE1IT.TAB2")==0);
    }


    @Override
    protected TestConnection getNewConnection() throws Exception {
        return methodWatcher.createConnection();
    }
}
