package com.splicemachine.management;

import com.splicemachine.derby.impl.sql.catalog.SpliceXplainUtils;
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
import java.util.regex.Pattern;

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


    @Rule public SpliceWatcher methodWatcher = new SpliceWatcher(CLASS_NAME);


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
                        ps.close();
                        ps = spliceClassWatcher.prepareStatement("create index t1i on " + CLASS_NAME +".tab1 (i)");
                        ps.execute();
                        ps.close();
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
                        ps.close();
                        ps = spliceClassWatcher.prepareStatement("create index t2i on " + CLASS_NAME +".tab2 (i)");
                        ps.execute();
                        ps.close();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            })
            ;



    @Test
    public void testBroadcastJoin() throws Exception {
        xPlainTrace.turnOnTrace();

        String sql = String.format("select * from %s t1,%s t2 --SPLICE-PROPERTIES joinStrategy=BROADCAST \n" +
                " where t1.i = t2.i",spliceTableWatcher1,spliceTableWatcher2);
        long count = baseConnection.count(sql);
        Assert.assertEquals("Incorrect row count with XPLAIN enabled",nrows,count);
        xPlainTrace.turnOffTrace();

        long statementId = getLastStatementId();

        XPlainTreeNode operation = xPlainTrace.getOperationTree(statementId);
        String operationType = operation.getOperationType();
        System.out.println(operationType);
        if (operationType.compareToIgnoreCase(SpliceXPlainTrace.PROJECTRESTRICT) == 0) {
            operation = operation.getChildren().getFirst();
            operationType = operation.getOperationType();
            System.out.println(operationType);
        }
        Assert.assertEquals(SpliceXPlainTrace.BROADCASTJOIN.toUpperCase(),operationType.toUpperCase());
        Assert.assertEquals(operation.getInputRows(), 10);
        Assert.assertEquals(operation.getOutputRows(), 10);
        Assert.assertEquals(operation.getWriteRows(), 0);
        Assert.assertEquals(operation.getRemoteScanRows(), 10);
        if (operation.getInfo() != null) {
            Assert.assertTrue(operation.getInfo().contains("Join Condition:"));
        }
        Deque<XPlainTreeNode> children = operation.getChildren();
        Assert.assertEquals(2, children.size());
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
        Assert.assertEquals(operationType.toUpperCase(), SpliceXPlainTrace.MERGESORTJOIN.toUpperCase());
        Assert.assertTrue(operation.getInfo().contains("Join Condition:(T1.I[4:1] = T2.I[4:2])"));
        Assert.assertEquals(nrows,operation.getInputRows());
        Assert.assertEquals(nrows,operation.getOutputRows());
        Assert.assertEquals(2*nrows,operation.getWriteRows());
        Assert.assertEquals(2*nrows,operation.getRemoteScanRows());
    }

    @Test
    public void testMergeJoin() throws Exception{
        try {
            xPlainTrace.turnOnTrace();
            String sql = "select * from \n" +
                    CLASS_NAME + "." + TABLE1 + " t1 --SPLICE-PROPERTIES index=t1i\n" +
                    "," + CLASS_NAME + "." + TABLE2 + " t2 --SPLICE-PROPERTIES index=t2i, joinStrategy=MERGE\n" +
                    "where t1.i = t2.i";
            long count = baseConnection.count(sql);
            Assert.assertEquals("Incorrect row count with XPLAIN enabled", count, nrows);
            xPlainTrace.turnOffTrace();

            long statementId = getLastStatementId();
            XPlainTreeNode operation = xPlainTrace.getOperationTree(statementId);
            String operationType = operation.getOperationType();
            Assert.assertEquals(operationType.toUpperCase(), SpliceXPlainTrace.MERGEJOIN.toUpperCase());
            Assert.assertTrue(operation.getInfo().contains("Join Condition:(T1.I[4:1] = T2.I[4:2])"));
            Assert.assertEquals(nrows,operation.getInputRows());
            Assert.assertEquals(nrows,operation.getOutputRows());
            Assert.assertEquals(nrows,operation.getRemoteScanRows());

            Deque<XPlainTreeNode> children = operation.getChildren();
            Assert.assertEquals(2, children.size());
        }
        finally {
//            methodWatcher.executeUpdate("drop index t1i");
//            methodWatcher.executeUpdate("drop index t2i");
        }
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
    public void testRepeatedTableScan() throws Exception{
        for(int i=0;i<1000;i++){
            testTableScan();
        }
    }

    @Test
    public void testTableScan() throws Exception {
        xPlainTrace.turnOnTrace();
        String sql = "select * from " + spliceTableWatcher1 + " where i > 0";
        long count =  baseConnection.count(sql);
        Assert.assertEquals("Incorrect query with XPLAIN enabted",nrows-1,count);
        xPlainTrace.turnOffTrace();

        //get the last statement id
        ResultSet statementLine = getStatementsForTxn();
        Assert.assertTrue("Count not find statement line for explain query!", statementLine.next());
        long statementId = statementLine.getLong("STATEMENTID");
        Assert.assertFalse("No statement id found!",statementLine.wasNull());

        XPlainTreeNode topOperation = xPlainTrace.getOperationTree(statementId);
        String operationType = topOperation.getOperationType();

        Assert.assertTrue("No table scan found!",operationType.contains(SpliceXPlainTrace.TABLESCAN));
        Assert.assertEquals("Incorrect output row count!",count,topOperation.getOutputRows());
        Assert.assertEquals("Output rows do not match local + filtered!",
                topOperation.getOutputRows(),topOperation.getLocalScanRows()-topOperation.getFilteredRows());
        String info = topOperation.getInfo();
//        System.out.println(info);
        Assert.assertTrue("Info incorrect: info="+info,info.contains("Scan filter:(I[0:1] > 0),"));
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
        Assert.assertEquals(operationType, SpliceXPlainTrace.PROJECTRESTRICT);
        Assert.assertEquals(topOperation.getInputRows(), 1);

        // Should be ScalarAggregate
        Deque<XPlainTreeNode> children = topOperation.getChildren();
        Assert.assertEquals(children.size(), 1);

        XPlainTreeNode child = children.getFirst();
        Assert.assertEquals(child.getOperationType(), SpliceXPlainTrace.SCALARAGGREGATE);
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
    public void testNestedLoopJoin() throws Exception {
        xPlainTrace.turnOnTrace();

        String sql = "select t1.i from --SPLICE-PROPERTIES joinOrder=FIXED\n" +
                CLASS_NAME + "." + TABLE1 + " t1, " + CLASS_NAME + "." + TABLE2 + " t2 --SPLICE-PROPERTIES joinStrategy=NESTEDLOOP\n" +
                "where t1.i = t2.i";
        long count = baseConnection.count(sql);
        Assert.assertEquals(nrows,count);
        long statementId = getLastStatementId();
        xPlainTrace.turnOffTrace();

        XPlainTreeNode operation = xPlainTrace.getOperationTree(statementId);
        String operationType = operation.getOperationType();
//        System.out.println(operationType);
        Assert.assertEquals("Incorrect tree: expected ProjectRestrict on top",SpliceXPlainTrace.PROJECTRESTRICT,operationType);
        Assert.assertEquals("ProjectRestrict has incorrect input rows!",count,operation.getInputRows());
        Assert.assertEquals("ProjectRestrict has incorrect output rows!",count,operation.getOutputRows());

        Assert.assertEquals("ProjectRestrict has more than one child",1,operation.getChildren().size());
        operation = operation.getChildren().getFirst();
        operationType = operation.getOperationType();
        Assert.assertEquals("Incorrect tree: expected NestedLoop under ProjectRestrict",SpliceXPlainTrace.NESTEDLOOPJOIN,operationType);
        Assert.assertEquals("Incorrect left-side input rows",nrows,operation.getInputRows());
        Assert.assertEquals("Incorrect join right-side rows",count,operation.getRemoteScanRows());
        Assert.assertEquals("Incorrect join output rows",count,operation.getOutputRows());

        // First child should be a bulk table scan operation
        XPlainTreeNode child = operation.getChildren().getFirst();
        operationType = child.getOperationType();
        boolean isTableScan = SpliceXPlainTrace.BULKTABLESCAN.equalsIgnoreCase(operationType)
                || SpliceXPlainTrace.TABLESCAN.equalsIgnoreCase(operationType);
        Assert.assertTrue("Not a table scan! expected="+SpliceXPlainTrace.BULKTABLESCAN+" or "+ SpliceXPlainTrace.TABLESCAN, isTableScan);
        Assert.assertEquals("Incorrect output of left side table scan",nrows,child.getOutputRows());
        Assert.assertEquals("table scan output does not match scan+ filtered",child.getOutputRows(),child.getLocalScanRows()-child.getFilteredRows());

        child = operation.getChildren().getLast();
        operationType = child.getOperationType();
//        System.out.println(operationType);
        Assert.assertTrue("No table scan found!",operationType.contains(SpliceXPlainTrace.TABLESCAN));
        Assert.assertEquals("Table scan not outputing correct row count",count,child.getOutputRows());
        Assert.assertEquals("Table scan has incorrect number of iterations",count,child.getIterations());
        String info = child.getInfo();
//        System.out.println(info);
        Assert.assertTrue("Incorrect info: info="+info,info.compareToIgnoreCase("Scan filter:(T1.I[1:1] = T2.I[2:1]), table:XPLAINTRACE1IT.TAB2")==0);
    }


    @Override
    protected TestConnection getNewConnection() throws Exception {
        return methodWatcher.createConnection();
    }
}
