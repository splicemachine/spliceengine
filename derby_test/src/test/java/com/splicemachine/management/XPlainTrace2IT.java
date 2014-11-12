package com.splicemachine.management;

import com.splicemachine.derby.management.XPlainTreeNode;
import com.splicemachine.derby.test.framework.*;
import com.splicemachine.test.SerialTest;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;

import java.io.PrintWriter;
import java.sql.PreparedStatement;

/**
 * Created by jyuan on 7/14/14.
 */
@Category(SerialTest.class)
public class XPlainTrace2IT extends BaseXplainIT{

    public static int nrows = 10;
    public static int numLoops = 3;

    public XPlainTrace2IT() {

    }

    public static final String CLASS_NAME = XPlainTrace2IT.class.getSimpleName().toUpperCase();
    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
    public static final String TABLE1 = "T1";
    public static final String TABLE2 = "T2";
    public static final String TABLE3 = "T3";
    public static final String TABLE4 = "T4";
    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);

    private static String tableDef1 = "(I INT, J INT)";
    private static String tableDef2 = "(COL1 INT, COL2 VARCHAR(10))";
    protected static SpliceTableWatcher spliceTableWatcher1 = new SpliceTableWatcher(TABLE1,CLASS_NAME, tableDef1);
    protected static SpliceTableWatcher spliceTableWatcher2 = new SpliceTableWatcher(TABLE2,CLASS_NAME, tableDef1);
    protected static SpliceTableWatcher spliceTableWatcher3 = new SpliceTableWatcher(TABLE3,CLASS_NAME, tableDef1);
    protected static SpliceTableWatcher spliceTableWatcher4 = new SpliceTableWatcher(TABLE4,CLASS_NAME, tableDef2);
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
                                String.format("insert into %s (i, j) values (?, ?)", spliceTableWatcher1));
                        for (int i = 0; i < nrows; i++) {
                            ps.setInt(1, i);
                            ps.setInt(2, i);
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
                                String.format("insert into %s (i, j) values (?, ?)", spliceTableWatcher2));
                        for(int i=0;i<nrows;i++){
                            for (int j = 0; j < numLoops; ++j) {
                                ps.setInt(1, i);
                                ps.setInt(2, i);
                                ps.execute();
                            }
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            })
            .around(spliceTableWatcher3).around(new SpliceDataWatcher() {
                @Override
                protected void starting(Description description) {
                    PreparedStatement ps;
                    try {
                        ps = spliceClassWatcher.prepareStatement(
                                String.format("insert into %s (i, j) values (?, ?)", spliceTableWatcher3));
                        for (int i = 0; i < nrows; i++) {
                            ps.setInt(1, i);
                            ps.setInt(2, i);
                            ps.execute();
                        }
                        String createIndex = "Create index ti on " + CLASS_NAME + "." + TABLE3 + "(i)";
                        ps = spliceClassWatcher.prepareStatement(createIndex);
                        ps.execute();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            })
            .around(spliceTableWatcher4);


    @Test
    public void testIndexLookup() throws Exception {
        System.out.println(txnId);
        xPlainTrace.turnOnTrace();
        String sql = "select j from " + spliceTableWatcher3 + " --SPLICE-PROPERTIES index=TI \n where i = 1";
        long count =  baseConnection.count(sql);
        Assert.assertEquals("Incorrect row count with XPLAIN enabled", 1, count);
        xPlainTrace.turnOffTrace();
        long statementId = getLastStatementId();


        XPlainTreeNode operation = xPlainTrace.getOperationTree(statementId);
        operation = operation.getChildren().getFirst();
        String operationType = operation.getOperationType();
        Assert.assertTrue(operationType.compareToIgnoreCase(SpliceXPlainTrace.INDEXROWTOBASEROW) == 0);
        Assert.assertTrue(operation.getInfo().compareToIgnoreCase("baseTable:T3") == 0);
        Assert.assertEquals(operation.getInputRows(), 1);
        Assert.assertEquals(operation.getOutputRows(), 1);
        Assert.assertEquals(operation.getRemoteGetRows(), 1);

        operation = operation.getChildren().getFirst();
        Assert.assertTrue(operation.getOperationType().compareToIgnoreCase(SpliceXPlainTrace.BULKTABLESCAN)==0);
        Assert.assertTrue(operation.getInfo().contains("Scan filter:(I[1:1] = 1)"));
        Assert.assertTrue(operation.getInfo().contains("index:TI"));
        Assert.assertEquals(operation.getOutputRows(), 1);
        Assert.assertEquals(operation.getLocalScanRows(), 1);
    }

    @Test
    public void testOrderBy() throws Exception {
        xPlainTrace.turnOnTrace();
        String sql = "select j from " + spliceTableWatcher2 + " order by i desc";
        long count = baseConnection.count(sql);
        Assert.assertEquals("Incorrect row count with XPLAIN enabled",numLoops*nrows,count);
        xPlainTrace.turnOffTrace();

        long statementId = getLastStatementId();
        XPlainTreeNode operation = xPlainTrace.getOperationTree(statementId);
        if (operation != null) {
            operation = operation.getChildren().getFirst();
            Assert.assertEquals(operation.getOperationType().compareToIgnoreCase(SpliceXPlainTrace.SORT), 0);
            Assert.assertEquals(operation.getInputRows(), numLoops * nrows);
            Assert.assertEquals(operation.getWriteRows(), numLoops * nrows);
            Assert.assertEquals(operation.getOutputRows(), numLoops * nrows);
        }
    }

    @Test
    public void testDistinctScan() throws Exception {
        xPlainTrace.turnOnTrace();
        String sql = "select distinct i from " + spliceTableWatcher2;
        long count = baseConnection.count(sql);
        Assert.assertEquals("Incorrect result count with XPLAIN enabled", nrows,count);
        xPlainTrace.turnOffTrace();

        long statementId = getLastStatementId();
        XPlainTreeNode operation = xPlainTrace.getOperationTree(statementId);
        Assert.assertEquals(operation.getOperationType().compareToIgnoreCase(SpliceXPlainTrace.DISTINCTSCAN), 0);
        Assert.assertEquals(operation.getWriteRows(), nrows);
        Assert.assertEquals(operation.getInputRows(), nrows*numLoops);
    }

    @Test
    public void testGroupedAggregate() throws Exception {
        xPlainTrace.turnOnTrace();
        String sql = "select i, sum(j) from " + spliceTableWatcher2 + " group by i order by i";
        long count = baseConnection.count(sql);
        Assert.assertEquals("Incorrect result count with XPLAIN enabled",nrows,count);
        xPlainTrace.turnOffTrace();

        long statementId = getLastStatementId();
        XPlainTreeNode operation = xPlainTrace.getOperationTree(statementId);
        Assert.assertEquals(operation.getOperationType().compareToIgnoreCase(SpliceXPlainTrace.SORT), 0);

        operation = operation.getChildren().getFirst();
        operation = operation.getChildren().getFirst();
        Assert.assertEquals(operation.getInputRows(), nrows*numLoops);
        Assert.assertEquals(operation.getWriteRows(), nrows);
    }

    @Test
    public void testOnceAndSubquery() throws Exception {
        xPlainTrace.turnOnTrace();
        String sql = "select i from " + spliceTableWatcher1 + " a where a.i>  (select min(i) from "+ CLASS_NAME + "." + TABLE1 +" b where a.i < b.i)";
        long count = baseConnection.count(sql);
        Assert.assertEquals("Incorrect row count with XPLAIN enabled",1,count);
        xPlainTrace.turnOffTrace();

        long statementId = getLastStatementId();
        XPlainTreeNode operation = xPlainTrace.getOperationTree(statementId);
        Assert.assertEquals(operation.getOperationType().compareToIgnoreCase(SpliceXPlainTrace.PROJECTRESTRICT), 0);

        XPlainTreeNode child = operation.getChildren().getLast();
        if (child.getOperationType().compareToIgnoreCase(SpliceXPlainTrace.ONCE) == 0) {
            Assert.assertTrue(child.getInfo().contains("Subquery"));
            Assert.assertEquals(child.getIterations(), nrows);
            Assert.assertEquals(child.getInputRows(), nrows);
            Assert.assertEquals(child.getOutputRows(), nrows);
        }
        else {
            child = operation.getChildren().getFirst();
            Assert.assertEquals(child.getOperationType().compareToIgnoreCase(SpliceXPlainTrace.ONCE), 0);
            Assert.assertTrue(child.getInfo().contains("Subquery"));
            Assert.assertEquals(child.getIterations(), nrows);
            Assert.assertEquals(child.getInputRows(), nrows);
            Assert.assertEquals(child.getOutputRows(), nrows);
        }
    }

    @Test
    @Ignore("Ignored until DB-1755 is resolved")
    public void testDropColumn() throws Exception {
        xPlainTrace.turnOnTrace();
        String sql = "alter table " + spliceTableWatcher1 + " drop column j";
        baseConnection.execute(sql);
        xPlainTrace.turnOffTrace();

        long statementId = getLastStatementId();
        XPlainTreeNode operation = xPlainTrace.getOperationTree(statementId);
        //Assert.assertEquals(operation.getInfo().compareToIgnoreCase(SpliceXPlainTrace.POPULATEINDEX)

    }

    @Test
    public void testImport() throws Exception {

        PrintWriter writer1 = new PrintWriter("/tmp/Test1.txt","UTF-8");
        writer1.println("yuas,123");
        writer1.println("YifuMa,52");
        writer1.println("PeaceNLove,214");
        writer1.close();

        String sql = "call SYSCS_UTIL.IMPORT_DATA('"+CLASS_NAME+"','"+TABLE4+"','COL2,COL1','/tmp/Test1.txt',',',null,null,null,null,0,null)";
        xPlainTrace.turnOnTrace();
        baseConnection.execute(sql);
        xPlainTrace.turnOffTrace();
        long statementId = getLastStatementId();
        XPlainTreeNode operation = xPlainTrace.getOperationTree(statementId);
        Assert.assertEquals(operation.getOperationType().compareToIgnoreCase(SpliceXPlainTrace.IMPORT), 0);
        Assert.assertEquals(3, operation.getWriteRows());
    }

    @Override
    protected TestConnection getNewConnection() throws Exception {
        return methodWatcher.createConnection();
    }
}
