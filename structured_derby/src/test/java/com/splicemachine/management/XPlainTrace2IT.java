package com.splicemachine.management;

import com.splicemachine.derby.management.XPlainTreeNode;
import com.splicemachine.derby.test.framework.*;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * Created by jyuan on 7/14/14.
 */
public class XPlainTrace2IT {

    public static int nrows = 10;
    public static int numLoops = 3;

    private SpliceXPlainTrace xPlainTrace = new SpliceXPlainTrace();

    public XPlainTrace2IT() {

    }

    public static final String CLASS_NAME = XPlainTrace2IT.class.getSimpleName().toUpperCase();
    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
    public static final String TABLE1 = "T1";
    public static final String TABLE2 = "T2";
    public static final String TABLE3 = "T3";
    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);

    private static String tableDef = "(I INT, J INT)";
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
            });

    @Test
    public void testIndexLookup() throws Exception {

        xPlainTrace.turnOnTrace();
        String sql = "select j from " + CLASS_NAME + "." + TABLE3 + " --splice-properties index=ti \n where i = 1";
        ResultSet rs = xPlainTrace.executeQuery(sql);
        int count = 0;
        while(rs.next()) {
            ++count;
        }
        Assert.assertEquals(count, 1);

        xPlainTrace.turnOffTrace();

        XPlainTreeNode operation = xPlainTrace.getOperationTree();
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
        String sql = "select j from " + CLASS_NAME + "." + TABLE2 + " order by i desc";
        ResultSet rs = xPlainTrace.executeQuery(sql);
        int count = 0;
        while(rs.next()) {
            ++count;
        }
        Assert.assertEquals(count, numLoops*nrows);
        xPlainTrace.turnOffTrace();

        XPlainTreeNode operation = xPlainTrace.getOperationTree();
        if (operation != null) {
            operation = operation.getChildren().getFirst();
            Assert.assertEquals(operation.getOperationType().compareToIgnoreCase(SpliceXPlainTrace.SORT), 0);
            Assert.assertEquals(operation.getInputRows(), numLoops * nrows);
            Assert.assertEquals(operation.getWriteRows(), numLoops * nrows);
        }
    }

    @Test
    public void testDistinctScan() throws Exception {
        xPlainTrace.turnOnTrace();
        String sql = "select distinct i from " + CLASS_NAME + "." + TABLE2;
        ResultSet rs = xPlainTrace.executeQuery(sql);
        int count = 0;
        while(rs.next()) {
            ++count;
        }
        Assert.assertEquals(count, nrows);
        xPlainTrace.turnOffTrace();

        XPlainTreeNode operation = xPlainTrace.getOperationTree();
        Assert.assertEquals(operation.getOperationType().compareToIgnoreCase(SpliceXPlainTrace.DISTINCTSCAN), 0);
        Assert.assertEquals(operation.getWriteRows(), nrows);
        Assert.assertEquals(operation.getInputRows(), nrows*numLoops);
    }

    @Test
    public void testGroupedAggregate() throws Exception {
        xPlainTrace.turnOnTrace();
        String sql = "select i, sum(j) from " + CLASS_NAME + "." + TABLE2 + " group by i order by i";
        ResultSet rs = xPlainTrace.executeQuery(sql);
        int count = 0;
        while(rs.next()) {
            ++count;
        }
        Assert.assertEquals(count, nrows);
        xPlainTrace.turnOffTrace();

        XPlainTreeNode operation = xPlainTrace.getOperationTree();
        Assert.assertEquals(operation.getOperationType().compareToIgnoreCase(SpliceXPlainTrace.SORT), 0);

        operation = operation.getChildren().getFirst();
        operation = operation.getChildren().getFirst();
        Assert.assertEquals(operation.getInputRows(), nrows*numLoops);
        Assert.assertEquals(operation.getWriteRows(), nrows);
    }

    @Test
    public void testOnceAndSubquery() throws Exception {
        xPlainTrace.turnOnTrace();
        String sql = "select i from " + CLASS_NAME + "." + TABLE1 + " a where a.i>  (select min(i) from "+ CLASS_NAME + "." + TABLE1 +" b where a.i < b.i)";
        ResultSet rs = xPlainTrace.executeQuery(sql);
        int count = 0;
        while(rs.next()) {
            ++count;
        }
        Assert.assertEquals(count, 1);
        xPlainTrace.turnOffTrace();

        XPlainTreeNode operation = xPlainTrace.getOperationTree();
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
    public void testDropColumn() throws Exception {
        xPlainTrace.turnOnTrace();
        String sql = "alter table " + CLASS_NAME + "." + TABLE1 + " drop column j";
        xPlainTrace.execute(sql);
        xPlainTrace.turnOffTrace();

        XPlainTreeNode operation = xPlainTrace.getOperationTree();
        //Assert.assertEquals(operation.getInfo().compareToIgnoreCase(SpliceXPlainTrace.POPULATEINDEX)

    }
}
