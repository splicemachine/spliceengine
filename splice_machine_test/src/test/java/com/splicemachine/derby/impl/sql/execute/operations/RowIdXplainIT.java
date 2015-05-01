package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.management.XPlainTreeNode;
import com.splicemachine.derby.test.framework.*;
import com.splicemachine.management.BaseXplainIT;
import com.splicemachine.test.SerialTest;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import java.sql.ResultSet;
import java.sql.PreparedStatement;

/**
 * Created by jyuan on 4/30/15.
 */
@Category(SerialTest.class)
public class RowIdXplainIT extends BaseXplainIT {

    public static final String CLASS_NAME = RowIdXplainIT.class.getSimpleName().toUpperCase();
    public static final String TABLE1_NAME = "A";
    public static final String TABLE2_NAME = "B";

    private static String tableDef1 = "(I INT, J INT, K INT, PRIMARY KEY(I,J))";
    private static String tableDef2 = "(I INT, J INT, K INT)";

    protected static SpliceTableWatcher spliceTableWatcher1 = new SpliceTableWatcher(TABLE1_NAME,CLASS_NAME, tableDef1);
    protected static SpliceTableWatcher spliceTableWatcher2 = new SpliceTableWatcher(TABLE2_NAME,CLASS_NAME, tableDef2);

    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);
    private static int nrows = 4;

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(CLASS_NAME);

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher)
            .around(spliceTableWatcher1).around(new SpliceDataWatcher() {
                @Override
                protected void starting(Description description) {
                    PreparedStatement ps;
                    try {
                        ps = spliceClassWatcher.prepareStatement(
                                String.format("insert into %s (i, j, k) values (?, ?, ?)", spliceTableWatcher1));
                        for (int i = 0; i < nrows; ++i) {
                            ps.setInt(1, i);
                            ps.setInt(2, i);
                            ps.setInt(3, i);
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
                                String.format("insert into %s (i, j, k) values (?, ?, ?)", spliceTableWatcher2));
                        for (int i = 0; i < nrows; ++i) {
                            ps.setInt(1, i);
                            ps.setInt(2, i);
                            ps.setInt(3, i);
                            ps.execute();
                        }

                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            });

    @Test
    public void testRowIdLookupOnTableWithPK() throws Exception {
        xPlainTrace.turnOnTrace();
        String sql = String.format("select i, j from %s where rowid='810081'", spliceTableWatcher1);
        long count = baseConnection.count(sql);
        Assert.assertEquals("Incorrect row count with XPLAIN enabled", 1, count);
        xPlainTrace.turnOffTrace();

        long statementId = getLastStatementId();

        XPlainTreeNode operation = xPlainTrace.getOperationTree(statementId);
        operation = operation.getChildren().getFirst();
        operation = operation.getChildren().getFirst();
        String operationType = operation.getOperationType();
        Assert.assertTrue(operationType.toUpperCase().contains("TABLESCAN"));
        Assert.assertEquals(1, operation.getLocalScanRows());
    }

    @Test
    public void testRowIdLookupOnTableWithoutPK() throws Exception {

        String sql = String.format("select rowid from %s where i=1", spliceTableWatcher2);
        PreparedStatement ps = spliceClassWatcher.prepareStatement(sql);
        ResultSet rs = ps.executeQuery();
        Assert.assertTrue(rs.next());
        String rowId = rs.getString(1);

        xPlainTrace.turnOnTrace();
        sql = String.format("select i from %s where rowid='%s'", spliceTableWatcher2, rowId);
        long count = baseConnection.count(sql);
        Assert.assertEquals("Incorrect row count with XPLAIN enabled", 1, count);
        xPlainTrace.turnOffTrace();

        long statementId = getLastStatementId();

        XPlainTreeNode operation = xPlainTrace.getOperationTree(statementId);
        operation = operation.getChildren().getFirst();
        operation = operation.getChildren().getFirst();
        String operationType = operation.getOperationType();
        Assert.assertTrue(operationType.toUpperCase().contains("TABLESCAN"));
        Assert.assertEquals(1, operation.getLocalScanRows());
    }

    @Test
    public void testRowIdRangeQueryTableWithPK() throws Exception {
        xPlainTrace.turnOnTrace();
        String sql = String.format("select i, j from %s where rowid>'800080' and rowid<'830083'", spliceTableWatcher1);
        long count = baseConnection.count(sql);
        Assert.assertEquals("Incorrect row count with XPLAIN enabled", 2, count);
        xPlainTrace.turnOffTrace();

        long statementId = getLastStatementId();

        XPlainTreeNode operation = xPlainTrace.getOperationTree(statementId);
        operation = operation.getChildren().getFirst();
        operation = operation.getChildren().getFirst();
        String operationType = operation.getOperationType();
        Assert.assertTrue(operationType.toUpperCase().contains("TABLESCAN"));
        Assert.assertEquals(2, operation.getLocalScanRows());
    }

    @Test
    public void testGreaterThanQuery() throws Exception {
        xPlainTrace.turnOnTrace();
        String sql = String.format("select i, j from %s where rowid>'810081' and rowid>'800080'", spliceTableWatcher1);
        long count = baseConnection.count(sql);
        Assert.assertEquals("Incorrect row count with XPLAIN enabled", 2, count);
        xPlainTrace.turnOffTrace();

        long statementId = getLastStatementId();

        XPlainTreeNode operation = xPlainTrace.getOperationTree(statementId);
        operation = operation.getChildren().getFirst();
        operation = operation.getChildren().getFirst();
        String operationType = operation.getOperationType();
        Assert.assertTrue(operationType.toUpperCase().contains("TABLESCAN"));
        Assert.assertTrue(operation.getLocalScanRows()<nrows);
    }

    @Test
    public void testRowIdRangeQueryTableWithoutPK() throws Exception {

        String sql = String.format("select rowid from %s where i=0", spliceTableWatcher2);
        PreparedStatement ps = spliceClassWatcher.prepareStatement(sql);
        ResultSet rs = ps.executeQuery();
        Assert.assertTrue(rs.next());
        String rowId1 = rs.getString(1);

        sql = String.format("select rowid from %s where i=3", spliceTableWatcher2);
        ps = spliceClassWatcher.prepareStatement(sql);
        rs = ps.executeQuery();
        Assert.assertTrue(rs.next());
        String rowId2 = rs.getString(1);

        xPlainTrace.turnOnTrace();
        sql = String.format("select i, j from %s where rowid>'%s' and rowid<'%s'", spliceTableWatcher2, rowId1, rowId2);
        long count = baseConnection.count(sql);
        Assert.assertEquals("Incorrect row count with XPLAIN enabled", 2, count);
        xPlainTrace.turnOffTrace();

        long statementId = getLastStatementId();

        XPlainTreeNode operation = xPlainTrace.getOperationTree(statementId);
        operation = operation.getChildren().getFirst();
        operation = operation.getChildren().getFirst();
        String operationType = operation.getOperationType();
        Assert.assertTrue(operationType.toUpperCase().contains("TABLESCAN"));
        Assert.assertEquals(2, operation.getLocalScanRows());
    }


    @Override
    protected TestConnection getNewConnection() throws Exception {
        return methodWatcher.createConnection();
    }
}
