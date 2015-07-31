package com.splicemachine.derby.impl.stats;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.derby.test.framework.TestConnection;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Specific ITs for statistics tests.
 *
 * @author Scott Fines
 *         Date: 6/24/15
 */
public class FixedStatsIT{
    private static final SpliceWatcher classWatcher = new SpliceWatcher();
    private static final SpliceSchemaWatcher schema = new SpliceSchemaWatcher(FixedStatsIT.class.getSimpleName().toUpperCase());

    private static final SpliceTableWatcher charDelete = new SpliceTableWatcher("CHAR_DELETE",schema.schemaName,"(c char(10))");

    @ClassRule
    public static final TestRule rule = RuleChain.outerRule(classWatcher)
            .around(schema)
            .around(charDelete);

    private static TestConnection conn;

    @BeforeClass
    public static void setUpClass() throws Exception {
        conn = classWatcher.getOrCreateConnection();
        conn.setAutoCommit(false);
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
        conn.reset();
    }

    @After
    public void afterMethod() throws Exception{
        conn.rollback();
    }

    @Test
    public void testCorrectRowCountsAfterDelete() throws Exception{
        /*
         * Regression test for DB-3468
         */
        try(PreparedStatement ps=conn.prepareStatement("insert into "+charDelete+" (c) values (?)")){
            ps.setString(1,"1");
            ps.execute();
            ps.setString(1,"2");
            ps.execute();
        }

        conn.collectStats(schema.schemaName,charDelete.tableName);
        try(Statement s = conn.createStatement()){
            assertExpectedCount(s,2);

            int changed = s.executeUpdate("delete from "+charDelete);
            Assert.assertEquals("did not properly delete values!",2,changed);

            conn.collectStats(schema.schemaName,charDelete.tableName);
            assertExpectedCount(s,0);
        }
    }

    @Test
    public void testUpdateDoesNotThrowError() throws Exception{
        /*
         * Regression test for DB-3469
         */
        try(PreparedStatement ps=conn.prepareStatement("insert into "+charDelete+" (c) values (?)")){
            ps.setString(1,"1");
            ps.execute();
        }

        conn.collectStats(schema.schemaName,charDelete.tableName);
        try(Statement s = conn.createStatement()){
            assertExpectedCount(s,1);

            //the bug is that this throws an error, so we just want to make sure that it doesn't blow up here
            int changed = s.executeUpdate("update "+charDelete+" set c='2' where c = '1'");
            Assert.assertEquals("did not properly delete values!",1,changed);
        }
    }

    @Test
    public void testQualifiedScanHasLowerCost() throws Exception{
        try(PreparedStatement ps=conn.prepareStatement("insert into "+charDelete+" (c) values (?)")){
            ps.setString(1,"1");
            ps.execute();
            ps.setString(1,"2");
            ps.execute();
            ps.setString(1,"3");
            ps.execute();
            ps.setString(1,"4");
            ps.execute();
            ps.setString(1,"5");
            ps.execute();
        }

        conn.collectStats(schema.schemaName,charDelete.tableName);
        ExplainRow unqualifiedRow;
        ExplainRow qualifiedRow;
        String baseSql = "explain select * from "+charDelete;
        try(Statement s = conn.createStatement()){
            try(ResultSet resultSet=s.executeQuery(baseSql)){
                Assert.assertTrue("no row returned!",resultSet.next());
                unqualifiedRow=ExplainRow.parse(resultSet.getString(1));
            }
            try(ResultSet resultSet=s.executeQuery(baseSql+" where c = '5         '")){
                Assert.assertTrue("no row returned!",resultSet.next());
                qualifiedRow=ExplainRow.parse(resultSet.getString(1));
            }
        }
        ExplainRow.Cost qualifiedCost=qualifiedRow.cost();
        ExplainRow.Cost unqualifiedCost=unqualifiedRow.cost();
        Assert.assertTrue("Total costs is not lower!",qualifiedCost.overallCost()<unqualifiedCost.overallCost());
        Assert.assertTrue("row count is not lower!!",qualifiedCost.rowCount()<unqualifiedCost.rowCount());
        Assert.assertTrue("Transfer cost is not lower!!",qualifiedCost.remoteCost()<unqualifiedCost.remoteCost());
        Assert.assertEquals("Local cost does not match!!",unqualifiedCost.localCost(),qualifiedCost.localCost(),1e-10);
    }

    private void assertExpectedCount(Statement s,int expectedCount) throws SQLException{
        try(ResultSet resultSet=s.executeQuery("select * from sys.systablestatistics "+
                "where schemaname = '"+schema.schemaName+"' and tablename = '"+charDelete.tableName+"'")){
            Assert.assertTrue("No row returned after stats collection!",resultSet.next());
            long rowCount=resultSet.getLong("TOTAL_ROW_COUNT");
            /*
             * WARNING(-sf-): If you add more data to the charDelete table, you might contaminate this number, so
             * be careful!
             */
            Assert.assertEquals("Incorrect row count!",expectedCount,rowCount);
        }
    }
}
