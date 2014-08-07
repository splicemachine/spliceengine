package org.apache.derby.parser.test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.derby.testutils.ASTVisitorConfig;
import org.apache.derby.testutils.XmlASTVisitor;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Test that prints the parse tree (AST) for a given query.<br/>
 * Useful to see the parse tree when making changes to derby's sql grammar.
 * <p/>
 * For each query, a plan tree is printed for each of the three phases;
 * parse, bind, optimize.
 * <p/>
 * <b>TO RUN</b> This test depends on the splice derby jars you have built
 * and installed into your local repo. The splice server must be built against
 * these jars and it must be running.  (See the pom.xml for dependency details)<br/>
 * <b>NOTE</b> currently you must have schema and tables populated for the given query you're running.
 *
 * @author Jeff Cunningham
 *         Date: 6/9/14
 */
public class AST_PrinterTest {
    private static XmlASTVisitor grapher;
    private static Connection connection;

    @BeforeClass
    public static void beforeClass() throws Exception {
        grapher = new XmlASTVisitor();
        connection = ASTVisitorConfig.Factory.createConnection();
        grapher.setConnection(connection);
        grapher.setOutputStream(System.out);
        grapher.setContinueAfterParse();
        grapher.initializeVisitor();
//        createLoadTable(connection);
    }

    @AfterClass
    public static void afterClass() throws Exception {
//        ASTVisitorConfig.Factory.dropTable(connection, "splice", "emptab");
        grapher.teardownVisitor();
        ASTVisitorConfig.Factory.closeConnection(connection);
    }

    @Test
    public void testAggregateGroupby() throws Exception {
        String query = "SELECT dept, max(salary) as maxsal from emptab group by dept";
        grapher.execute(query);
    }

    @Test
    public void testAggregateOrderby() throws Exception {
        String query = "SELECT max(salary) as maxi, dept from emptab group by dept order by dept";
        grapher.execute(query);
    }

    @Test
    public void testAllColsAggregateGroupbyOrderby() throws Exception {
        String query = "SELECT empnum, dept, salary, max(salary) as maxsal from emptab group by empnum, dept, salary order by salary";
        grapher.execute(query);
    }

    @Test
    public void testAggregateWindow() throws Exception {
        String query = "SELECT dept, max(salary) over (Partition by dept) as maxsal from emptab";
        ASTVisitorConfig.Factory.printResult(query, grapher.execute(query), System.out);
    }

    @Test
    public void testAggregateWindowOrderbySmall() throws Exception {
        String query = "SELECT dept, salary, max(salary) over (Partition by dept,salary ORDER BY salary) as maxsal from emptab";
        ASTVisitorConfig.Factory.printResult(query, grapher.execute(query), System.out);
    }

    @Test
    public void testRankWindowPartitionOrderbySmall() throws Exception {
        String query = "SELECT empnum, dept, salary, RANK() OVER (Partition by dept ORDER BY salary) AS DenseRank FROM emptab";
        ASTVisitorConfig.Factory.printResult(query, grapher.execute(query), System.out);
    }

    @Test
    public void testRankWindowOrderbySmall() throws Exception {
        String query = "SELECT empnum, dept, salary, RANK() OVER (ORDER BY salary) AS DenseRank FROM emptab";
        ASTVisitorConfig.Factory.printResult(query, grapher.execute(query), System.out);
    }

    @Test
    public void testDenseRankWindowPartitionOrderbySmall() throws Exception {
        String query = "SELECT empnum, dept, salary, DENSE_RANK() OVER (Partition by dept ORDER BY salary) AS DenseRank FROM emptab";
        ASTVisitorConfig.Factory.printResult(query, grapher.execute(query), System.out);
    }

    @Test
    public void testDenseRankWindowOrderbySmall() throws Exception {
        String query = "SELECT empnum, dept, salary, DENSE_RANK() OVER (ORDER BY salary) AS DenseRank FROM emptab";
        ASTVisitorConfig.Factory.printResult(query, grapher.execute(query), System.out);
    }

    @Test
    public void testRowNumWindowPartitionOrderbySmall() throws Exception {
        String query = "SELECT empnum, dept, salary, ROW_NUMBER() OVER (Partition by dept ORDER BY salary) AS DenseRank FROM emptab";
        ASTVisitorConfig.Factory.printResult(query, grapher.execute(query), System.out);
    }

    @Test
    public void testRowNumWindowOrderbySmall() throws Exception {
        String query = "SELECT empnum, dept, salary, ROW_NUMBER() OVER (ORDER BY salary) AS DenseRank FROM emptab";
        ASTVisitorConfig.Factory.printResult(query, grapher.execute(query), System.out);
    }

    @Test
    public void testAggregateWindowOrderby() throws Exception {
        String query = "SELECT dept, max(salary) over (Partition by dept ORDER BY salary) as maxsal from emptab";
        grapher.execute(query);
    }

    @Test
    public void testAllColsAggregateWindowOrderby() throws Exception {
        String query = "SELECT empnum, dept, salary, max(salary) over (Partition by dept ORDER BY salary) as maxsal from emptab";
        grapher.execute(query);
    }

    @Test
    public void testFrame() throws Exception {
        String query = "SELECT max(salary) over (Partition by dept RANGE BETWEEN 2 PRECEDING AND 3 FOLLOWING) from emptab";

        try {
            grapher.execute(query);
        } catch (SQLException e) {
            // expected - can't specify rows in range mode
        }
    }

    @Test
    public void testAggregateWindowPartitionOrderBy() throws Exception {
        String query = "SELECT * FROM (SELECT max(salary) OVER (PARTITION BY dept ORDER BY empnum) AS maxi, emptab.* FROM emptab) AS foo WHERE maxi > 2";
        grapher.execute(query);
    }

    @Ignore("Derby functions can't have embedded SELECT?")
    //@Test
    public void testAggregateExpressionWindowPartitionOrderBy() throws Exception {
        // BROKEN: Derby functions can't have embedded SELECT?
        String query = "SELECT max (SELECT min(salary*3) OVER (PARTITION BY dept ORDER BY empnum) AS mini, emptab.* FROM emptab) AS foo WHERE mini < 3";
        grapher.execute(query);
    }

    @Test
    public void testEmbeddedRankWindowPartitionOrderBy() throws Exception {
        String query = "SELECT * FROM (SELECT RANK() OVER (PARTITION BY dept ORDER BY empnum) AS ranks, emptab.* FROM emptab ) AS foo";
        grapher.execute(query);
    }

    @Test
    public void testRankWindowPartitionOrderBy() throws Exception {
        String query = "SELECT RANK() OVER (PARTITION BY dept ORDER BY empnum) AS ranks, emptab.* FROM emptab";
        grapher.execute(query);
    }

    @Test
    public void testRowNumWindowBetweenFrame() throws Exception {
        String query = "SELECT * FROM (SELECT ROW_NUMBER() OVER (PARTITION BY dept ORDER BY empnum NULLS LAST RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS rownumber, emptab.* FROM emptab ) AS foo WHERE rownumber > 2";
        grapher.execute(query);
    }

    @Test
    public void testAllRankingFunctions() throws Exception {
        String query = "SELECT empnum, dept, salary, ROW_NUMBER() OVER (ORDER BY dept) AS RowNumber, RANK() OVER (ORDER BY salary) AS Rank, DENSE_RANK() OVER (ORDER BY salary) AS DenseRank FROM emptab";
        grapher.execute(query);
    }

    // ============================================

    private static void createLoadTable(Connection connection) throws SQLException {
        // TODO: not working
        Statement statement = connection.createStatement();
        statement.execute(String.format("set schema %s","splice"));
        connection.commit();

        statement = connection.createStatement();
        statement.execute(String.format("create table %s.%s %s","splice","emptab","(empnum int, dept int, salary int)"));
        connection.commit();

        String collLoadQuery = String.format("insert into %s.%s values (?,?,?)", "splice","emptab");
        PreparedStatement ps = connection.prepareStatement(collLoadQuery);
        ps.setInt(1, 90 );  ps.setInt(2, 2); ps.setInt(3, 51000); ps.addBatch();
        ps.setInt(1, 40 );  ps.setInt(2, 2); ps.setInt(3, 52000); ps.addBatch();
        ps.setInt(1, 80 );  ps.setInt(2, 3); ps.setInt(3, 79000); ps.addBatch();
        ps.setInt(1, 20 );  ps.setInt(2, 1); ps.setInt(3, 75000); ps.addBatch();
        ps.setInt(1, 100);  ps.setInt(2, 3); ps.setInt(3, 55000); ps.addBatch();
        ps.setInt(1, 70 );  ps.setInt(2, 1); ps.setInt(3, 76000); ps.addBatch();
        ps.setInt(1, 120);  ps.setInt(2, 3); ps.setInt(3, 75000); ps.addBatch();
        ps.setInt(1, 60 );  ps.setInt(2, 1); ps.setInt(3, 78000); ps.addBatch();
        ps.setInt(1, 30 );  ps.setInt(2, 3); ps.setInt(3, 84000); ps.addBatch();
        ps.setInt(1, 110);  ps.setInt(2, 1); ps.setInt(3, 53000); ps.addBatch();
        ps.setInt(1, 50 );  ps.setInt(2, 1); ps.setInt(3, 52000); ps.addBatch();
        ps.setInt(1, 10 );  ps.setInt(2, 1); ps.setInt(3, 50000); ps.addBatch();
        ps.executeBatch();
        connection.commit();
    }

}
