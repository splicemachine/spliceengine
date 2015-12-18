package com.splicemachine.derby.transactions;

import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;
import static org.junit.Assert.assertEquals;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.splicemachine.derby.test.framework.SpliceTestDataSource;
import com.splicemachine.homeless.TestUtils;
import com.splicemachine.test_dao.TableDAO;
import com.splicemachine.test_tools.TableCreator;

/**
 * Test that requires two RegionServers running to connect to one RS, do some DDL and make sure
 * a connection to the other RS sees the appropriate state of the system.
 */
public class ClusterDDLTestIT {
    private static final String SCHEMA = ClusterDDLTestIT.class.getSimpleName().toUpperCase();


    private SpliceTestDataSource dataSource;

    @Before
    public void startup() {
        dataSource = new SpliceTestDataSource();
    }

    @After
    public void shutdown() {
        dataSource.shutdown();
    }

    @Test
    public void createDropTableOnTwoRegionServers() throws Exception {
        Connection conn1 = dataSource.getConnection("localhost", 1527);
        Connection conn2 = dataSource.getConnection("localhost", 1528);

        String tableName = "twoints";
        String tableRef = SCHEMA+"."+tableName;
        String tableDef = "(a int, b int)";
        new TableDAO(conn1).drop(SCHEMA, tableName);

        //noinspection unchecked
        new TableCreator(conn1)
            .withCreate(String.format("create table %s %s", tableRef, tableDef))
            .withInsert(String.format("insert into %s (a, b) values (?,?)", tableRef))
            .withRows(rows(
                row(1, null), row(1,15), row(1, 11), row(1, null), row(2, 25), row(2, 10)
            ))
            .create();
        String sqlText = String.format("SELECT * FROM %s ORDER BY B", tableRef);
        String expectedResult =
            "A |  B  |\n" +
                "----------\n" +
                " 2 | 10  |\n" +
                " 1 | 11  |\n" +
                " 1 | 15  |\n" +
                " 2 | 25  |\n" +
                " 1 |NULL |\n" +
                " 1 |NULL |";

        // Assure connection to RS 1 can see the table contents
        ResultSet rs = conn1.createStatement().executeQuery(sqlText);
        String conn1Str = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
        assertEquals(sqlText, expectedResult, conn1Str);

        // Assure connection to RS 2 can see the table contents
        rs = conn2.createStatement().executeQuery(sqlText);
        String conn2Str = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);

        // Assure both connections see the same table contents
        assertEquals(sqlText, conn1Str, conn2Str);

        // Drop table on RS 1 and assure connection to RS 2 cannot see it any more
        new TableDAO(conn1).drop(SCHEMA, tableName);
        try {
            conn2.createStatement().executeQuery(sqlText);
        } catch (SQLException e) {
            assertEquals("Expected table not to exist.", "42X05", e.getSQLState());
        }
    }

//    @Test
    public void testDataSource() throws Exception {
        Connection conn1 = dataSource.getConnection("localhost", 1527);
        Connection conn2 = dataSource.getConnection("localhost", 1528);
        System.out.println("----------------------------------");
        for(String connStatus : dataSource.connectionStatus()) {
            System.out.println(connStatus);
        }

        System.out.println("----------------------------------");
        conn1.close();
        for(String connStatus : dataSource.connectionStatus()) {
            System.out.println(connStatus);
        }

        System.out.println("----------------------------------");
        Connection conn1a = dataSource.getConnection("localhost", 1527);
        for(String connStatus : dataSource.connectionStatus()) {
            System.out.println(connStatus);
        }
    }
}
