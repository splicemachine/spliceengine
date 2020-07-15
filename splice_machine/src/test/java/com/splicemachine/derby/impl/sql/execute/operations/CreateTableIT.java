package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.fail;

public class CreateTableIT {
    private static final String SCHEMA = CreateTableIT.class.getSimpleName().toUpperCase();

    @ClassRule
    public final static SpliceWatcher watcher = new SpliceWatcher(SCHEMA);

    @ClassRule
    public static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(SCHEMA);

    static int counter = 0;
    private static String generateTableName() {
        return "T" + counter++;
    }

    private void shouldContain(String tableName, List<List<Integer>> rows) throws SQLException {
        ResultSet rs =  watcher.executeQuery("SELECT * FROM " + tableName + " ORDER BY c1 ASC");
        for(List<Integer> row : rows) {
            Assert.assertTrue(rs.next());
            for(int i=0; i<row.size(); ++i) {
                Assert.assertEquals((int)row.get(i), rs.getInt(i+1));
            }
        }
        Assert.assertFalse(rs.next());
    }

    @SuppressFBWarnings(value = {"OBL_UNSATISFIED_OBLIGATION_EXCEPTION_EDGE", "OBL_UNSATISFIED_OBLIGATION"}, justification = "Test not related to business logic")
    private String idOf(final String tableName) throws SQLException {
        PreparedStatement ps = watcher.prepareStatement("SELECT tableid FROM sys.systables WHERE tablename = ?");
        ps.setString(1, tableName);
        ResultSet rs = ps.executeQuery();
        rs.next();
        return rs.getString(1);
    }

    @SuppressFBWarnings(value = {"OBL_UNSATISFIED_OBLIGATION_EXCEPTION_EDGE", "OBL_UNSATISFIED_OBLIGATION"}, justification = "Test not related to business logic")
    private List<String> constraintsOf(final String tableName) throws SQLException {
        String id = idOf(tableName);
        PreparedStatement ps = watcher.prepareStatement("SELECT constraintname FROM sys.sysconstraints WHERE tableid = ?");
        ps.setString(1, id);
        List<String> result = new ArrayList<>();
        ResultSet rs = ps.executeQuery();
        while(rs.next()) {
            result.add(rs.getString(1));
        }
        return result;
    }

    @Test
    public void createTableWithRedundantUniqueConstraintIsIgnored() throws Exception {
        String tbl = generateTableName();
        String pkCon = tbl + "PK", uqCon = tbl + "UQ";
        watcher.executeUpdate("CREATE TABLE " + tbl + "(c1 INT, c2 INT, CONSTRAINT " + pkCon + " PRIMARY KEY(c1, c2), CONSTRAINT " + uqCon + " UNIQUE(c1, c2))");
        watcher.executeUpdate("INSERT INTO " + tbl + " VALUES(1, 2), (3, 4)");

        // check that data exists
        shouldContain(tbl, Arrays.asList(Arrays.asList(1,2), Arrays.asList(3,4)));

        // check that the unique constraint is NOT created
        Assert.assertFalse(constraintsOf(tbl).contains(uqCon));

        // todo: try to also further verify this by dropping the PK and add duplicate rows once DB-9809 is fixed
    }

    @Test
    public void createTableWithRedundantUniqueConstraintIsIgnoredDifferentColumnOrder() throws Exception {
        String tbl = generateTableName();
        String pkCon = tbl + "PK", uqCon = tbl + "UQ";
        watcher.executeUpdate("CREATE TABLE " + tbl + "(c1 INT, c2 INT, CONSTRAINT " + pkCon + " PRIMARY KEY(c1, c2), CONSTRAINT " + uqCon + " UNIQUE(c2, c1))");
        watcher.executeUpdate("INSERT INTO " + tbl + " VALUES(1, 2), (3, 4)");

        // check that data exists
        shouldContain(tbl, Arrays.asList(Arrays.asList(1,2), Arrays.asList(3,4)));

        // check that the unique constraint is NOT created
        Assert.assertFalse(constraintsOf(tbl).contains(uqCon));

        // todo: try to also further verify this by dropping the PK and add duplicate rows once DB-9809 is fixed
    }

    @Test
    public void alterTableWithRedundantUniqueConstraintFails() throws Exception {
        String tbl = generateTableName();
        String pkCon = tbl + "PK", uqCon = tbl + "UQ";
        watcher.executeUpdate("CREATE TABLE " + tbl + "(c1 INT, c2 INT, CONSTRAINT " + pkCon + " PRIMARY KEY(c1, c2))");
        watcher.executeUpdate("INSERT INTO " + tbl + " VALUES(1, 2), (3, 4)");

        try {
            watcher.executeUpdate("ALTER TABLE " + tbl + " ADD CONSTRAINT " + uqCon + " UNIQUE(c1, c2)");
            fail("Expected: ERROR 42Z93: Constraints 'CCC1' and 'CCC' have the same set of columns, which is not allowed");
        } catch (SQLException e) {
            Assert.assertEquals("42Z93", e.getSQLState());
            return;
        }
        fail("Expected: ERROR 42Z93: Constraints 'CCC1' and 'CCC' have the same set of columns, which is not allowed");
    }
}