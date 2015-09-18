package com.splicemachine.derby.impl.sql.execute.actions;

import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.splicemachine.derby.test.framework.*;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;

import com.splicemachine.test.SerialTest;
import com.splicemachine.test_dao.TableDAO;
import com.splicemachine.test_tools.TableCreator;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

/**
 * Integration tests specific to CHECK constraints.
 * 
 * @see {@link ConstraintConstantOperationIT} for other constraint tests
 * 
 * @author Walt Koetke
 */
@Category(SerialTest.class)
public class CheckConstraintIT extends SpliceUnitTest {
    public static final String CLASS_NAME = CheckConstraintIT.class.getSimpleName().toUpperCase();
    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher(CLASS_NAME);
    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
        .around(spliceSchemaWatcher);
    
    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(CLASS_NAME);

    @SuppressWarnings("unchecked")
	public static void createData(Connection conn, String schemaName) throws Exception {

        // Table2 has check constraints upon creation
        new TableCreator(conn)
            .withCreate(
            	"create table table1 ( " +
                "id int not null primary key constraint id_ge_ck check (id >= 1000), " +
                "i_gt int constraint i_gt_ck check (i_gt > 100), " +
                "i_eq int constraint i_eq_ck check (i_eq = 90), " +
                "i_lt int constraint i_lt_ck check (i_lt < 80), " +
                "v_neq varchar(32) constraint v_neq_ck check (v_neq <> 'bad'), " +
                "v_in varchar(32) constraint v_in_ck check (v_in in ('good1', 'good2', 'good3')))")
            .withInsert("insert into table1 values (?, ?, ?, ?, ?, ?)")
            .withRows(rows(
                row(1000, 200, 90, 79, "ok", "good1")))
            .create();

        // Table2 has no check constraints initially
        new TableCreator(conn)
        .withCreate(
        	"create table table2 ( " +
            "id int not null primary key, " +
            "i_gt int, " +
            "i_eq int, " +
            "i_lt int, " +
            "v_neq varchar(32), " +
            "v_in varchar(32))")
        .withInsert("insert into table2 values (?, ?, ?, ?, ?, ?)")
        .withRows(rows(
            row(1000, 200, 90, 79, "ok", "good1")))
        .create();

        conn.commit();
    }

    @BeforeClass
    public static void createDataSet() throws Exception {
        createData(spliceClassWatcher.getOrCreateConnection(), spliceSchemaWatcher.toString());
    }

    private void verifyNoViolation(String query, int count) throws Exception {
    	Assert.assertEquals("Invalid row count", count, methodWatcher.executeUpdate(query));
    }
    
    protected void verifyViolation(String query, String msgStart, Object... args) throws Exception {
    	try {
    		methodWatcher.executeUpdate(query);
            fail("Expected check constraint violation.");
    	} catch (SQLException e) {
    		Assert.assertTrue("Unexpected exception type", e instanceof SQLIntegrityConstraintViolationException);
            Assert.assertTrue(e.getLocalizedMessage(), e.getLocalizedMessage().startsWith(String.format(msgStart, args)));
    	}
    }

    protected static String MSG_START_DEFAULT = 
		"The check constraint '%s' was violated while performing an INSERT or UPDATE on table";

    @Test
    public void testSingleInserts() throws Exception {
    	verifyNoViolation("insert into table1 values (1001, 101, 90, 79, 'ok', 'good2')", 1); // all good

    	verifyViolation("insert into table1 values (999, 101, 90, 79, 'ok', 'good1')", // col 1 (PK) bad
    		MSG_START_DEFAULT, "ID_GE_CK");

    	verifyViolation("insert into table1 values (1002, 25, 90, 79, 'ok', 'good1')", // col 2 bad
    		MSG_START_DEFAULT, "I_GT_CK");

    	verifyViolation("insert into table1 values (1003, 101, 901, 79, 'ok', 'good1')", // col 3 bad
    		MSG_START_DEFAULT, "I_EQ_CK");

    	verifyViolation("insert into table1 values (1004, 101, 90, 85, 'ok', 'good2')", // col 4 bad
    		MSG_START_DEFAULT, "I_LT_CK");

    	verifyViolation("insert into table1 values (1005, 101, 90, 85, 'bad', 'good3')", // col 5 bad
    		MSG_START_DEFAULT, "V_NEQ_CK");

    	verifyViolation("insert into table1 values (1005, 101, 90, 85, 'ok', 'notsogood')", // col 6 bad
    		MSG_START_DEFAULT, "V_IN_CK");

    }

    @Test
    public void testAlterTableExistingDataViolatesConstraint() throws Exception {
        try {
            methodWatcher.executeUpdate("alter table table2 add constraint id_ge_ck check (id > 1000)");
            Assert.fail("Expected add or enable constraint exception");
        } catch (Exception e) {
            Assert.assertTrue("Wrong Exception Message",e.getMessage().contains("Attempt to add or enable constraint(s) on table"));
        }
    }



    @Test
    public void testSingleInsertsAfterAlterTable() throws Exception {
        methodWatcher.executeUpdate("alter table table2 add constraint id_ge_ck check (id >= 1000)");
        methodWatcher.executeUpdate("alter table table2 add constraint i_gt_ck check (i_gt > 100)");
        methodWatcher.executeUpdate("alter table table2 add constraint i_eq_ck check (i_eq = 90)");
        methodWatcher.executeUpdate("alter table table2 add constraint i_lt_ck check (i_lt < 80)");
        methodWatcher.executeUpdate("alter table table2 add constraint v_neq_ck check (v_neq <> 'bad')");
        methodWatcher.executeUpdate("alter table table2 add constraint v_in_ck check (v_in in ('good1', 'good2', 'good3'))");

        verifyNoViolation("insert into table2 values (1001, 101, 90, 79, 'ok', 'good2')", 1); // all good

    	verifyViolation("insert into table2 values (999, 101, 90, 79, 'ok', 'good1')", // col 1 (PK) bad
    		MSG_START_DEFAULT, "ID_GE_CK");

    	verifyViolation("insert into table2 values (1002, 25, 90, 79, 'ok', 'good1')", // col 2 bad
    		MSG_START_DEFAULT, "I_GT_CK");

    	verifyViolation("insert into table2 values (1003, 101, 901, 79, 'ok', 'good1')", // col 3 bad
    		MSG_START_DEFAULT, "I_EQ_CK");

    	verifyViolation("insert into table2 values (1004, 101, 90, 85, 'ok', 'good2')", // col 4 bad
    		MSG_START_DEFAULT, "I_LT_CK");

    	verifyViolation("insert into table2 values (1005, 101, 90, 85, 'bad', 'good3')", // col 5 bad
    		MSG_START_DEFAULT, "V_NEQ_CK");

    	verifyViolation("insert into table2 values (1005, 101, 90, 85, 'ok', 'notsogood')", // col 6 bad
    		MSG_START_DEFAULT, "V_IN_CK");

    }

    @Test
    public void testViolationErrorMsg() throws Exception {
        // DB-3864 - bad erorr msg
        String tableName = "table3".toUpperCase();
        String tableRef = spliceSchemaWatcher.schemaName+"."+tableName;
        TableDAO tableDAO = new TableDAO(methodWatcher.getOrCreateConnection());
        tableDAO.drop(spliceSchemaWatcher.schemaName, tableName);

        methodWatcher.executeUpdate(format("create table %s (int1_col int not null constraint constr_int1 check" +
                                               "(int1_col<5))", tableName));
        try {
            methodWatcher.executeUpdate(format("insert into %s values(10)", tableName));
            fail("Expected constraint violation");
        } catch (Exception e) {
            assertTrue("Expected single ticks around table ref.", e.getLocalizedMessage().contains(format("'%s'", tableRef)));
        }
    }

    // Additional tests to consider:
    //
    // more data types and operators
    // alter table and create table usage
    // insert multiple rows at a time
    // updates
    // disable/enable
    
}
