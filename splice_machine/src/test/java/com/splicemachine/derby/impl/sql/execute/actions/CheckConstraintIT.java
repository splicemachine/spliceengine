package com.splicemachine.derby.impl.sql.execute.actions;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;

import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.test.SerialTest;
import com.splicemachine.test_dao.TableDAO;

/**
 * Integration tests specific to CHECK constraints.
 * 
 * @see {@link ConstraintConstantOperationIT} for other constraint tests
 * 
 * @author Walt Koetke
 */
//@Category(SerialTest.class)
public class CheckConstraintIT extends SpliceUnitTest {
    public static final String CLASS_NAME = CheckConstraintIT.class.getSimpleName().toUpperCase();
    private static SpliceWatcher classWatcher = new SpliceWatcher(CLASS_NAME);
    private static SpliceSchemaWatcher schemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(classWatcher)
        .around(schemaWatcher);
    
    private SpliceWatcher methodWatcher = new SpliceWatcher(CLASS_NAME);

    private void verifyNoViolation(String query, int count) throws Exception {
    	Assert.assertEquals("Invalid row count", count, methodWatcher.executeUpdate(query));
    }
    
    protected void verifyViolation(String query, String msgStart, Object... args) throws Exception {
    	try {
    		methodWatcher.executeUpdate(query);
            fail("Expected check constraint violation.");
    	} catch (SQLException e) {
    		Assert.assertTrue("Unexpected exception type", e instanceof SQLIntegrityConstraintViolationException);
            String expected = String.format(msgStart, args);
            Assert.assertEquals(e.getLocalizedMessage()+" Expected:\n"+ expected, expected, e.getLocalizedMessage());
    	}
    }

    protected static String MSG_START_DEFAULT = 
		"The check constraint '%s' was violated while performing an INSERT or UPDATE on table '%s.%s'.";

    @Test
    public void testSingleInserts() throws Exception {
        String tableName = "table1".toUpperCase();
        TableDAO tableDAO = new TableDAO(methodWatcher.getOrCreateConnection());
        tableDAO.drop(schemaWatcher.schemaName, tableName);
        methodWatcher.executeUpdate(format("create table %s ( " +
                                               "id int not null primary key constraint id_ge_ck1 check (id >= 1000), " +
                                               "i_gt int constraint i_gt_ck1 check (i_gt > 100), " +
                                               "i_eq int constraint i_eq_ck1 check (i_eq = 90), " +
                                               "i_lt int constraint i_lt_ck1 check (i_lt < 80), " +
                                               "v_neq varchar(32) constraint v_neq_ck1 check (v_neq <> 'bad'), " +
                                               "v_in varchar(32) constraint v_in_ck1 check (v_in in ('good1', " +
                                               "'good2', 'good3')))", tableName));
        methodWatcher.executeUpdate(format("insert into %s values(1000, 200, 90, 79, 'ok', 'good1')", tableName));

    	verifyNoViolation(format("insert into %s values (1001, 101, 90, 79, 'ok', 'good2')", tableName), 1); // all good

    	verifyViolation(format("insert into %s values (999, 101, 90, 79, 'ok', 'good1')", tableName), // col 1 (PK) bad
    		MSG_START_DEFAULT, "ID_GE_CK1", schemaWatcher.schemaName, tableName);

    	verifyViolation(format("insert into %s values (1002, 25, 90, 79, 'ok', 'good1')", tableName), // col 2 bad
    		MSG_START_DEFAULT, "I_GT_CK1", schemaWatcher.schemaName, tableName);

    	verifyViolation(format("insert into %s values (1003, 101, 901, 79, 'ok', 'good1')", tableName), // col 3 bad
    		MSG_START_DEFAULT, "I_EQ_CK1", schemaWatcher.schemaName, tableName);

    	verifyViolation(format("insert into %s values (1004, 101, 90, 85, 'ok', 'good2')", tableName), // col 4 bad
    		MSG_START_DEFAULT, "I_LT_CK1", schemaWatcher.schemaName, tableName);

    	verifyViolation(format("insert into %s values (1005, 101, 90, 85, 'bad', 'good3')", tableName), // col 5 bad
    		MSG_START_DEFAULT, "V_NEQ_CK1", schemaWatcher.schemaName, tableName);

    	verifyViolation(format("insert into %s values (1005, 101, 90, 85, 'ok', 'notsogood')", tableName), // col 6 bad
    		MSG_START_DEFAULT, "V_IN_CK1", schemaWatcher.schemaName, tableName);

    }

    @Test
    public void testSingleInsertsAfterAlterTable() throws Exception {
        String tableName = "table2".toUpperCase();
        TableDAO tableDAO = new TableDAO(methodWatcher.getOrCreateConnection());
        tableDAO.drop(schemaWatcher.schemaName, tableName);
        methodWatcher.executeUpdate(format("create table %s ( " +
                                               "id int not null primary key, " +
                                               "i_gt int, " +
                                               "i_eq int, " +
                                               "i_lt int, " +
                                               "v_neq varchar(32), " +
                                               "v_in varchar(32))", tableName));
        methodWatcher.executeUpdate(format("insert into %s values(1000, 200, 90, 79, 'ok', 'good1')", tableName));

        methodWatcher.executeUpdate(format("alter table %s add constraint id_ge_ck check (id >= 1000)", tableName));
        methodWatcher.executeUpdate(format("alter table %s add constraint i_gt_ck check (i_gt > 100)", tableName));
        methodWatcher.executeUpdate(format("alter table %s add constraint i_eq_ck check (i_eq = 90)", tableName));
        methodWatcher.executeUpdate(format("alter table %s add constraint i_lt_ck check (i_lt < 80)", tableName));
        methodWatcher.executeUpdate(format("alter table %s add constraint v_neq_ck check (v_neq <> 'bad')", tableName));
        methodWatcher.executeUpdate(format("alter table %s add constraint v_in_ck check (v_in in ('good1', 'good2', 'good3'))", tableName));

        verifyNoViolation(format("insert into %s values (1001, 101, 90, 79, 'ok', 'good2')", tableName), 1); // all good

    	verifyViolation(format("insert into %s values (999, 101, 90, 79, 'ok', 'good1')", tableName), // col 1 (PK) bad
    		MSG_START_DEFAULT, "ID_GE_CK", schemaWatcher.schemaName, tableName);

    	verifyViolation(format("insert into %s values (1002, 25, 90, 79, 'ok', 'good1')", tableName), // col 2 bad
    		MSG_START_DEFAULT, "I_GT_CK", schemaWatcher.schemaName, tableName);

    	verifyViolation(format("insert into %s values (1003, 101, 901, 79, 'ok', 'good1')", tableName), // col 3 bad
    		MSG_START_DEFAULT, "I_EQ_CK", schemaWatcher.schemaName, tableName);

    	verifyViolation(format("insert into %s values (1004, 101, 90, 85, 'ok', 'good2')", tableName), // col 4 bad
                        MSG_START_DEFAULT, "I_LT_CK", schemaWatcher.schemaName, tableName);

    	verifyViolation(format("insert into %s values (1005, 101, 90, 79, 'bad', 'good3')", tableName), // col 5 bad
                        MSG_START_DEFAULT, "V_NEQ_CK", schemaWatcher.schemaName, tableName);

    	verifyViolation(format("insert into %s values (1005, 101, 90, 79, 'ok', 'notsogood')", tableName), // col 6 bad
                        MSG_START_DEFAULT, "V_IN_CK", schemaWatcher.schemaName, tableName);

    }

    @Test
    public void testViolationErrorMsg() throws Exception {
        // DB-3864 - bad error msg
        String tableName = "table3".toUpperCase();
        String tableRef = schemaWatcher.schemaName+"."+tableName;
        TableDAO tableDAO = new TableDAO(methodWatcher.getOrCreateConnection());
        tableDAO.drop(schemaWatcher.schemaName, tableName);

        methodWatcher.executeUpdate(format("create table %s (int1_col int not null constraint constr_int1 check" +
                                               "(int1_col<5))", tableName));
        try {
            methodWatcher.executeUpdate(format("insert into %s values(10)", tableName));
            fail("Expected constraint violation");
        } catch (Exception e) {
            assertTrue("Expected single ticks around table ref.", e.getLocalizedMessage().contains(format("'%s'", tableRef)));
        }
    }

    @Test
    public void testAlterTableExistingDataViolatesConstraint() throws Exception {
        String tableName = "table4".toUpperCase();
        TableDAO tableDAO = new TableDAO(methodWatcher.getOrCreateConnection());
        tableDAO.drop(schemaWatcher.schemaName, tableName);
        methodWatcher.executeUpdate(format("create table %s ( " +
                                               "id int not null primary key, " +
                                               "i_gt int, " +
                                               "i_eq int, " +
                                               "i_lt int, " +
                                               "v_neq varchar(32), " +
                                               "v_in varchar(32))", tableName));
        methodWatcher.executeUpdate(format("insert into %s values(1000, 200, 90, 79, 'ok', 'good1')", tableName));
        try {
            methodWatcher.executeUpdate(format("alter table %s add constraint id_ge_ck check (id > 1000)", tableName));
            Assert.fail("Expected add or enable constraint exception");
        } catch (SQLException e) {
            Assert.assertEquals("Expected failed attempt to add check constraint.", "X0Y59", e.getSQLState());
        }
    }

    @Test
    public void testCreateAlterDropConstraint() throws Exception {
        String tableName = "table5".toUpperCase();
        TableDAO tableDAO = new TableDAO(methodWatcher.getOrCreateConnection());
        tableDAO.drop(schemaWatcher.schemaName, tableName);
        String constraintName = "ten_or_less";

        methodWatcher.executeUpdate(format("create table %s (col1 int not null)", tableName));
        methodWatcher.executeUpdate(format("insert into %s values(9)", tableName));
        methodWatcher.executeUpdate(format("alter table %s add constraint %s check (col1 < 10)", tableName, constraintName));
        try {
            methodWatcher.executeUpdate(format("insert into %s values (11)", tableName));
            fail("Expected check constraint violation.");
        } catch (SQLException e) {
            // expected
            assertEquals("Expected constraint violation", "23513", e.getSQLState());
        }
        methodWatcher.executeUpdate(format("alter table %s drop constraint %s", tableName, constraintName));
        methodWatcher.executeUpdate(format("insert into %s values (11)", tableName));
    }

    // TODO: Additional tests to consider:
    //
    // more data types and operators
    // alter table and create table usage
    // insert multiple rows at a time
    // updates
    // disable/enable
    
}
