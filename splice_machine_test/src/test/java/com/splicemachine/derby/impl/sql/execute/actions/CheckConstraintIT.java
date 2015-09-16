package com.splicemachine.derby.impl.sql.execute.actions;

import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;

import com.splicemachine.derby.test.framework.*;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;

import com.splicemachine.test.SerialTest;
import com.splicemachine.test_tools.TableCreator;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
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

        new TableCreator(conn) // maybe insert some base 'good' rows too
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
            Assert.fail("Expected check constraint violation.");
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

    // Additional tests to consider:
    //
    // more data types and operators
    // alter table and create table usage
    // insert multiple rows at a time
    // updates
    // disable/enable
    
}
