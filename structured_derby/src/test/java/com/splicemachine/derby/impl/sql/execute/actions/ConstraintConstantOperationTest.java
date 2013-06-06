package com.splicemachine.derby.impl.sql.execute.actions;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;

import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Test constraints.
 *
 * Note; this test currently broken (Bug 535). Seems a table created with a constraint is in an error state:
 *
 * splice> create table fred.bill (name varchar(40), val int, constraint gtzero check (val > 0));
 * 0 rows inserted/updated/deleted
 *
 * splice> describe fred.bill;
 * COLUMN_NAME         |TYPE_NAME|DEC&|NUM&|COLUM&|COLUMN_DEF|CHAR_OCTE&|IS_NULL&
 * ------------------------------------------------------------------------------
 * NAME                |VARCHAR  |NULL|NULL|40    |NULL      |80        |YES
 * VAL                 |INTEGER  |0   |10  |10    |NULL      |NULL      |YES
 *
 * 2 rows selected
 *
 * splice> alter table fred.bill drop constraint gtzero;
 * ERROR 08006: A network protocol error was encountered and the connection has been terminated: the requested command encountered an unarchitected and implementation-specific condition for which there was no architected message (additional information may be available in the derby.log file on the server)
 * ERROR XJ001: DERBY SQL error: SQLCODE: -1, SQLSTATE: XJ001, SQLERRMC: java.lang.ClassCastExceptionorg.apache.derby.iapi.sql.dictionary.SubCheckConstraintDescriptor cannot be cast to org.apache.derby.iapi.sql.dictionary.SubKeyConstraintDescriptorXJ001.U
 * ERROR 08003: DERBY SQL error: SQLCODE: -1, SQLSTATE: 08003, SQLERRMC: No current connection.
 *
 */
@Ignore("Bug 535")
public class ConstraintConstantOperationTest extends SpliceUnitTest {

    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
    protected static SpliceSchemaWatcher spliceSchemaWatcher =
            new SpliceSchemaWatcher(ConstraintConstantOperationTest.class.getSimpleName());
    protected static SpliceTableWatcher spliceTableWatcher =
            new SpliceTableWatcher("Tasks",ConstraintConstantOperationTest.class.getSimpleName(),
    "(TaskId INT, StartedAt INT, FinishedAt INT, CONSTRAINT CHK_StartedAt_Before_FinishedAt CHECK (StartedAt < FinishedAt))");

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher)
            .around(spliceTableWatcher);

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher();

    @Test
    @Ignore("Bug 535")
    public void testInsertGoodRow() throws Exception{
        methodWatcher.getStatement().execute("insert into"+this.getPaddedTableReference("Tasks")+"(TaskId, StartedAt, FinishedAt) values (1234,0500,0600)");
        ResultSet rs = methodWatcher.executeQuery(format("select * from %s where name = '%s'", this.getTableReference("Tasks"), 1234));
        Assert.assertTrue(rs.next());
    }

    @Test(expected=SQLException.class)
    @Ignore("Bug 535")
    public void testInsertBadRow() throws Exception{
        methodWatcher.getStatement().execute("insert into" + this.getPaddedTableReference("Tasks") + "(TaskId, StartedAt, FinishedAt) values (1235,0601,0600)");
    }

}
