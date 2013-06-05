package com.splicemachine.derby.impl.sql.execute.actions;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
<<<<<<< HEAD

@Ignore("Bug 535")
=======
@Ignore
>>>>>>> Commented out class until ready
public class ConstraintConstantOperationTest extends SpliceUnitTest {

    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
    protected static SpliceSchemaWatcher spliceSchemaWatcher =
            new SpliceSchemaWatcher(ConstraintConstantOperationTest.class.getSimpleName());
    protected static SpliceTableWatcher spliceTableWatcher =
            new SpliceTableWatcher("Tasks",ConstraintConstantOperationTest.class.getSimpleName(),
    "(TaskId INT, StartedAt INT, FinishedAt INT, CONSTRAINT CHK_StartedAt_Before_FinishedAt CHECK ( StartedAt < FinishedAt ))");

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
    }

    @Test
    @Ignore("Bug 535")
    public void testInsertBadRow() throws Exception{
        methodWatcher.getStatement().execute("insert into"+this.getPaddedTableReference("Tasks")+"(TaskId, StartedAt, FinishedAt) values (1235,0601,0600)");
    }

}
