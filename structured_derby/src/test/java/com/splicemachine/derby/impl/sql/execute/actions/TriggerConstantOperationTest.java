package com.splicemachine.derby.impl.sql.execute.actions;

import com.splicemachine.derby.test.framework.*;
import com.splicemachine.homeless.TestUtils;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Ignore("Bug 541")
public class TriggerConstantOperationTest extends SpliceUnitTest {
    public static final String CLASS_NAME = TriggerConstantOperationTest.class.getSimpleName().toUpperCase();

    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
    public static final String TABLE_NAME_1A = "TASKS";
    public static final String TABLE_NAME_1B = "COMPLETED_TASKS";

    public static final String TRIG_NAME_1 = "TRIGGER_1";
    public static final String TRIG_NAME_2 = "TRIGGER_2";

    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);
    protected static SpliceSchemaWatcher spliceTriggerSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME+"_TRIGGER");

    private static String tableDef = "(TaskId INT NOT NULL, empId Varchar(3) NOT NULL, StartedAt INT NOT NULL, FinishedAt INT NOT NULL)";
    private static String triggerTableDef = "(TaskId INT NOT NULL, empId Varchar(3) NOT NULL, Completed TIMESTAMP NOT NULL, Comment Varchar(3) NOT NULL)";
    protected static SpliceTableWatcher spliceTableWatcher1 = new SpliceTableWatcher(TABLE_NAME_1A,CLASS_NAME, tableDef);
    protected static SpliceTableWatcher spliceTableWatcher2 = new SpliceTableWatcher(TABLE_NAME_1B,CLASS_NAME, triggerTableDef);

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher)
            .around(spliceTriggerSchemaWatcher)
            .around(spliceTableWatcher1)
            .around(spliceTableWatcher2);

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher();

    /**
     * Test basic trigger creation and drop.
     * @throws Exception
     */
    @Test
    @Ignore("Bug 541")
    public void testCreateDropTrigger() throws Exception{
        String triggerCreateStr =
                String.format("AFTER INSERT ON %s REFERENCING NEW AS INSROW FOR EACH ROW MODE DB2SQL INSERT INTO %s VALUES (INSROW.TaskId, INSROW.empId, CURRENT_TIMESTAMP, 'INSERTED BY %s')",
                        this.getTableReference(TABLE_NAME_1A),
                        this.getTableReference(TABLE_NAME_1B),
                        TRIG_NAME_1);
        SpliceTriggerWatcher tw = new SpliceTriggerWatcher(TRIG_NAME_1,spliceSchemaWatcher.schemaName,triggerCreateStr);
        tw.starting(null);
        // now drop
        tw.finished(null);
    }

    /**
     * Test basic trigger creation and drop but create in schema other than its table.
     * @throws Exception
     */
    @Test
    public void testCreateTriggerInDiffSchema() throws Exception {
        String triggerCreateStr =
                String.format("AFTER INSERT ON %s REFERENCING NEW AS INSROW FOR EACH ROW MODE DB2SQL INSERT INTO %s VALUES (INSROW.TaskId, INSROW.empId, CURRENT_TIMESTAMP, 'INSERTED BY %s')",
                        this.getTableReference(TABLE_NAME_1A),
                        this.getTableReference(TABLE_NAME_1B),
                        TRIG_NAME_2);
        SpliceTriggerWatcher tw = new SpliceTriggerWatcher(TRIG_NAME_2,spliceTriggerSchemaWatcher.schemaName,triggerCreateStr);
        tw.starting(null);
        // now drop
        tw.finished(null);
    }

    /**
     * Test basic trigger creation .
     * @throws Exception
     */
    @Test
    @Ignore
    public void testCreateTriggerInsert() throws Exception{
        String triggerCreateStr =
                String.format("AFTER INSERT ON %s REFERENCING NEW AS INSROW FOR EACH ROW MODE DB2SQL INSERT INTO %s VALUES (INSROW.TaskId, INSROW.empId, CURRENT_TIMESTAMP, 'INSERTED BY %s')",
                        this.getTableReference(TABLE_NAME_1A),
                        this.getTableReference(TABLE_NAME_1B),
                        TRIG_NAME_1);
        new SpliceTriggerWatcher(TRIG_NAME_1,CLASS_NAME,triggerCreateStr).starting(null);

        // insert into table
        Connection connection1 = methodWatcher.createConnection();
        connection1.createStatement().execute(
                String.format("insert into %1$s (TaskId, empId, StartedAt, FinishedAt) values (%2$d,'JC',09%3$d,09%4$d)",
                        this.getTableReference(TABLE_NAME_1A), 1244, 0, 30));

        // read from trigger table
        ResultSet rs = connection1.createStatement().executeQuery(String.format("select * from %s", this.getTableReference(TABLE_NAME_1B)));
        List<Map> result = TestUtils.resultSetToMaps(rs);
        // expecting one row
        Assert.assertEquals(1,result.size());
        for (Object blob : result.get(0).entrySet()) {
            Map.Entry entry = (Map.Entry)blob;
            System.out.println(entry.getKey() + " - " + entry.getValue());
        }
    }
}
