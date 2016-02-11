package com.splicemachine.triggers;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.test_dao.TableDAO;
import com.splicemachine.test_dao.TriggerBuilder;
import com.splicemachine.test_dao.TriggerDAO;
import org.junit.*;

import java.sql.SQLException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Tests that triggers are dropped when the table or column(s) they depend on are dropped.
 */
public class Trigger_Dependency_IT {

    private static final String SCHEMA = Trigger_Dependency_IT.class.getSimpleName();

    @ClassRule
    public static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(SCHEMA);

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(SCHEMA);

    private TriggerBuilder tb = new TriggerBuilder();
    private TriggerDAO triggerDAO = new TriggerDAO(methodWatcher.getOrCreateConnection());

    @Before
    public void createTables() throws Exception {
        new TableDAO(methodWatcher.getOrCreateConnection()).drop(SCHEMA, "R", "T");
        methodWatcher.executeUpdate("create table T (a int, b int, c int)");
        methodWatcher.executeUpdate("create table R (z int)");
    }

    @Test
    public void dropTableDropsTriggers() throws Exception {
        // given
        createTrigger(tb.named("trig1").after().update().of("a").on("T").row().then("INSERT INTO R VALUES(1)"));
        createTrigger(tb.named("trig2").after().update().of("b").on("T").row().then("INSERT INTO R VALUES(2)"));
        triggerDAO.assertTriggerExists("trig1", "trig2");

        // when - drop table
        methodWatcher.executeUpdate("DROP TABLE T");

        // then - triggers gone
        triggerDAO.assertTriggerGone("trig1");
        triggerDAO.assertTriggerGone("trig2");
    }

    @Test
    public void dropColumnDropsTrigger() throws Exception {
        // given
        createTrigger(tb.named("trig1").after().update().of("a").on("T").statement().then("INSERT INTO R VALUES(1)"));
        createTrigger(tb.named("trig2").after().update().of("b").on("T").statement().then("INSERT INTO R VALUES(1)"));
        createTrigger(tb.named("trig3").after().update().of("c").on("T").statement().then("INSERT INTO R VALUES(1)"));

        // when - drop column b
        methodWatcher.executeUpdate("ALTER TABLE T DROP COLUMN b");

        // then - trigger2 is gone, but others are still there.
        triggerDAO.assertTriggerGone("trig2");
        triggerDAO.assertTriggerExists("trig1", "trig3");
    }

    @Test
    public void dropTableReferencedFromTriggerActionInvalidatesButDoesNotDropTrigger() throws Exception {
        // given
        createTrigger(tb.named("trig1").after().insert().on("T").statement().then("INSERT INTO R VALUES(1)"));

        // when - drop action-referenced table
        methodWatcher.executeUpdate("DROP TABLE R");

        // then - trigger still exists
        triggerDAO.assertTriggerExists("trig1");

        // but trigger will not fire
        try {
            methodWatcher.executeUpdate("insert into T values(1,1,1)");
            fail("expected insert statement to fail because related trigger should fail");
        } catch (SQLException e) {
            assertEquals("Table/View 'R' does not exist.", e.getMessage());
        }
    }


    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    //
    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    private void createTrigger(TriggerBuilder tb) throws Exception {
        methodWatcher.executeUpdate(tb.build());
    }
}
