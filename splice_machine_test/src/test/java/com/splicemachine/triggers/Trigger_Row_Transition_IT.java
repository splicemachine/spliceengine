package com.splicemachine.triggers;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.test_dao.TriggerBuilder;
import com.splicemachine.test_dao.TriggerDAO;
import org.junit.*;

import java.sql.SQLException;

import static org.junit.Assert.*;

/**
 * Test ROW triggers with transition variables.
 */
public class Trigger_Row_Transition_IT {

    private static final String SCHEMA = Trigger_Row_Transition_IT.class.getSimpleName();

    @ClassRule
    public static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(SCHEMA);

    @ClassRule
    public static SpliceWatcher classWatcher = new SpliceWatcher(SCHEMA);

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(SCHEMA);

    private TriggerBuilder tb = new TriggerBuilder();
    private TriggerDAO triggerDAO = new TriggerDAO(methodWatcher.getOrCreateConnection());

    /* Create tables once */
    @BeforeClass
    public static void createSharedTables() throws Exception {
        classWatcher.executeUpdate("create table T (a varchar(9), b int)");
        classWatcher.executeUpdate("create table RECORD_OLD (a varchar(9), b int)");
        classWatcher.executeUpdate("create table RECORD_NEW (a varchar(9), b int)");
    }

    /* Each test starts with same table state */
    @Before
    public void initTable() throws Exception {
        triggerDAO.dropAllTriggers("T");
        classWatcher.executeUpdate("delete from T");
        classWatcher.executeUpdate("delete from RECORD_OLD");
        classWatcher.executeUpdate("delete from RECORD_NEW");
        classWatcher.executeUpdate("insert into T values('AAA',1),('BBB',2),('CCC',3)");
    }

    @Test
    public void afterUpdate() throws Exception {
        methodWatcher.executeUpdate(tb.named("trig1").after().update().on("T").referencing("OLD AS O")
                .row().then("INSERT INTO RECORD_OLD VALUES(O.a, O.b)").build());

        // when - update a row
        methodWatcher.executeUpdate("update T set b = 2000, a='ZZZ' where a='BBB'");

        assertEquals(1L, methodWatcher.query("select count(*) from RECORD_OLD where a='BBB' and b=2"));
    }

    @Test
    public void afterDelete() throws Exception {
        methodWatcher.executeUpdate(tb.named("trig1").after().delete().on("T").referencing("OLD AS O")
                .row().then("INSERT INTO RECORD_OLD VALUES(O.a, O.b)").build());

        // when - update a row
        methodWatcher.executeUpdate("delete from T where b = 2");

        assertEquals(1L, methodWatcher.query("select count(*) from RECORD_OLD where a='BBB' and b=2"));
    }

    @Test
    public void afterInsert() throws Exception {
        methodWatcher.executeUpdate(tb.named("trig1").after().insert().on("T").referencing("NEW AS N")
                .row().then("INSERT INTO RECORD_NEW VALUES(N.a, N.b)").build());

        // when - update a row
        methodWatcher.executeUpdate("insert into T values('DDD', 4),('EEE', 5)");

        assertEquals(1L, methodWatcher.query("select count(*) from RECORD_NEW where a='DDD' and b=4"));
        assertEquals(1L, methodWatcher.query("select count(*) from RECORD_NEW where a='EEE' and b=5"));
    }

    @Test
    public void beforeUpdate() throws Exception {
        methodWatcher.executeUpdate(tb.named("trig1").before().update().on("T").referencing("OLD AS O")
                .row().then("select O.b/0 from sys.systables").build());

        // when - update a row
        try {
            methodWatcher.executeUpdate("update T set b = 2000, a='ZZZ' where a='BBB'");
            fail("expected trigger to cause exception");
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("divide by zero"));
        }

    }


}