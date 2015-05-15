package com.splicemachine.triggers;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.test_dao.TriggerBuilder;
import com.splicemachine.test_dao.TriggerDAO;
import org.junit.*;

import static org.junit.Assert.*;

/**
 * Test ROW triggers.
 */
public class Trigger_Row_IT {

    private static final String SCHEMA = Trigger_Row_IT.class.getSimpleName();

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
        classWatcher.executeUpdate("create table T (a int, b int, c int)");
        classWatcher.executeUpdate("create table RECORD (text varchar(99))");
    }

    /* Each test starts with same table state */
    @Before
    public void initTable() throws Exception {
        triggerDAO.dropAllTriggers("T");
        classWatcher.executeUpdate("delete from T");
        classWatcher.executeUpdate("delete from RECORD");
        classWatcher.executeUpdate("insert into T values(1,1,1),(2,2,2),(3,3,3),(4,4,4),(5,5,5),(6,6,6)");
    }

    //- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    //
    // AFTER triggers
    //
    //- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    @Test
    public void afterUpdate() throws Exception {
        methodWatcher.executeUpdate(tb.after().update().on("T").row().then("INSERT INTO RECORD VALUES('after-update')").build());

        // when - update all rows
        long updateCount1 = methodWatcher.executeUpdate("update T set c = c + 1");
        assertRecordCount("after-update", updateCount1);

        // when -- update one row
        long updateCount2 = methodWatcher.executeUpdate("update T set c = c + 1 where a = 1");
        assertRecordCount("after-update", updateCount1 + updateCount2);

        // when -- update two rows
        long updateCount3 = methodWatcher.executeUpdate("update T set c = c + 1 where a = 1 or a = 2");
        assertRecordCount("after-update", updateCount1 + updateCount2 + updateCount3);

        assertNonZero(updateCount1, updateCount2, updateCount3);

        // when -- update ZERO rows
        long updateCount4 = methodWatcher.executeUpdate("update T set c = c + 1 where a = -9999999");
        assertRecordCount("after-update", updateCount1 + updateCount2 + updateCount3);
        assertEquals(0, updateCount4);
    }

    /* Trigger on subset of columns */
    @Test
    public void afterUpdateOfColumns() throws Exception {
        methodWatcher.executeUpdate(tb.after().update().of("a,c").on("T").row().then("INSERT INTO RECORD VALUES('after-update')").build());

        // when - update non trigger col
         methodWatcher.executeUpdate("update T set b = 100");
        assertRecordCount("after-update", 0);

        // when -- update trigger col 1
        methodWatcher.executeUpdate("update T set a = 100");
        assertRecordCount("after-update", 6);

        // when -- update trigger col 2
        methodWatcher.executeUpdate("update T set c = 100");
        assertRecordCount("after-update", 12);
    }

    @Test
    public void afterInsert() throws Exception {
        methodWatcher.executeUpdate(tb.after().insert().on("T").row().then("INSERT INTO RECORD VALUES('after-insert')").build());

        // when - insert over select
        long insertCount1 = methodWatcher.executeUpdate("insert into T select * from T");
        assertRecordCount("after-insert", insertCount1);

        // when -- insert over values
        long insertCount2 = methodWatcher.executeUpdate("insert into T values(8,8,8),(9,9,9)");
        assertRecordCount("after-insert", insertCount1 + insertCount2);

        // when -- insert over more values
        long insertCount3 = methodWatcher.executeUpdate("insert into T values(8,8,8),(9,9,9),(10,10,10)");
        assertRecordCount("after-insert", insertCount1 + insertCount2 + insertCount3);

        assertNonZero(insertCount1, insertCount2, insertCount3);
    }

    @Test
    public void afterDelete() throws Exception {
        methodWatcher.executeUpdate(tb.after().delete().on("T").row().then("INSERT INTO RECORD VALUES('after-delete')").build());

        // when - delete
        long deleteCount1 = methodWatcher.executeUpdate("delete from T where a >=4");
        assertRecordCount("after-delete", deleteCount1);

        // when -- delete all
        long deleteCount2 = methodWatcher.executeUpdate("delete from T");
        assertRecordCount("after-delete", deleteCount1 + deleteCount2);

        assertNonZero(deleteCount1, deleteCount2);

        // when -- delete ZERO rows
        long deleteCount3 = methodWatcher.executeUpdate("delete from T where a = -9999999");
        assertRecordCount("after-delete", deleteCount1 + deleteCount2);
        assertEquals(0, deleteCount3);
    }

    //- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    //
    // AFTER row triggers with a sinking action -- at the time I'm writing this the insert over values trigger actions
    // used in other tests in this class do not use the task framework; adding a few tests here with trigger actions
    // that do.
    //
    //- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    @Test
    public void afterInsert_sinkingTriggerAction() throws Exception {
        methodWatcher.executeUpdate("insert into RECORD values('aaa')");

        methodWatcher.executeUpdate(tb.after().insert().on("T").row().then("insert into RECORD select * from RECORD").build());

        // when - insert over select
        methodWatcher.executeUpdate("insert into T values(1,1,1)");
        assertRecordCount("aaa", 2);
        methodWatcher.executeUpdate("insert into T values(8,8,8)");
        assertRecordCount("aaa", 4);
        methodWatcher.executeUpdate("insert into T values(8,8,8)");
        assertRecordCount("aaa", 8);
        methodWatcher.executeUpdate("insert into T values(8,8,8)");
        assertRecordCount("aaa", 16);
    }

    @Test
    public void afterUpdate_sinkingTriggerAction() throws Exception {
        methodWatcher.executeUpdate("insert into RECORD values('aaa')");

        methodWatcher.executeUpdate(tb.after().update().on("T").row().then("insert into RECORD select * from RECORD").build());

        // when - update all rows
        methodWatcher.executeUpdate("update T set c = c + 1");
        // this should actually be 2^6
        assertRecordCount("aaa", 7);
    }


    //- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    //
    // BEFORE row triggers -- cannot update/insert, so we verify that they fire with trigger action that throws.
    //
    //- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    @Test
    public void beforeUpdate() throws Exception {
        methodWatcher.executeUpdate(tb.before().update().on("T").row().then("select 1/0 from sys.systables").build());
        assertQueryFails("update T set b = b * 2 where a <= 4", "Attempt to divide by zero.");
    }

    @Test
    public void beforeInsert() throws Exception {
        methodWatcher.executeUpdate(tb.before().insert().on("T").row().then("select 1/0 from sys.systables").build());
        assertQueryFails("insert into T values(8,8,8)", "Attempt to divide by zero.");
    }

    @Test
    public void beforeDelete() throws Exception {
        methodWatcher.executeUpdate(tb.before().delete().on("T").row().then("select 1/0 from sys.systables").build());
        assertQueryFails("delete from T where c = 6", "Attempt to divide by zero.");
    }

    //- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    //
    //- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    private void assertRecordCount(String tag, long expectedCount) throws Exception {
        Long actualCount = methodWatcher.query("select count(*) from RECORD where text='" + tag + "'");
        assertEquals("Didn't find expected number of rows:", expectedCount, actualCount.longValue());
    }

    private void assertQueryFails(String query, String expectedError) {
        try {
            methodWatcher.executeUpdate(query);
            fail("expected to fail with message = " + expectedError);
        } catch (Exception e) {
            assertEquals(expectedError, e.getMessage());
        }
    }

    private void assertNonZero(long... values) {
        for (long i : values) {
            assertTrue(i > 0);
        }
    }

}