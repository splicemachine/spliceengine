package com.splicemachine.triggers;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.test_dao.TriggerBuilder;
import com.splicemachine.test_dao.TriggerDAO;
import org.junit.*;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;

import static java.lang.System.currentTimeMillis;
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
        createTrigger(tb.after().update().on("T").row().then("INSERT INTO RECORD VALUES('after-update')"));

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
        createTrigger(tb.after().update().of("a,c").on("T").row().then("INSERT INTO RECORD VALUES('after-update')"));

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
        createTrigger(tb.after().insert().on("T").row().then("INSERT INTO RECORD VALUES('after-insert')"));

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
        createTrigger(tb.after().delete().on("T").row().then("INSERT INTO RECORD VALUES('after-delete')"));

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

        createTrigger(tb.after().insert().on("T").row().then("insert into RECORD select * from RECORD"));

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

        createTrigger(tb.after().update().on("T").row().then("insert into RECORD select * from RECORD"));

        // when - update all rows
        methodWatcher.executeUpdate("update T set c = c + 1");
        // this would be 2^6 if our trigger actions executed sequentially
        assertRecordCount("aaa", 64);
    }

    /* DB-3391: trigger would fail to fire if trigger statement was one-row delete/update on PK or unique index */
    @Test
    public void afterDelete_deleteTriggerAction() throws Exception {
        // given -- a row in the RECORD table, and a trigger that should delete it
        methodWatcher.executeUpdate("create table T3391 (a bigint primary key, b bigint unique, c bigint)");
        methodWatcher.executeUpdate("insert into T3391 values(1,1,1),(2,2,2)");
        methodWatcher.executeUpdate("insert into RECORD values('aaa'), ('bbb')");
        createTrigger(tb.named("t3391a").after().delete().on("T3391").row().then("delete from RECORD where text='aaa'"));
        createTrigger(tb.named("t3391b").after().delete().on("T3391").row().then("delete from RECORD where text='bbb'"));
        assertRecordCount("aaa", 1);
        assertRecordCount("bbb", 1);

        // when - that trigger is fired
        methodWatcher.executeUpdate("delete from T3391 where a = 1");
        methodWatcher.executeUpdate("delete from T3391 where b = 2");

        assertRecordCount("bbb", 0);
        assertRecordCount("aaa", 0);
    }

    // DB-3354: Nested triggers produce null transition variables.
    @Test
    public void afterInsertCascadingTriggers() throws Exception {
        methodWatcher.executeUpdate("create table cascade1 (a int)");
        methodWatcher.executeUpdate("create table cascade2 (a int, name varchar(20), t timestamp)");
        methodWatcher.executeUpdate("create table cascade3 (a int, name varchar(20), t timestamp)");

        createTrigger(tb.named("cascade1_insert").after().insert().on("cascade1").referencing("NEW as NEW").row().
                then("insert into cascade2(a, name, t) values (NEW.a, 'cascade1_insert', CURRENT_TIMESTAMP)"));
        createTrigger(tb.named("cascade2_insert").after().insert().on("cascade2").referencing("NEW as NEW").row().
                then("insert into cascade3(a, name, t) values (NEW.a, 'cascade2_insert', CURRENT_TIMESTAMP)"));

        // when -- fire the trigger
        methodWatcher.executeUpdate("insert into cascade1 (a) values (1)");

        // then -- verify content of table 1
        assertEquals(1, methodWatcher.query("select a from cascade1"));

        // then -- verify content of table 2
        assertEquals(1, methodWatcher.query("select a from cascade2"));
        assertEquals("cascade1_insert", methodWatcher.query("select name from cascade2"));
        Timestamp ts2 = methodWatcher.query("select t from cascade2");

        // then -- verify content of table 3
        assertEquals(1, methodWatcher.query("select a from cascade3"));
        assertEquals("cascade2_insert", methodWatcher.query("select name from cascade3"));
        Timestamp ts3 = methodWatcher.query("select t from cascade3");

        assertTrue(ts2.before(ts3));
        assertTrue("ts should be close to current time", Math.abs(currentTimeMillis() - ts2.getTime()) < 3000);
        assertTrue("ts should be close to current time", Math.abs(currentTimeMillis() - ts3.getTime()) < 3000);
    }


    //- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    //
    // BEFORE row triggers -- cannot update/insert, so we verify that they fire with trigger action that throws.
    //
    //- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    @Test
    public void beforeUpdate() throws Exception {
        createTrigger(tb.before().update().on("T").row().then("select 1/0 from sys.systables"));
        assertQueryFails("update T set b = b * 2 where a <= 4", "Attempt to divide by zero.");
    }

    @Test
    public void beforeInsert() throws Exception {
        createTrigger(tb.before().insert().on("T").row().then("select 1/0 from sys.systables"));
        assertQueryFails("insert into T values(8,8,8)", "Attempt to divide by zero.");
    }

    @Test
    public void beforeDelete() throws Exception {
        createTrigger(tb.before().delete().on("T").row().then("select 1/0 from sys.systables"));
        assertQueryFails("delete from T where c = 6", "Attempt to divide by zero.");
    }

    //- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    //
    // Triggers and Constraint violations
    //
    //- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    @Test
    public void afterInsertWithUniqueConstraintViolation() throws Exception {
        methodWatcher.executeUpdate("create table T2(a int, constraint T_INDEX1 unique(a))");
        methodWatcher.executeUpdate("insert into T2 values(1),(2),(3)");

        /* When the triggering statement is rolled back because of a constraint violation we can't use the effect of
         * the trigger to test if it fired when that effect is also rolled back.  The SET_GLOBAL_DATABASE_PROPERTY
         * stored procedure is non transactional.
         */
        createTrigger(tb.named("constraintTrig1").after().insert().on("T2").row()
                .then("call syscs_util.SYSCS_SET_GLOBAL_DATABASE_PROPERTY('triggerRowItPropA', 'someValue')"));

        // when - insert over select
        assertQueryFails("insert into T2 select * from T2", "The statement was aborted because it would have caused a duplicate key value in a unique or primary key constraint or unique index identified by 'T_INDEX1' defined on 'T2'.");
        // when - insert over values
        assertQueryFails("insert into T2 values(1)", "The statement was aborted because it would have caused a duplicate key value in a unique or primary key constraint or unique index identified by 'T_INDEX1' defined on 'T2'.");

        // Trigger should NOT have fired.
        Connection connection = methodWatcher.createConnection();
        ResultSet rs = connection.prepareCall("call syscs_util.SYSCS_GET_GLOBAL_DATABASE_PROPERTY('triggerRowItPropA')").executeQuery();
        rs.next();
        assertNull(rs.getString(2));
    }

    @Test
    public void afterUpdateWithUniqueConstraintViolation() throws Exception {
        methodWatcher.executeUpdate("create table T3(a int, constraint T_INDEX2 unique(a))");
        methodWatcher.executeUpdate("insert into T3 values(1),(2),(3)");

        createTrigger(tb.named("constraintTrig2").after().update().on("T3").row()
                .then("call syscs_util.SYSCS_SET_GLOBAL_DATABASE_PROPERTY('triggerRowItPropB', 'someValue')"));

        // when - update
        assertQueryFails("update T3 set a=1 where a=3", "The statement was aborted because it would have caused a duplicate key value in a unique or primary key constraint or unique index identified by 'T_INDEX2' defined on 'T3'.");

        // Trigger should NOT have fired.
        Connection connection = methodWatcher.createConnection();
        ResultSet rs = connection.prepareCall("call syscs_util.SYSCS_GET_GLOBAL_DATABASE_PROPERTY('triggerRowItPropB')").executeQuery();
        rs.next();
        assertNull(rs.getString(2));
    }

    //- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    //
    // Misc other tests
    //
    //- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    @Test
    public void multipleRowAndStatementTriggersOnOneTable() throws Exception {
        // given - six row triggers on same table.
        createTrigger(tb.named("u_1").after().update().on("T").row().then("INSERT INTO RECORD VALUES('u1')"));
        createTrigger(tb.named("u_2").after().update().on("T").row().then("INSERT INTO RECORD VALUES('u2')"));
        createTrigger(tb.named("i_1").after().insert().on("T").row().then("INSERT INTO RECORD VALUES('i1')"));
        createTrigger(tb.named("i_2").after().insert().on("T").row().then("INSERT INTO RECORD VALUES('i2')"));
        createTrigger(tb.named("d_1").after().delete().on("T").row().then("INSERT INTO RECORD VALUES('d1')"));
        createTrigger(tb.named("d_2").after().delete().on("T").row().then("INSERT INTO RECORD VALUES('d2')"));

        // given - three statement triggers
        createTrigger(tb.named("u_stat").after().update().on("T").statement().then("INSERT INTO RECORD VALUES('statement-1')"));
        createTrigger(tb.named("i_stat").after().insert().on("T").statement().then("INSERT INTO RECORD VALUES('statement-2')"));
        createTrigger(tb.named("d_stat").after().delete().on("T").statement().then("INSERT INTO RECORD VALUES('statement-3')"));

        // when - update
        methodWatcher.executeUpdate("update T set c = 0 where c=1 or c=2 or c=3");
        assertRecordCount("u1", 3);
        assertRecordCount("u2", 3);
        assertRecordCount("statement-1", 1);
        // when - insert
        methodWatcher.executeUpdate("insert into T values(7,7,7),(8,8,8),(9,9,9),(10,10,10)");
        assertRecordCount("i1", 4);
        assertRecordCount("i2", 4);
        assertRecordCount("statement-2", 1);
        // when - delete
        methodWatcher.executeUpdate("delete from T where c=4 or c=5 or c=6 or c=7 or c=8");
        assertRecordCount("d1", 5);
        assertRecordCount("d2", 5);
        assertRecordCount("statement-3", 1);
    }

    @Test
    public void oldPreparedStatementsFireNewRowTriggers() throws Exception {
        // given - create a prepared statement and execute it so we are sure that it is compiled
        PreparedStatement ps = methodWatcher.getOrCreateConnection().prepareStatement("update T set a = a + 1");
        assertEquals(6, ps.executeUpdate());

        // given - define the trigger after ps is created/compiled
        createTrigger(tb.named("u_1").after().update().on("T").row().then("INSERT INTO RECORD VALUES('u1')"));

        // when - execute ps again
        assertEquals(6, ps.executeUpdate());

        // then - trigger still fires
        assertRecordCount("u1", 6);
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

    private void createTrigger(TriggerBuilder tb) throws Exception {
        methodWatcher.executeUpdate(tb.build());
    }

}