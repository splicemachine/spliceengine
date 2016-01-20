package com.splicemachine.triggers;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.concurrent.TimeUnit;

import org.junit.*;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.test_dao.TriggerBuilder;
import com.splicemachine.test_dao.TriggerDAO;

/**
 * Test ROW triggers with transition variables.
 */
@Ignore("DB-4272")
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
        triggerDAO.dropAllTriggers("simulate");
        triggerDAO.dropAllTriggers("recursion_test");
        classWatcher.executeUpdate("delete from T");
        classWatcher.executeUpdate("delete from RECORD_OLD");
        classWatcher.executeUpdate("delete from RECORD_NEW");
        classWatcher.executeUpdate("insert into T values('AAA',1),('BBB',2),('CCC',3)");
        classWatcher.executeUpdate("drop table if exists recursion_test");
    }

    @Test
    public void afterUpdateTriggerUpdatesOwnTable() throws Exception {
        methodWatcher.executeUpdate(tb.named("trig1").after().update().on("T").referencing("NEW AS N")
                .row().then("INSERT INTO T VALUES(N.a, N.b)").build());

        // when - update a row
        methodWatcher.executeUpdate("update T set b = 2000 where a='BBB'");

        // then -- updated row and row from trigger
        assertEquals(2L, methodWatcher.query("select count(*) from T where b=2000"));
    }

    @Test
    public void afterDelete() throws Exception {
        methodWatcher.executeUpdate(tb.named("trig1").after().delete().on("T").referencing("OLD AS O")
                .row().then("INSERT INTO RECORD_OLD VALUES(O.a, O.b)").build());

        // when - delete a row
        methodWatcher.executeUpdate("delete from T where b = 2");

        assertEquals(1L, methodWatcher.query("select count(*) from RECORD_OLD where a='BBB' and b=2"));
    }

    @Test
    public void afterInsert() throws Exception {
        methodWatcher.executeUpdate(tb.named("trig1").after().insert().on("T").referencing("NEW AS N")
                .row().then("INSERT INTO RECORD_NEW VALUES(N.a, N.b)").build());

        // when - insert a row
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

    @Test
    public void afterUpdate() throws Exception {
        methodWatcher.executeUpdate(tb.named("trig1").after().update().on("T").referencing("OLD AS O")
                .row().then("INSERT INTO RECORD_OLD VALUES(O.a, O.b)").build());

        // when - update a row
        methodWatcher.executeUpdate("update T set b = 2000, a='ZZZ' where a='BBB'");

        assertEquals(1L, methodWatcher.query("select count(*) from RECORD_OLD where a='BBB' and b=2"));
    }

    @Test
    public void afterUpdateTransitionNew() throws Exception {
        // DB-3570: transition values - always seeing old
        methodWatcher.executeUpdate(tb.named("trig4").after().update().on("T").referencing("NEW AS N")
                                      .row().then("INSERT INTO RECORD_NEW VALUES(N.a, N.b)").build());

        // when - update a row
        methodWatcher.executeUpdate("update T set a='new', b=999 where a='CCC'");

        assertEquals(1L, methodWatcher.query("select count(*) from RECORD_NEW where a='new' and b=999"));
    }

    @Test
    public void afterUpdateTransitionNewTwoTriggers() throws Exception {
        // DB-3570: transition values - always seeing old (event being cleared prematurely)
        methodWatcher.executeUpdate("create table t_master(id int, col1 int, col2 int, col3 int, col4 char(10), col5 varchar(20))");
        methodWatcher.executeUpdate("insert into t_master values(1,1,1,1,'01','Master01')");
        methodWatcher.executeUpdate("create table t_slave2(id int ,description varchar(20),tm_time timestamp)");

        methodWatcher.executeUpdate(tb.named("Master45").after().update().on("t_master").referencing("NEW AS newt")
                                      .row().then("INSERT INTO t_slave2 VALUES(newt.id,'Master45',CURRENT_TIMESTAMP)")
                                      .build());
        methodWatcher.executeUpdate(tb.named("Master48").after().update().of("col1").on("t_master").referencing("NEW AS newt")
                                      .row().then("INSERT INTO t_slave2 VALUES(newt.id,'Master48',CURRENT_TIMESTAMP)").build());

        // when - update a row
        methodWatcher.executeUpdate("update t_master set id=778, col1=778,col4='778' where id=1");

        assertEquals(2L, methodWatcher.query("select count(*) from t_slave2 where id=778"));
    }

    @Test
    public void simulateMySQLTimestampColumnToPopulateCreatedTimeColumnOnInsert() throws Exception {
        // given - table
        methodWatcher.executeUpdate("create table simulate(a int primary key, b int, createdtime timestamp)");
        // given - trigger
        methodWatcher.executeUpdate(tb.after().insert().on("simulate").row().referencing("NEW AS N")
                .then("update simulate set createdtime=CURRENT_TIMESTAMP where a=N.a").build());

        // when - insert
        methodWatcher.executeUpdate("insert into simulate values(1,10, null)");
        methodWatcher.executeUpdate("insert into simulate values(2,20, null)");

        // then - assert date is approx now
        Timestamp triggerDateTime1 = methodWatcher.query("select createdtime from simulate where a = 1");
        assertTrue(Math.abs(triggerDateTime1.getTime() - System.currentTimeMillis()) < TimeUnit.SECONDS.toMillis(10));

        // then - assert date is approx now
        Timestamp triggerDateTime2 = methodWatcher.query("select createdtime from simulate where a = 2");
        assertTrue(Math.abs(triggerDateTime2.getTime() - System.currentTimeMillis()) < TimeUnit.SECONDS.toMillis(10));
    }

    @Test
    public void recursiveUpdate() throws Exception {
        // DB-3354
        // given - table and rows
        methodWatcher.executeUpdate("create table recursion_test (id int generated by default as identity not null " +
                                        "primary key, num_c INT, str_c VARCHAR(40), bool_c BOOLEAN, time_c TIMESTAMP)");
        methodWatcher.executeUpdate("INSERT INTO recursion_test(num_c, str_c, bool_c, time_c) VALUES (13, 'rt->1', TRUE, CURRENT_TIMESTAMP)");
        methodWatcher.executeUpdate("INSERT INTO recursion_test(num_c, str_c, bool_c, time_c) VALUES (14, 'rt->2', TRUE, CURRENT_TIMESTAMP)");

        // given - trigger
        methodWatcher.executeUpdate(tb.after().insert().on("recursion_test").row().referencing("NEW AS NEW_S")
                                      .then("update recursion_test\n" +
                                                "set num_c=-1 , str_c=NEW_S.str_c, bool_c=NEW_S.bool_c, time_c=NEW_S" +
                                                ".time_c where num_c <= 15").build());

        // when - insert
        methodWatcher.executeUpdate("INSERT INTO recursion_test(num_c, str_c, bool_c, time_c) VALUES (15, 'rt->3', TRUE, CURRENT_TIMESTAMP)");
        methodWatcher.executeUpdate("INSERT INTO recursion_test(num_c, str_c, bool_c, time_c) VALUES (16, 'rt->4', TRUE, CURRENT_TIMESTAMP)");
        methodWatcher.executeUpdate("INSERT INTO recursion_test(num_c, str_c, bool_c, time_c) VALUES (17, 'rt->5', TRUE, CURRENT_TIMESTAMP)");

        // then - assert 2 rows have correct values
        ResultSet resultSet = methodWatcher.executeQuery("select * from recursion_test where NUM_C < 0");
        int count = 0;
        while (resultSet.next()) {
            ++count;
            assertEquals(-1, resultSet.getInt(2));
            String str_c = resultSet.getString(3);
            assertNotNull("Expected non-null STR_C", str_c);
            assertEquals("Expected all STR_C columns to have been updated by trigger to 'rt->5'", "rt->5",str_c);
            Boolean boolVal = resultSet.getBoolean(4);
            assertNotNull("Expected non-null BOOL_C", boolVal);
            assertEquals("Expected BOOL_C to be TRUE", true, boolVal);
            assertNotNull("Expected non-null TIME_C", resultSet.getString(5));
        }
        assertEquals("Expected 3 rows with NUM_C less that zero.", 3, count);
    }
}