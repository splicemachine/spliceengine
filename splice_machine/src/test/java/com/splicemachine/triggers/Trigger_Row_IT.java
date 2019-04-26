/*
 * Copyright (c) 2012 - 2019 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.triggers;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.derby.test.framework.TestConnection;
import com.splicemachine.test.SerialTest;
import com.splicemachine.test_dao.TriggerBuilder;
import com.splicemachine.util.StatementUtils;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.spark_project.guava.collect.Lists;

import java.sql.*;
import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static java.lang.System.currentTimeMillis;
import static org.junit.Assert.*;

/**
 * Test ROW triggers.
 */
@Category(value = {SerialTest.class})
@RunWith(Parameterized.class)
public class Trigger_Row_IT {

    private static final String SCHEMA = Trigger_Row_IT.class.getSimpleName();

    @ClassRule
    public static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(SCHEMA);

    @ClassRule
    public static SpliceWatcher classWatcher = new SpliceWatcher(SCHEMA);

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(SCHEMA);

    private TriggerBuilder tb = new TriggerBuilder();

    private TestConnection conn;


    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        Collection<Object[]> params = Lists.newArrayListWithCapacity(2);
        params.add(new Object[]{"jdbc:splice://localhost:1527/splicedb;user=splice;password=admin"});
        params.add(new Object[]{"jdbc:splice://localhost:1527/splicedb;user=splice;password=admin;useSpark=true"});
        return params;
    }

    private String connectionString;

    public Trigger_Row_IT(String connecitonString) {
        this.connectionString = connecitonString;
    }

    /* Create tables once */
    @BeforeClass
    public static void createSharedTables() throws Exception {
        Connection c = classWatcher.createConnection();
        try(Statement s = c.createStatement()){
            s.executeUpdate("create table T (a int, b int, c int)");
            s.executeUpdate("create table RECORD (txt varchar(99))");
            s.executeUpdate("insert into T values(1,1,1),(2,2,2),(3,3,3),(4,4,4),(5,5,5),(6,6,6)");
        }
    }

    /* Each test starts with same table state */
    @Before
    public void initTable() throws Exception {
        conn = new TestConnection(DriverManager.getConnection(connectionString, new Properties()));
        conn.setAutoCommit(false);
        conn.setSchema(SCHEMA.toUpperCase());
    }

    @After
    public void rollback() throws Exception{
        conn.rollback();
        conn.reset();
    }

    //- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    //
    // AFTER triggers
    //
    //- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    @Test
    public void afterUpdate() throws Exception {
        createTrigger(tb.after().update().on("T").row().then("INSERT INTO RECORD VALUES('after-update')"));

        try(Statement s = conn.createStatement()){
            // when - update all rows
            long updateCount1=s.executeUpdate("update T set c = c + 1");
            assertRecordCount(s,"after-update",updateCount1);

            // when -- update one row
            long updateCount2=s.executeUpdate("update T set c = c + 1 where a = 1");
            assertRecordCount(s,"after-update",updateCount1+updateCount2);

            // when -- update two rows
            long updateCount3=s.executeUpdate("update T set c = c + 1 where a = 1 or a = 2");
            assertRecordCount(s,"after-update",updateCount1+updateCount2+updateCount3);

            assertNonZero(updateCount1,updateCount2,updateCount3);

            // when -- update ZERO rows
            long updateCount4=s.executeUpdate("update T set c = c + 1 where a = -9999999");
            assertRecordCount(s,"after-update",updateCount1+updateCount2+updateCount3);
            assertEquals(0,updateCount4);
        }
    }

    /* Trigger on subset of columns */
    @Test
    public void afterUpdateOfColumns() throws Exception {
        createTrigger(tb.after().update().of("a,c").on("T").row().then("INSERT INTO RECORD VALUES('after-update')"));

        try(Statement s = conn.createStatement()){
            // when - update non trigger col
            s.executeUpdate("update T set b = 100");
            assertRecordCount(s,"after-update",0);

            // when -- update trigger col 1
            s.executeUpdate("update T set a = 100");
            assertRecordCount(s,"after-update",6);

            // when -- update trigger col 2
            s.executeUpdate("update T set c = 100");
            assertRecordCount(s,"after-update",12);
        }
    }

    @Test
    public void afterInsert() throws Exception {
        createTrigger(tb.after().insert().on("T").row().then("INSERT INTO RECORD VALUES('after-insert')"));

        try(Statement s = conn.createStatement()){
            // when - insert over select
            long insertCount1=s.executeUpdate("insert into T select * from T");
            assertRecordCount(s,"after-insert",insertCount1);

            // when -- insert over values
            long insertCount2=s.executeUpdate("insert into T values(8,8,8),(9,9,9)");
            assertRecordCount(s,"after-insert",insertCount1+insertCount2);

            // when -- insert over more values
            long insertCount3=s.executeUpdate("insert into T values(8,8,8),(9,9,9),(10,10,10)");
            assertRecordCount(s,"after-insert",insertCount1+insertCount2+insertCount3);

            assertNonZero(insertCount1,insertCount2,insertCount3);
        }
    }

    @Test
    public void afterDelete() throws Exception {
        createTrigger(tb.after().delete().on("T").row().then("INSERT INTO RECORD VALUES('after-delete')"));

        try(Statement s = conn.createStatement()){
            // when - delete
            long deleteCount1=s.executeUpdate("delete from T where a >=4");
            assertRecordCount(s,"after-delete",deleteCount1);

            // when -- delete all
            long deleteCount2=s.executeUpdate("delete from T");
            assertRecordCount(s,"after-delete",deleteCount1+deleteCount2);

            assertNonZero(deleteCount1,deleteCount2);

            // when -- delete ZERO rows
            long deleteCount3=s.executeUpdate("delete from T where a = -9999999");
            assertRecordCount(s,"after-delete",deleteCount1+deleteCount2);
            assertEquals(0,deleteCount3);
        }
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
        try(Statement s = conn.createStatement()){
            s.executeUpdate("insert into RECORD values('aaa')");

            createTrigger(tb.after().insert().on("T").row().then("insert into RECORD select * from RECORD"));

            // when - insert over select
            s.executeUpdate("insert into T values(1,1,1)");
            assertRecordCount(s,"aaa",2);
            s.executeUpdate("insert into T values(8,8,8)");
            assertRecordCount(s,"aaa",4);
            s.executeUpdate("insert into T values(8,8,8)");
            assertRecordCount(s,"aaa",8);
            s.executeUpdate("insert into T values(8,8,8)");
            assertRecordCount(s,"aaa",16);
        }
    }

    @Test
    public void afterUpdate_sinkingTriggerAction() throws Exception {
        try(Statement s = conn.createStatement()){
            s.executeUpdate("insert into RECORD values('aaa')");

            createTrigger(tb.after().update().on("T").row().then("insert into RECORD select * from RECORD"));

            // when - update all rows
            s.executeUpdate("update T set c = c + 1");
            // this would be 2^6 if our trigger actions executed sequentially
            assertRecordCount(s,"aaa",64);
        }
    }

    /* DB-3391: trigger would fail to fire if trigger statement was one-row delete/update on PK or unique index */
    @Test
    public void afterDelete_deleteTriggerAction() throws Exception {
        try(Statement s = conn.createStatement()){
            // given -- a row in the RECORD table, and a trigger that should delete it
            s.executeUpdate("create table T3391 (a bigint primary key, b bigint unique, c bigint)");
            s.executeUpdate("insert into T3391 values(1,1,1),(2,2,2)");
            s.executeUpdate("insert into RECORD values('aaa'), ('bbb')");
            createTrigger(tb.named("t3391a").after().delete().on("T3391").row().then("delete from RECORD where txt='aaa'"));
            createTrigger(tb.named("t3391b").after().delete().on("T3391").row().then("delete from RECORD where txt='bbb'"));
            assertRecordCount(s,"aaa",1);
            assertRecordCount(s,"bbb",1);

            // when - that trigger is fired
            s.executeUpdate("delete from T3391 where a = 1");
            s.executeUpdate("delete from T3391 where b = 2");

            assertRecordCount(s,"bbb",0);
            assertRecordCount(s,"aaa",0);
        }
    }

    // DB-3354: Nested triggers produce null transition variables.
    @Test
    public void afterInsertCascadingTriggers() throws Exception {
        try(Statement s = conn.createStatement()){
            s.executeUpdate("create table cascade1 (a int)");
            s.executeUpdate("create table cascade2 (a int, name varchar(20), t timestamp)");
            s.executeUpdate("create table cascade3 (a int, name varchar(20), t timestamp)");

            createTrigger(tb.named("cascade1_insert").after().insert().on("cascade1").referencing("NEW as NEW").row().
                    then("insert into cascade2(a, name, t) values (NEW.a, 'cascade1_insert', CURRENT_TIMESTAMP)"));
            createTrigger(tb.named("cascade2_insert").after().insert().on("cascade2").referencing("NEW as NEW").row().
                    then("insert into cascade3(a, name, t) values (NEW.a, 'cascade2_insert', CURRENT_TIMESTAMP)"));

            // when -- fire the trigger
            s.executeUpdate("insert into cascade1 (a) values (1)");

            // then -- verify content of table 1
            Assert.assertEquals(1,StatementUtils.onlyInt(s,"select a from cascade1"));

            // then -- verify content of table 2
            Assert.assertEquals(1,StatementUtils.onlyInt(s,"select a from cascade2"));
            Assert.assertEquals("cascade1_insert",StatementUtils.onlyString(s,"select name from cascade2"));
            Timestamp ts2=StatementUtils.onlyTimestamp(s,"select t from cascade2");

            // then -- verify content of table 3
            Assert.assertEquals(1,StatementUtils.onlyInt(s,"select a from cascade3"));
            Assert.assertEquals("cascade2_insert",StatementUtils.onlyString(s,"select name from cascade3"));
            Timestamp ts3=StatementUtils.onlyTimestamp(s,"select t from cascade3");

            assertTrue(ts2.before(ts3));
            final long timeDelta = TimeUnit.SECONDS.toMillis(15); // enough for a GC to get in between
            assertTrue("ts should be close to current time",Math.abs(currentTimeMillis()-ts2.getTime())<timeDelta);
            assertTrue("ts should be close to current time",Math.abs(currentTimeMillis()-ts3.getTime())<timeDelta);
        }
    }


    //- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    //
    // BEFORE row triggers -- cannot update/insert, so we verify that they fire with trigger action that throws.
    //
    //- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    @Test
    public void beforeUpdate() throws Exception {
        createTrigger(tb.before().update().on("T").row().then("select 1/0 from sys.systables"));
        try(Statement s = conn.createStatement()){
            assertQueryFails(s,"update T set b = b * 2 where a <= 4","Attempt to divide by zero.");
        }
    }

    @Test
    public void beforeInsert() throws Exception {
        createTrigger(tb.before().insert().on("T").row().then("select 1/0 from sys.systables"));
        try(Statement s = conn.createStatement()){
            assertQueryFails(s,"insert into T values(8,8,8)","Attempt to divide by zero.");
        }
    }

    @Test
    public void beforeDelete() throws Exception {
        createTrigger(tb.before().delete().on("T").row().then("select 1/0 from sys.systables"));
        try(Statement s = conn.createStatement()){
            assertQueryFails(s,"delete from T where c = 6","Attempt to divide by zero.");
        }
    }

    //- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    //
    // Triggers and Constraint violations
    //
    //- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    @Test
    public void afterInsertWithUniqueConstraintViolation() throws Exception {
        try(Statement s = conn.createStatement()){
            s.executeUpdate("create table T2(a int, constraint T_INDEX1 unique(a))");
            s.executeUpdate("insert into T2 values(1),(2),(3)");

        /* When the triggering statement is rolled back because of a constraint violation we can't use the effect of
         * the trigger to test if it fired when that effect is also rolled back.  The SET_GLOBAL_DATABASE_PROPERTY
         * stored procedure is non transactional.
         */
            createTrigger(tb.named("constraintTrig1").after().insert().on("T2").row()
                    .then("call syscs_util.SYSCS_SET_GLOBAL_DATABASE_PROPERTY('triggerRowItPropA', 'someValue')"));

            // when - insert over select
            assertQueryFails(s,"insert into T2 select * from T2","The statement was aborted because it would have caused a duplicate key value in a unique or primary key constraint or unique index identified by 'T_INDEX1' defined on 'T2'.");
            // when - insert over values
            assertQueryFails(s,"insert into T2 values(1)","The statement was aborted because it would have caused a duplicate key value in a unique or primary key constraint or unique index identified by 'T_INDEX1' defined on 'T2'.");

            // Trigger should NOT have fired.
            Connection connection=methodWatcher.createConnection();
            try(CallableStatement cs=connection.prepareCall("call syscs_util.SYSCS_GET_GLOBAL_DATABASE_PROPERTY('triggerRowItPropA')")){
                try(ResultSet rs=cs.executeQuery()){
                    Assert.assertTrue("No rows returned!",rs.next());
                    assertNull(rs.getString(2));
                    Assert.assertTrue("Did not return null!",rs.wasNull());
                }
            }
        }
    }

    @Test
    public void afterUpdateWithUniqueConstraintViolation() throws Exception {
        try(Statement s = conn.createStatement()){
            s.executeUpdate("create table T3(a int, constraint T_INDEX2 unique(a))");
            s.executeUpdate("insert into T3 values(1),(2),(3)");

            createTrigger(tb.named("constraintTrig2").after().update().on("T3").row()
                    .then("call syscs_util.SYSCS_SET_GLOBAL_DATABASE_PROPERTY('triggerRowItPropB', 'someValue')"));

            // when - update
            assertQueryFails(s,"update T3 set a=1 where a=3","The statement was aborted because it would have caused a duplicate key value in a unique or primary key constraint or unique index identified by 'T_INDEX2' defined on 'T3'.");

            // Trigger should NOT have fired.
            Connection connection=methodWatcher.createConnection();
            try(CallableStatement call=connection.prepareCall("call syscs_util.SYSCS_GET_GLOBAL_DATABASE_PROPERTY('triggerRowItPropB')")){
                try(ResultSet rs=call.executeQuery()){
                    Assert.assertTrue("No rows returned!",rs.next());
                    assertNull(rs.getString(2));
                    Assert.assertTrue("Did not return null!",rs.wasNull());
                }
            }
        }
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
        try(Statement s = conn.createStatement()){
            s.executeUpdate("update T set c = 0 where c=1 or c=2 or c=3");
            assertRecordCount(s,"u1",3);
            assertRecordCount(s,"u2",3);
            assertRecordCount(s,"statement-1",1);
            // when - insert
            s.executeUpdate("insert into T values(7,7,7),(8,8,8),(9,9,9),(10,10,10)");
            assertRecordCount(s,"i1",4);
            assertRecordCount(s,"i2",4);
            assertRecordCount(s,"statement-2",1);
            // when - delete
            s.executeUpdate("delete from T where c=4 or c=5 or c=6 or c=7 or c=8");
            assertRecordCount(s,"d1",5);
            assertRecordCount(s,"d2",5);
            assertRecordCount(s,"statement-3",1);
        }
    }

    @Test
    public void oldPreparedStatementsFireNewRowTriggers() throws Exception {
        // given - create a prepared statement and execute it so we are sure that it is compiled
        try(PreparedStatement ps=conn.prepareStatement("update T set a = a + 1");
            Statement s=conn.createStatement()){
            assertEquals(6,ps.executeUpdate());

            // given - define the trigger after ps is created/compiled
            createTrigger(tb.named("u_1").after().update().on("T").row().then("INSERT INTO RECORD VALUES('u1')"));

            // when - execute ps again
            assertEquals(6,ps.executeUpdate());

            // then - trigger still fires
            assertRecordCount(s,"u1",6);
        }
    }

    @Test
    public void testCascadeConstraintViolation() throws Exception {

        conn.execute("create table a(i varchar(40) unique)");
        conn.execute("insert into a values 'One', 'Two', 'Three'");
        conn.execute("create table cas1 (str_f varchar(40), int_f int)");
        conn.execute("insert into cas1(str_f, int_f) values ('One', 1)");

        conn.execute("create table cas2(str_f varchar(40), int_f int)");
        conn.execute("insert into cas2(str_f, int_f) values ('Two', 2)");

        conn.execute("create table cas3 (str_f varchar(40) constraint test_rollback references a(i), int_f int)");
        conn.execute("insert into cas3(str_f, int_f) values ('Three', 3)");

        conn.execute("create trigger cascade1_update after update on cas1 referencing NEW as NEW for each row update cas2 set str_f=NEW.str_f, int_f=NEW.int_f where str_f='Two'");

        conn.execute("create trigger cascade2_update after update on cas2 referencing NEW as NEW for each row update cas3 set str_f=NEW.str_f, int_f=NEW.int_f where str_f='Three'");

        try {
            conn.execute("update cas1 set str_f='Incorrect', int_f=-1 where str_f = 'One'");
            fail("Expected to fail with a SQLIntegrityConstraintViolationException");
        }
        catch (Exception e) {
            String message = e.getLocalizedMessage();
            Assert.assertTrue(message.compareTo("Operation on table 'CAS3' caused a violation of foreign key constraint 'TEST_ROLLBACK' for key (STR_F).  The statement has been rolled back.") == 0);
        }
    }

    @Test
    public void testUpdateColumnTrigger() throws Exception {
        conn.execute("create table tu(c1 int, c2 varchar(10), c3 bigint)");
        conn.execute("insert into tu values (1, 'varchar', 123456)");
        conn.execute("create table tr2(c2 varchar(10))");
        conn.execute("create table tr3(c3 bigint)");
        conn.execute("CREATE TRIGGER trigger2\n" +
                "after update of c1 on tu\n" +
                "referencing new as N\n" +
                "for each row\n" +
                "insert into tr2 values (N.c2)");
        conn.execute("CREATE TRIGGER trigger3\n" +
                "after update of c1 on tu\n" +
                "referencing new as N\n" +
                "for each row\n" +
                "insert into tr3 values (N.c3)");
        conn.execute("update tu set c1=c1+1");

        ResultSet rs = conn.query("select * from tr2");
        rs.next();
        Assert.assertEquals("varchar", rs.getString(1));

        rs = conn.query("select * from tr3");
        rs.next();
        Assert.assertEquals(123456, rs.getLong(1));

    }

    //- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    //
    //- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    private void assertRecordCount(Statement s,String tag,long expectedCount) throws Exception {
        long actualCount = StatementUtils.onlyLong(s, "select count(*) from RECORD where txt='" + tag + "'");
        assertEquals("Didn't find expected number of rows:", expectedCount, actualCount);
    }


    private void assertQueryFails(Statement s, String query, String expectedError) {
        try {
            s.executeUpdate(query);
            fail("expected to fail with message = " + expectedError);
        } catch (Exception e) {
            assertTrue(e.getMessage().contains(expectedError));
        }
    }

    private void assertNonZero(long... values) {
        for (long i : values) {
            assertTrue(i > 0);
        }
    }

    private void createTrigger(TriggerBuilder tb) throws Exception {
        try(Statement s = conn.createStatement()){
            s.executeUpdate(tb.build());
        }
    }

}
