/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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
import splice.com.google.common.collect.Lists;

import java.sql.*;
import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

/**
 * Test ROW triggers with transition variables.
 */
@Category(value = {SerialTest.class})
@RunWith(Parameterized.class)
public class Trigger_Row_Transition_IT {

    private static final String SCHEMA = Trigger_Row_Transition_IT.class.getSimpleName();

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

        // TODO enable for spark @Ignore("DB-5474")
         params.add(new Object[]{"jdbc:splice://localhost:1527/splicedb;user=splice;password=admin;useSpark=true"});
        return params;
    }

    private String connectionString;

    public Trigger_Row_Transition_IT(String connecitonString) {
        this.connectionString = connecitonString;
    }

    /* Create tables once */
    @BeforeClass
    public static void createSharedTables() throws Exception {
        Connection conn = classWatcher.getOrCreateConnection();
        try(Statement s = conn.createStatement()){
            s.executeUpdate("create table T (a varchar(9), b int)");
            s.executeUpdate("create table RECORD_OLD (a varchar(9), b int)");
            s.executeUpdate("create table RECORD_NEW (a varchar(9), b int)");
            s.executeUpdate("insert into T values('AAA',1),('BBB',2),('CCC',3)");
        }
    }

    /* Each test starts with same table state */
    @Before
    public void initTable() throws Exception {
        conn = new TestConnection(DriverManager.getConnection(connectionString, new Properties()));
        conn.setSchema(SCHEMA.toUpperCase());
        conn.setAutoCommit(false);
    }

    @After
    public void rollback() throws Exception{
        conn.rollback();
        conn.reset();
    }

    @Test
    public void afterUpdateTriggerUpdatesOwnTable() throws Exception {
        try(Statement s = conn.createStatement()){
            s.executeUpdate(tb.named("trig1").after().update().on("T").referencing("NEW AS N")
                    .row().then("INSERT INTO T VALUES(N.a, N.b)").build());

            // when - update a row
            s.executeUpdate("update T set b = 2000 where a='BBB'");

            // then -- updated row and row from trigger
            Assert.assertEquals(2L,StatementUtils.onlyLong(s,"select count(*) from T where b=2000"));
        }
    }


    @Test
    public void afterDelete() throws Exception {
        try(Statement s = conn.createStatement()){
            s.executeUpdate(tb.named("trig1").after().delete().on("T").referencing("OLD AS O")
                    .row().then("INSERT INTO RECORD_OLD VALUES(O.a, O.b)").build());

            // when - delete a row
            s.executeUpdate("delete from T where b = 2");

            Assert.assertEquals(1L,StatementUtils.onlyLong(s,"select count(*) from RECORD_OLD where a='BBB' and b=2"));
        }
    }

    @Test
    public void afterInsert() throws Exception {
        try(Statement s = conn.createStatement()){
            s.executeUpdate(tb.named("trig1").after().insert().on("T").referencing("NEW AS N")
                    .row().then("INSERT INTO RECORD_NEW VALUES(N.a, N.b)").build());

            // when - insert a row
            s.executeUpdate("insert into T values('DDD', 4),('EEE', 5)");

            Assert.assertEquals(1L,StatementUtils.onlyLong(s,"select count(*) from RECORD_NEW where a='DDD' and b=4"));
            Assert.assertEquals(1L,StatementUtils.onlyLong(s,"select count(*) from RECORD_NEW where a='EEE' and b=5"));
        }
    }

    @Test
    public void beforeUpdate() throws Exception {
        try(Statement s = conn.createStatement()){
            s.executeUpdate(tb.named("trig1").before().update().on("T").referencing("OLD AS O")
                    .row().then("select O.b/0 from sys.systables").build());

            // when - update a row
            try{
                s.executeUpdate("update T set b = 2000, a='ZZZ' where a='BBB'");
                fail("expected trigger to cause exception");
            }catch(SQLException e){
                assertTrue(e.getMessage().contains("divide by zero"));
            }
        }
    }

    @Test
    public void afterUpdate() throws Exception {
        try(Statement s = conn.createStatement()){
            s.executeUpdate(tb.named("trig1").after().update().on("T").referencing("OLD AS O")
                    .row().then("INSERT INTO RECORD_OLD VALUES(O.a, O.b)").build());

            // when - update a row
            s.executeUpdate("update T set b = 2000, a='ZZZ' where a='BBB'");

            Assert.assertEquals(1L,StatementUtils.onlyLong(s,"select count(*) from RECORD_OLD where a='BBB' and b=2"));
        }
    }

    @Test
    public void afterUpdateTransitionNew() throws Exception {
        // DB-3570: transition values - always seeing old
        try(Statement s = conn.createStatement()){
            s.executeUpdate(tb.named("trig4").after().update().on("T").referencing("NEW AS N")
                    .row().then("INSERT INTO RECORD_NEW VALUES(N.a, N.b)").build());

            // when - update a row
            s.executeUpdate("update T set a='new', b=999 where a='CCC'");

            Assert.assertEquals(1L,StatementUtils.onlyLong(s,"select count(*) from RECORD_NEW where a='new' and b=999"));
        }
    }

    @Test
    public void afterUpdateTransitionNewTwoTriggers() throws Exception {
        // DB-3570: transition values - always seeing old (event being cleared prematurely)
        try(Statement s = conn.createStatement()){
            s.executeUpdate("create table t_master(id int, col1 int, col2 int, col3 int, col4 char(10), col5 varchar(20))");
            s.executeUpdate("insert into t_master values(1,1,1,1,'01','Master01')");
            s.executeUpdate("create table t_slave2(id int ,description varchar(20),tm_time timestamp)");

            s.executeUpdate(tb.named("Master45").after().update().on("t_master").referencing("NEW AS newt")
                    .row().then("INSERT INTO t_slave2 VALUES(newt.id,'Master45',CURRENT_TIMESTAMP)")
                    .build());
            s.executeUpdate(tb.named("Master48").after().update().of("col1").on("t_master").referencing("NEW AS newt")
                    .row().then("INSERT INTO t_slave2 VALUES(newt.id,'Master48',CURRENT_TIMESTAMP)").build());

            // when - update a row
            s.executeUpdate("update t_master set id=778, col1=778,col4='778' where id=1");

            Assert.assertEquals(2L,StatementUtils.onlyLong(s,"select count(*) from t_slave2 where id=778"));
        }
    }

    @Test
    public void simulateMySQLTimestampColumnToPopulateCreatedTimeColumnOnInsert() throws Exception {
        // given - table
        try(Statement s = conn.createStatement()){
            s.executeUpdate("create table simulate(a int primary key, b int, createdtime timestamp)");
            // given - trigger
            s.executeUpdate(tb.after().insert().on("simulate").row().referencing("NEW AS N")
                    .then("update simulate set createdtime=CURRENT_TIMESTAMP where a=N.a").build());

            // when - insert
            s.executeUpdate("insert into simulate values(1,10, null)");
            s.executeUpdate("insert into simulate values(2,20, null)");

            // then - assert date is approx now
            Timestamp triggerDateTime1=StatementUtils.onlyTimestamp(s,"select createdtime from simulate where a = 1");
            assertTrue(Math.abs(triggerDateTime1.getTime()-System.currentTimeMillis())<TimeUnit.SECONDS.toMillis(10));

            // then - assert date is approx now
            Timestamp triggerDateTime2=StatementUtils.onlyTimestamp(s,"select createdtime from simulate where a = 2");
            assertTrue(Math.abs(triggerDateTime2.getTime()-System.currentTimeMillis())<TimeUnit.SECONDS.toMillis(10));
        }
    }


    @Test
    public void recursiveUpdate() throws Exception {
        // DB-3354
        // given - table and rows
        try(Statement s = conn.createStatement()){
            s.executeUpdate("create table recursion_test (id int generated by default as identity not null "+
                    "primary key, num_c INT, str_c VARCHAR(40), bool_c BOOLEAN, time_c TIMESTAMP)");
            s.executeUpdate("INSERT INTO recursion_test(num_c, str_c, bool_c, time_c) VALUES (13, 'rt->1', TRUE, CURRENT_TIMESTAMP)");
            s.executeUpdate("INSERT INTO recursion_test(num_c, str_c, bool_c, time_c) VALUES (14, 'rt->2', TRUE, CURRENT_TIMESTAMP)");

            // given - trigger
            s.executeUpdate(tb.after().insert().on("recursion_test").row().referencing("NEW AS NEW_S")
                    .then("update recursion_test\n"+
                            "set num_c=-1 , str_c=NEW_S.str_c, bool_c=NEW_S.bool_c, time_c=NEW_S"+
                            ".time_c where num_c <= 15").build());

            // when - insert
            s.executeUpdate("INSERT INTO recursion_test(num_c, str_c, bool_c, time_c) VALUES (15, 'rt->3', TRUE, CURRENT_TIMESTAMP)");
            s.executeUpdate("INSERT INTO recursion_test(num_c, str_c, bool_c, time_c) VALUES (16, 'rt->4', TRUE, CURRENT_TIMESTAMP)");
            s.executeUpdate("INSERT INTO recursion_test(num_c, str_c, bool_c, time_c) VALUES (17, 'rt->5', TRUE, CURRENT_TIMESTAMP)");

            // then - assert 2 rows have correct values
            try(ResultSet resultSet=s.executeQuery("select * from recursion_test where NUM_C < 0")){
                int count=0;
                while(resultSet.next()){
                    ++count;
                    assertEquals(-1,resultSet.getInt(2));
                    String str_c=resultSet.getString(3);
                    assertNotNull("Expected non-null STR_C",str_c);
                    assertEquals("Expected all STR_C columns to have been updated by trigger to 'rt->5'","rt->5",str_c);
                    boolean boolVal=resultSet.getBoolean(4);
                    Assert.assertFalse("Expected non-null BOOL_C",resultSet.wasNull());
                    assertEquals("Expected BOOL_C to be TRUE",true,boolVal);
                    assertNotNull("Expected non-null TIME_C",resultSet.getString(5));
                }
                assertEquals("Expected 3 rows with NUM_C less that zero.",3,count);
            }
        }
    }
}
