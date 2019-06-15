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

package com.splicemachine.derby.impl.sql.execute.actions;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.derby.test.framework.TestConnection;
import com.splicemachine.test.SerialTest;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.spark_project.guava.collect.Lists;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.Statement;
import java.util.Collection;
import java.util.Properties;

import static org.junit.Assert.*;

/**
 * Integration tests specific to CHECK constraints.
 * 
 * @see {@link ConstraintConstantOperationIT} for other constraint tests
 * 
 * @author Walt Koetke
 */
@Category(value = {SerialTest.class})
@RunWith(Parameterized.class)
public class CheckConstraintIT extends SpliceUnitTest {
    public static final String CLASS_NAME = CheckConstraintIT.class.getSimpleName().toUpperCase();
    private static SpliceWatcher classWatcher = new SpliceWatcher(CLASS_NAME);
    private static SpliceSchemaWatcher schemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(classWatcher)
        .around(schemaWatcher);
    
    private SpliceWatcher methodWatcher = new SpliceWatcher(CLASS_NAME);

    private TestConnection conn;
    private String connectionString;

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        Collection<Object[]> params = Lists.newArrayListWithCapacity(2);
        params.add(new Object[]{"jdbc:splice://localhost:1527/splicedb;create=true;user=splice;password=admin"});
        params.add(new Object[]{"jdbc:splice://localhost:1527/splicedb;create=true;user=splice;password=admin;useSpark=true"});
        return params;
    }

    public CheckConstraintIT(String connecitonString) {
        this.connectionString = connecitonString;
    }

    @Before
    public void setUp() throws Exception{
        conn = new TestConnection(DriverManager.getConnection(connectionString, new Properties()));
        conn.setAutoCommit(false);
        conn.setSchema(CLASS_NAME.toUpperCase());
    }

    @After
    public void tearDown() throws Exception{
        conn.rollback();
        conn.reset();
        conn.close();
    }

    @Test
    public void testSingleInserts() throws Exception {
        String tableName = "table1".toUpperCase();
        try(Statement s = conn.createStatement()){
            s.executeUpdate(format("create table %s ( "+
                    "id int not null primary key constraint id_ge_ck1 check (id >= 1000), "+
                    "i_gt int constraint i_gt_ck1 check (i_gt > 100), "+
                    "i_eq int constraint i_eq_ck1 check (i_eq = 90), "+
                    "i_lt int constraint i_lt_ck1 check (i_lt < 80), "+
                    "v_neq varchar(32) constraint v_neq_ck1 check (v_neq <> 'bad'), "+
                    "v_in varchar(32) constraint v_in_ck1 check (v_in in ('good1', "+
                    "'good2', 'good3')))",tableName));
            s.executeUpdate(format("insert into %s values(1000, 200, 90, 79, 'ok', 'good1')",tableName));

            verifyNoViolation(s,format("insert into %s values (1001, 101, 90, 79, 'ok', 'good2')",tableName),1); // all good

            verifyViolation(s,format("insert into %s values (999, 101, 90, 79, 'ok', 'good1')",tableName), // col 1 (PK) bad
                    MSG_START_DEFAULT,"ID_GE_CK1",schemaWatcher.schemaName,tableName);

            verifyViolation(s,format("insert into %s values (1002, 25, 90, 79, 'ok', 'good1')",tableName), // col 2 bad
                    MSG_START_DEFAULT,"I_GT_CK1",schemaWatcher.schemaName,tableName);

            verifyViolation(s,format("insert into %s values (1003, 101, 901, 79, 'ok', 'good1')",tableName), // col 3 bad
                    MSG_START_DEFAULT,"I_EQ_CK1",schemaWatcher.schemaName,tableName);

            verifyViolation(s,format("insert into %s values (1004, 101, 90, 85, 'ok', 'good2')",tableName), // col 4 bad
                    MSG_START_DEFAULT,"I_LT_CK1",schemaWatcher.schemaName,tableName);

            verifyViolation(s,format("insert into %s values (1005, 101, 90, 79, 'bad', 'good3')",tableName), // col 5 bad
                    MSG_START_DEFAULT,"V_NEQ_CK1",schemaWatcher.schemaName,tableName);

            verifyViolation(s,format("insert into %s values (1005, 101, 90, 79, 'ok', 'notsogood')",tableName), // col 6 bad
                    MSG_START_DEFAULT,"V_IN_CK1",schemaWatcher.schemaName,tableName);
        }
    }

    @Test
    public void testSingleInsertsAfterAlterTable() throws Exception {
        String tableName = "table2".toUpperCase();
        try(Statement s= conn.createStatement()){
            s.executeUpdate(format("create table %s ( "+
                    "id int not null primary key, "+
                    "i_gt int, "+
                    "i_eq int, "+
                    "i_lt int, "+
                    "v_neq varchar(32), "+
                    "v_in varchar(32))",tableName));
            s.executeUpdate(format("insert into %s values(1000, 200, 90, 79, 'ok', 'good1')",tableName));

            s.executeUpdate(format("alter table %s add constraint id_ge_ck check (id >= 1000)",tableName));
            s.executeUpdate(format("alter table %s add constraint i_gt_ck check (i_gt > 100)",tableName));
            s.executeUpdate(format("alter table %s add constraint i_eq_ck check (i_eq = 90)",tableName));
            s.executeUpdate(format("alter table %s add constraint i_lt_ck check (i_lt < 80)",tableName));
            s.executeUpdate(format("alter table %s add constraint v_neq_ck check (v_neq <> 'bad')",tableName));
            s.executeUpdate(format("alter table %s add constraint v_in_ck check (v_in in ('good1', 'good2', 'good3'))",tableName));

            verifyNoViolation(s,format("insert into %s values (1001, 101, 90, 79, 'ok', 'good2')",tableName),1); // all good

            verifyViolation(s,format("insert into %s values (999, 101, 90, 79, 'ok', 'good1')",tableName), // col 1 (PK) bad
                    MSG_START_DEFAULT,"ID_GE_CK",schemaWatcher.schemaName,tableName);

            verifyViolation(s,format("insert into %s values (1002, 25, 90, 79, 'ok', 'good1')",tableName), // col 2 bad
                    MSG_START_DEFAULT,"I_GT_CK",schemaWatcher.schemaName,tableName);

            verifyViolation(s,format("insert into %s values (1003, 101, 901, 79, 'ok', 'good1')",tableName), // col 3 bad
                    MSG_START_DEFAULT,"I_EQ_CK",schemaWatcher.schemaName,tableName);

            verifyViolation(s,format("insert into %s values (1004, 101, 90, 85, 'ok', 'good2')",tableName), // col 4 bad
                    MSG_START_DEFAULT,"I_LT_CK",schemaWatcher.schemaName,tableName);

            verifyViolation(s,format("insert into %s values (1005, 101, 90, 79, 'bad', 'good3')",tableName), // col 5 bad
                    MSG_START_DEFAULT,"V_NEQ_CK",schemaWatcher.schemaName,tableName);

            verifyViolation(s,format("insert into %s values (1005, 101, 90, 79, 'ok', 'notsogood')",tableName), // col 6 bad
                    MSG_START_DEFAULT,"V_IN_CK",schemaWatcher.schemaName,tableName);
        }

    }

    @Test
    public void testViolationErrorMsg() throws Exception {
        // DB-3864 - bad error msg
        String tableName = "table3".toUpperCase();
        String tableRef = schemaWatcher.schemaName+"."+tableName;
        try(Statement s = conn.createStatement()){
            s.executeUpdate(format("create table %s (int1_col int not null constraint constr_int1 check"+
                    "(int1_col<5))",tableName));
            try{
                s.executeUpdate(format("insert into %s values(10)",tableName));
                fail("Expected constraint violation");
            }catch(Exception e){
                assertTrue("Expected single ticks around table ref.",e.getLocalizedMessage().contains(format("'%s'",tableRef)));
            }
        }
    }

    @Test
    public void testAlterTableExistingDataViolatesConstraint() throws Exception {
        String tableName = "table4".toUpperCase();
        try(Statement s = conn.createStatement()){
            s.executeUpdate(format("create table %s ( "+
                    "id int not null primary key, "+
                    "i_gt int, "+
                    "i_eq int, "+
                    "i_lt int, "+
                    "v_neq varchar(32), "+
                    "v_in varchar(32))",tableName));
            s.executeUpdate(format("insert into %s values(1000, 200, 90, 79, 'ok', 'good1')",tableName));
            try{
                s.executeUpdate(format("alter table %s add constraint id_ge_ck check (id > 1000)",tableName));
                Assert.fail("Expected add or enable constraint exception");
            }catch(SQLException e){
                Assert.assertEquals("Expected failed attempt to add check constraint.","X0Y59",e.getSQLState());
            }
        }
    }

    @Test
    public void testCreateAlterDropConstraint() throws Exception {
        String tableName = "table5".toUpperCase();
        try(Statement s = conn.createStatement()){
            String constraintName="ten_or_less";

            s.executeUpdate(format("create table %s (col1 int not null)",tableName));
            s.executeUpdate(format("insert into %s values(9)",tableName));
            s.executeUpdate(format("alter table %s add constraint %s check (col1 < 10)",tableName,constraintName));
            try{
                s.executeUpdate(format("insert into %s values (11)",tableName));
                fail("Expected check constraint violation.");
            }catch(SQLException e){
                // expected
                assertEquals("Expected constraint violation","23513",e.getSQLState());
            }
            s.executeUpdate(format("alter table %s drop constraint %s",tableName,constraintName));
            s.executeUpdate(format("insert into %s values (11)",tableName));
        }
    }

    private void verifyNoViolation(Statement s,String query, int count) throws Exception {
        Assert.assertEquals("Invalid row count",count,s.executeUpdate(query));
    }

    protected void verifyViolation(Statement s,String query, String msgStart, Object... args) throws Exception {
        try {
            s.executeUpdate(query);
            fail("Expected check constraint violation.");
        } catch (SQLException e) {
            Assert.assertTrue("Unexpected exception type", e instanceof SQLIntegrityConstraintViolationException);
            String expected = String.format(msgStart, args);
            Assert.assertEquals(e.getLocalizedMessage()+" Expected:\n"+ expected, expected, e.getLocalizedMessage());
        }
    }

    protected static String MSG_START_DEFAULT =
            "The check constraint '%s' was violated while performing an INSERT or UPDATE on table '%s.%s'.";

    // TODO: Additional tests to consider:
    //
    // more data types and operators
    // alter table and create table usage
    // insert multiple rows at a time
    // updates
    // disable/enable
    
}
