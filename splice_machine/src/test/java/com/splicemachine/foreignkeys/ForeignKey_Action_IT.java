/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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

package com.splicemachine.foreignkeys;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.derby.test.framework.TestConnection;
import com.splicemachine.test.SerialTest;
import com.splicemachine.test_dao.TableDAO;
import com.splicemachine.util.StatementUtils;
import org.junit.*;
import org.junit.experimental.categories.Category;

import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.Statement;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Foreign key tests for referential actions:
 *
 * ON DELETE NO ACTION
 * ON DELETE CASCADE
 * ON DELETE SET NULL
 * ON UPDATE NO ACTION
 */
//
// SPLICE-894 Remove Serial
@Category(value = {SerialTest.class})
public class ForeignKey_Action_IT {

    private static final String SCHEMA = ForeignKey_Action_IT.class.getSimpleName();

    @ClassRule
    public static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(SCHEMA);

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(SCHEMA);

    private TestConnection conn;
    @Before
    public void deleteTables() throws Exception {
        conn = methodWatcher.getOrCreateConnection();
        conn.setAutoCommit(false);
        new TableDAO(conn).drop(SCHEMA, "C", "P");
    }

    @After
    public void tearDown() throws Exception{
        conn.rollback();
        conn.reset();
    }

    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    //
    // fk references unique index
    //
    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    @Test
    public void onDeleteNoAction() throws Exception {
        try(Statement s = conn.createStatement()){
            s.executeUpdate("create table P (a int unique, b int)");
            s.executeUpdate("create table C (a int, b int, CONSTRAINT FK_1 FOREIGN KEY (a) REFERENCES P(a) ON DELETE NO ACTION)");
            s.executeUpdate("insert into P values(1,10),(2,20),(3,30)");
            s.executeUpdate("insert into C values(1,10),(1,15),(2,20),(2,20),(3,30),(3,35)");

            assertQueryFail(s,"delete from P where a = 2","Operation on table 'P' caused a violation of foreign key constraint 'FK_1' for key (A).  The statement has been rolled back.");
            assertQueryFail(s,"update P set a=-1 where a = 2","Operation on table 'P' caused a violation of foreign key constraint 'FK_1' for key (A).  The statement has been rolled back.");
        }
    }

    @Test
    public void onDeleteNoActionImplicit() throws Exception {
        try(Statement s = conn.createStatement()){
            s.executeUpdate("create table P (a int unique, b int)");
            s.executeUpdate("create table C (a int, b int, CONSTRAINT FK_1 FOREIGN KEY (a) REFERENCES P(a))");
            s.executeUpdate("insert into P values(1,10),(2,20),(3,30)");
            s.executeUpdate("insert into C values(1,10),(1,15),(2,20),(2,20),(3,30),(3,35)");

            assertQueryFail(s,"delete from P where a = 2","Operation on table 'P' caused a violation of foreign key constraint 'FK_1' for key (A).  The statement has been rolled back.");
            assertQueryFail(s,"update P set a=-1 where a = 2","Operation on table 'P' caused a violation of foreign key constraint 'FK_1' for key (A).  The statement has been rolled back.");
        }
    }

    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    //
    // fk references primary key
    //
    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    @Test
    public void onDeleteNoAction_primaryKey() throws Exception {
        try(Statement s = conn.createStatement()){
            s.executeUpdate("create table P (a int primary key, b int)");
            s.executeUpdate("create table C (a int, b int, CONSTRAINT FK_1 FOREIGN KEY (a) REFERENCES P(a) ON DELETE NO ACTION)");
            s.executeUpdate("insert into P values(1,10),(2,20),(3,30)");
            s.executeUpdate("insert into C values(1,10),(1,15),(2,20),(2,20),(3,30),(3,35)");

            assertQueryFail(s,"delete from P where a = 2","Operation on table 'P' caused a violation of foreign key constraint 'FK_1' for key (A).  The statement has been rolled back.");
            assertQueryFail(s,"update P set a=-1 where a = 2","Operation on table 'P' caused a violation of foreign key constraint 'FK_1' for key (A).  The statement has been rolled back.");
        }
    }

    /* Make sure FKs still work when we create the parent, write to it first, then create the child that actually has the FK */
    @Test
    public void onDeleteNoAction_primaryKey_initializeWriteContextOfParentFirst() throws Exception {
        try(Statement s = conn.createStatement()){
            s.executeUpdate("create table P (a int primary key, b int unique)");
            s.executeUpdate("insert into P values(1,10),(2,20),(3,30),(4,40)");

            s.executeUpdate("create table C1 (a int, b int, CONSTRAINT FK_1 FOREIGN KEY (a) REFERENCES P(a))");
            s.executeUpdate("insert into C1 values(1,10),(1,15),(2,20),(2,20),(3,30),(3,35)");

            assertQueryFail(s,"delete from P where a = 2","Operation on table 'P' caused a violation of foreign key constraint 'FK_1' for key (A).  The statement has been rolled back.");
            assertQueryFail(s,"update P set a=-1 where a = 2","Operation on table 'P' caused a violation of foreign key constraint 'FK_1' for key (A).  The statement has been rolled back.");

            s.executeUpdate("create table C2 (a int, b int, CONSTRAINT FK_2 FOREIGN KEY (b) REFERENCES P(b))");
            s.executeUpdate("insert into C2 values(4,40)");

            // verify NEW FK constraint works
            assertQueryFail(s,"delete from P where a = 4","Operation on table 'P' caused a violation of foreign key constraint 'FK_2' for key (B).  The statement has been rolled back.");
            assertQueryFail(s,"update P set b=-1 where a = 4","Operation on table 'P' caused a violation of foreign key constraint 'FK_2' for key (B).  The statement has been rolled back.");

            // verify FIRST FK constraint STILL works
            assertQueryFail(s,"delete from P where a = 1","Operation on table 'P' caused a violation of foreign key constraint 'FK_1' for key (A).  The statement has been rolled back.");
            assertQueryFail(s,"update P set a=-1 where a = 1","Operation on table 'P' caused a violation of foreign key constraint 'FK_1' for key (A).  The statement has been rolled back.");
        }
    }

    @Test
    public void onDeleteNoAction_primaryKey_successAfterDeleteReference() throws Exception {
        try(Statement s = conn.createStatement()){
            s.executeUpdate("create table P (a int primary key, b int)");
            s.executeUpdate("create table C (a int, b int, CONSTRAINT FK_1 FOREIGN KEY (a) REFERENCES P(a) ON DELETE NO ACTION)");
            s.executeUpdate("insert into P values(1,10),(2,20),(3,30)");
            s.executeUpdate("insert into C values(1,10),(1,15),(2,20),(2,20),(3,30),(3,35)");

            assertQueryFail(s,"delete from P where a = 2","Operation on table 'P' caused a violation of foreign key constraint 'FK_1' for key (A).  The statement has been rolled back.");
            assertQueryFail(s,"update P set a=-1 where a = 2","Operation on table 'P' caused a violation of foreign key constraint 'FK_1' for key (A).  The statement has been rolled back.");

            // delete references
            s.executeUpdate("delete from C where a=2");

            // now delete from parent should succeed
            assertEquals(4L,StatementUtils.onlyLong(s,"select count(*) from C"));
            assertEquals(1L,s.executeUpdate("delete from P where a = 2"));
        }
    }

    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    //
    // helper methods
    //
    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    private void assertQueryFail(Statement s,String sql, String expectedExceptionMessage) {
        try{
            s.executeUpdate(sql);
            fail("expected query to fail: " + sql);
        } catch (Exception e) {
            assertEquals(expectedExceptionMessage, e.getMessage());
            assertEquals(SQLIntegrityConstraintViolationException.class, e.getClass());
        }
    }

}