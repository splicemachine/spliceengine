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

package com.splicemachine.foreignkeys;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.derby.test.framework.TestConnection;
import com.splicemachine.test_dao.TableDAO;
import com.splicemachine.util.StatementUtils;
import org.junit.*;

import java.sql.ResultSet;
import java.sql.SQLException;
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
public class ForeignKeyActionIT {

    private static final String SCHEMA = ForeignKeyActionIT.class.getSimpleName();

    @ClassRule
    public static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(SCHEMA);

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(SCHEMA);

    private TestConnection conn;
    @Before
    public void deleteTables() throws Exception {
        conn = methodWatcher.getOrCreateConnection();
        conn.setAutoCommit(false);
        new TableDAO(conn).drop(SCHEMA, "SRT", "LC", "YAC", "AC", "AP", "C", "P");
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

    private static void createDatabaseObjects1(Statement s) throws SQLException {
        s.executeUpdate("create table P (a int unique, b int)");
        s.executeUpdate("create table C (a int, b int, CONSTRAINT FK_1 FOREIGN KEY (a) REFERENCES P(a) ON DELETE NO ACTION)");
        s.executeUpdate("insert into P values(1,10),(2,20),(3,30)");
        s.executeUpdate("insert into C values(1,10),(1,15),(2,20),(2,20),(3,30),(3,35)");
    }

    @Test
    public void onDeleteNoAction() throws Exception {
        try(Statement s = conn.createStatement()){
            createDatabaseObjects1(s);
            assertQueryFail(s,"delete from P where a = 2","Operation on table 'P' caused a violation of foreign key constraint 'FK_1' for key (A).  The statement has been rolled back.");
            assertQueryFail(s,"update P set a=-1 where a = 2","Operation on table 'P' caused a violation of foreign key constraint 'FK_1' for key (A).  The statement has been rolled back.");
        }
    }

    @Test
    public void onDeleteNoActionImplicit() throws Exception {
        try(Statement s = conn.createStatement()){
            createDatabaseObjects1(s);

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
    public void onDeleteNoActionPrimaryKey() throws Exception {
        try(Statement s = conn.createStatement()){
            createDatabaseObjects1(s);

            assertQueryFail(s,"delete from P where a = 2","Operation on table 'P' caused a violation of foreign key constraint 'FK_1' for key (A).  The statement has been rolled back.");
            assertQueryFail(s,"update P set a=-1 where a = 2","Operation on table 'P' caused a violation of foreign key constraint 'FK_1' for key (A).  The statement has been rolled back.");
        }
    }

    /* Make sure FKs still work when we create the parent, write to it first, then create the child that actually has the FK */
    @Test
    public void onDeleteNoActionPrimaryKeyInitializeWriteContextOfParentFirst() throws Exception {
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
    public void onDeleteNoActionPrimaryKeySuccessAfterDeleteReference() throws Exception {
        try(Statement s = conn.createStatement()){
            createDatabaseObjects1(s);

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
    // FK - ON DELETE SET NULL
    //
    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    private static void createDatabaseObjects2(Statement s, boolean withSecondChild, boolean withThridChildNoOnDeleteSetNull) throws SQLException {
        s.executeUpdate("create table AP(col1 int, col2 varchar(2), col3 int, col4 int, primary key (col2, col4))");
        s.executeUpdate("insert into AP values (1, 'a', 1, 1)");
        s.executeUpdate("insert into AP values (2, 'b', 2, 2)");
        s.executeUpdate("insert into AP values (3, 'c', 3, 3)");

        s.executeUpdate("create table AC(col1 int, col2 varchar(2), col3 int, constraint fkey_1 foreign key(col2, col3) references AP(col2, col4) on delete set null)");
        s.executeUpdate("insert into AC values (1, 'b', 2)");
        s.executeUpdate("insert into AC values (2, 'a', 1)");
        s.executeUpdate("insert into AC values (3, 'b', 2)");

        if(withSecondChild) {
            s.executeUpdate("create table YAC(col1 int, col2 varchar(2), col3 int, constraint fkey_2 foreign key(col2, col1) references AP(col2, col4) on delete set null)");
            s.executeUpdate("insert into YAC values (2, 'b', 41)");
            s.executeUpdate("insert into YAC values (2, 'b', 42)");
            s.executeUpdate("insert into YAC values (3, 'c', 43)");
        }

        if(withThridChildNoOnDeleteSetNull) {
            s.executeUpdate("create table LC(col1 int, col2 varchar(2), constraint fkey_3 foreign key(col2, col1) references AP(col2, col4))");
            s.executeUpdate("insert into LC values (2, 'b')");
            s.executeUpdate("insert into LC values (2, 'b')");
            s.executeUpdate("insert into LC values (3, 'c')");
        }
    }

    private static void createDataBaseObjects3(Statement s) throws SQLException {
        s.executeUpdate("");
    }

    @Test
    public void onDeleteSetNullWorks() throws Exception {
        try(Statement s = conn.createStatement()){
            createDatabaseObjects2(s, false, false);
            s.executeUpdate("delete from AP where col1 = 1");
            fKColsShouldBeNull("AC", new boolean[]{false, true, false});
        }
    }

    @Test
    public void onDeleteSetNullWorksWithMultipleChildTables() throws Exception {
        try(Statement s = conn.createStatement()){
            createDatabaseObjects2(s, true, false);
            s.executeUpdate("delete from AP where col1 = 2");
            fKColsShouldBeNull("AC", new boolean[]{true, false, true});
            fKColsShouldBeNull("YAC", new boolean[]{true, true, false});
        }
    }

    @Test
    public void onDeleteSetNullWorksWithMultipleChildTablesAndPredicates() throws Exception {
        try(Statement s = conn.createStatement()){
            createDatabaseObjects2(s, true, false);
            s.executeUpdate("delete from AP where col1 < 100");
            fKColsShouldBeNull("AC", new boolean[]{true, true, true});
            fKColsShouldBeNull("YAC", new boolean[]{true, true, true});
        }
    }

    @Test
    public void onDeleteSetNullWorksWithOtherChildTablesWithNoAction() throws Exception {
        try(Statement s = conn.createStatement()) {
            createDatabaseObjects2(s, true, true);
            assertQueryFail(s,"delete from AP where col1 = 2",
                    "Operation on table 'AP' caused a violation of foreign key constraint 'FKEY_3' for key (COL2,COL1).  The statement has been rolled back.");
            fKColsShouldBeNull("AC", new boolean[]{false, false, false}); // no partial updates!
            fKColsShouldBeNull("YAC", new boolean[]{false, false, false}); // no partial updates!
            fKColsShouldBeNull("LC", new boolean[]{false, false, false}); // no partial updates!
        }
    }

    @Test
    public void onDeleteSetNullWithReferencingTableWorksWithDeletingSelfReferencingRow() throws Exception {
        try(Statement s = conn.createStatement()) {
            s.executeUpdate("create table SRT (a int primary key, b int, constraint fk foreign key (B) references SRT(a) on delete set null)");
            s.executeUpdate("insert into SRT values (42, 42)");
            // writepipeline should recognize the delete in self-referencing table and avoids adding an update mutation.
            s.executeUpdate("delete from SRT where a = 42");
            ResultSet rs = s.executeQuery("select * from SRT");
            Assert.assertFalse(rs.next());
        }
    }

    @Test
    public void onDeleteSetNullWithReferencingTableWorks() throws Exception {
        try(Statement s = conn.createStatement()) {
            s.executeUpdate("create table SRT (a int primary key, b int, constraint fk foreign key (B) references SRT(a) on delete set null)");
            s.executeUpdate("insert into SRT values (42, null)");
            s.executeUpdate("insert into SRT values (43, 42)");
            s.executeUpdate("insert into SRT values (44, 43)");
            // writepipeline should recognize the delete in self-referencing table and avoids adding an update mutation.
            s.executeUpdate("delete from SRT where a = 42");
            ResultSet rs = s.executeQuery("select * from SRT order by a asc");
            Assert.assertTrue(rs.next());
            Assert.assertEquals(43, rs.getInt(1));rs.getInt(2);Assert.assertTrue(rs.wasNull());
            Assert.assertTrue(rs.next());
            Assert.assertEquals(44, rs.getInt(1));Assert.assertEquals(43, rs.getInt(2));
            Assert.assertFalse(rs.next());
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

    private void fKColsShouldBeNull(String child, boolean[] affectedRows) throws SQLException {
        try(Statement s = conn.createStatement()) {
            if(child.equals("YAC")) {
                ResultSet rs = s.executeQuery("select * from YAC order by col3 asc");

                Assert.assertTrue(rs.next()); // first row
                if(!affectedRows[0]) {
                     Assert.assertEquals(2, rs.getInt(1));Assert.assertEquals("b", rs.getString(2));Assert.assertEquals(41, rs.getInt(3));
                } else {
                    rs.getInt(1); Assert.assertTrue(rs.wasNull());rs.getString(2);Assert.assertTrue(rs.wasNull());Assert.assertEquals(41, rs.getInt(3));
                }
                Assert.assertTrue(rs.next()); // second row
                if(!affectedRows[1]) {
                    Assert.assertEquals(2, rs.getInt(1));Assert.assertEquals("b", rs.getString(2));Assert.assertEquals(42, rs.getInt(3));
                } else {
                    rs.getInt(1); Assert.assertTrue(rs.wasNull());rs.getString(2);Assert.assertTrue(rs.wasNull());Assert.assertEquals(42, rs.getInt(3));
                }
                Assert.assertTrue(rs.next()); // third row
                if(!affectedRows[2]) {
                    Assert.assertEquals(3, rs.getInt(1));Assert.assertEquals("c", rs.getString(2));Assert.assertEquals(43, rs.getInt(3));
                } else {
                    rs.getInt(1); Assert.assertTrue(rs.wasNull());rs.getString(2);Assert.assertTrue(rs.wasNull());Assert.assertEquals(43, rs.getInt(3));
                }
                Assert.assertFalse(rs.next());
            } else if(child.equals("AC")) {
                ResultSet rs = s.executeQuery("select * from AC order by col1 asc");
                Assert.assertTrue(rs.next()); // first row
                if(!affectedRows[0]) {
                    Assert.assertEquals(1, rs.getInt(1));Assert.assertEquals("b", rs.getString(2));Assert.assertEquals(2, rs.getInt(3));
                } else {
                    Assert.assertEquals(1, rs.getInt(1));rs.getString(2);Assert.assertTrue(rs.wasNull());rs.getInt(3); Assert.assertTrue(rs.wasNull());
                }
                Assert.assertTrue(rs.next()); // second row
                if(!affectedRows[1]) {
                    Assert.assertEquals(2, rs.getInt(1));Assert.assertEquals("a", rs.getString(2));Assert.assertEquals(1, rs.getInt(3));
                } else {
                    Assert.assertEquals(2, rs.getInt(1));rs.getString(2);Assert.assertTrue(rs.wasNull());rs.getInt(3); Assert.assertTrue(rs.wasNull());
                }
                Assert.assertTrue(rs.next()); // third row
                if(!affectedRows[2]) {
                    Assert.assertEquals(3, rs.getInt(1));Assert.assertEquals("b", rs.getString(2));Assert.assertEquals(2, rs.getInt(3));
                } else {
                    Assert.assertEquals(3, rs.getInt(1));rs.getString(2);Assert.assertTrue(rs.wasNull());rs.getInt(3); Assert.assertTrue(rs.wasNull());
                }
                Assert.assertFalse(rs.next());
            } else {
                assert child.equals("LC");
                ResultSet rs = s.executeQuery("select * from LC order by col1 asc");
                Assert.assertTrue(rs.next()); // first row
                Assert.assertEquals(2, rs.getInt(1));Assert.assertEquals("b", rs.getString(2));
                Assert.assertTrue(rs.next()); // second row
                Assert.assertEquals(2, rs.getInt(1));Assert.assertEquals("b", rs.getString(2));
                Assert.assertTrue(rs.next()); // third row
                Assert.assertEquals(3, rs.getInt(1));Assert.assertEquals("c", rs.getString(2));
                Assert.assertFalse(rs.next());

            }
        }
    }

}
