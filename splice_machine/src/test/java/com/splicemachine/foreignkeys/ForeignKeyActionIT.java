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
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.derby.test.framework.TestConnection;
import com.splicemachine.test_dao.TableDAO;
import com.splicemachine.util.StatementUtils;
import org.junit.*;

import java.io.File;
import java.io.IOException;
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
 * ON DELETE RESTRICT
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
        new TableDAO(conn).drop(SCHEMA, "SNGC2", "SNGC1", "SNC", "SNP", "RP", "RC",
                                "DHC10", "DHC9", "DHC8", "DHC7", "DHC6", "DHC5", "DHC4", "DHC3", "DHC2", "DHC1",
                                "FC", "FP", "SRT2", "GC2", "GC1", "CC", "CP", "C1I", "C2I", "PI","SRT", "LC", "YAC", "AC", "AP", "C2", "C", "P",
                                "cc1", "cc2", "cc3", "cc4", "cc6", "cc7");
    }

    @After
    public void tearDown() throws Exception{
        conn.rollback();
        conn.reset();
    }

    private static File BADDIR;

    @BeforeClass
    public static void beforeClass() throws Exception {
        BADDIR = SpliceUnitTest.createBadLogDirectory(SCHEMA);
    }

    @AfterClass
    public static void afterClass() throws Exception {
        SpliceUnitTest.deleteTempDirectory(BADDIR);
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

    @Test
    public void onDeleteNoActionMultipleChildrenWorks() throws Exception {
        try(Statement s = conn.createStatement()){
            createDatabaseObjects1(s);
            s.executeUpdate("create table C2 (a int, b int, CONSTRAINT FK_2 FOREIGN KEY (a) REFERENCES P(a) ON DELETE NO ACTION)");
            s.executeUpdate("insert into P values(100,100)");
            s.executeUpdate("insert into C2 values(1,10),(100,100)");
            assertQueryFail(s,"delete from P where a = 100","Operation on table 'P' caused a violation of foreign key constraint 'FK_2' for key (A).  The statement has been rolled back.");
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

    private static void createDatabaseObjects3(Statement s) throws SQLException {
        s.executeUpdate("create table PI(col1 int, col2 varchar(2), primary key (col1))");
        s.executeUpdate("create table C1I(col1 int, col2 int, constraint ci_fk_key foreign key(col1) references PI(col1) on delete no action)");
        s.executeUpdate("create table C2I(col1 int, col2 int, constraint ci_fk_key2 foreign key(col1) references PI(col1) on delete no action)");
        s.executeUpdate("insert into PI values (1, 'a')");
        s.executeUpdate("insert into PI values (2, 'b')");
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

    @Test
    public void onDeleteNoActionFailsProperlyInImportDataIfMaxAllowedBadIsSetToZero() throws Exception {
        try(Statement s = conn.createStatement()) {
            createDatabaseObjects3(s);
            importData(s,"C1I", "fk_test/bad.csv", true);
            shouldContain("C1I", new int[][]{});
            importData(s, "C1I", "fk_test/good.csv", false);
            shouldContain("C1I", new int[][]{{1,1}, {2,2}});
            importData(s, "C2I", "fk_test/good.csv", false);
            shouldContain("C2I", new int[][]{{1,1}, {2,2}});
        }
    }

    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    //
    // FK - ON DELETE CASCADE
    //
    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    private static void createDatabaseObjects4(Statement s, boolean withGrandChildren, boolean withNoActionGrandChildren) throws SQLException {
        s.executeUpdate("create table CP(col1 int, col2 varchar(2), col3 int, col4 int, primary key (col2, col4))");
        s.executeUpdate("insert into CP values (1, 'a', 1, 1)");
        s.executeUpdate("insert into CP values (2, 'b', 2, 2)");
        s.executeUpdate("insert into CP values (3, 'c', 3, 3)");

        s.executeUpdate("create table CC(col1 int primary key, col2 varchar(2), col3 int, constraint ccfkey_1 foreign key(col2, col3) references CP(col2, col4) on delete cascade)");
        s.executeUpdate("insert into CC values (1, 'b', 2)");
        s.executeUpdate("insert into CC values (400, 'a', 1)");
        s.executeUpdate("insert into CC values (2, 'b', 2)");
        s.executeUpdate("insert into CC values (800, 'c', 3)");

        String clause = "cascade";
        if(withNoActionGrandChildren) {
            clause = "no action";
        }
        if(withGrandChildren) {
            s.executeUpdate("create table GC1(col1 int references CC(col1) on delete " + clause + ", col2 varchar(2))");
            s.executeUpdate("insert into GC1 values (1, 'z')");
            s.executeUpdate("insert into GC1 values (1, 'z')");
            s.executeUpdate("insert into GC1 values (1, 'z')");
            s.executeUpdate("insert into GC1 values (400, 'y')");

            s.executeUpdate("create table GC2(col1 int references CC(col1) on delete " + clause + ", col2 varchar(2))");
            s.executeUpdate("insert into GC2 values (2, 'y')");
            s.executeUpdate("insert into GC2 values (2, 'y')");
            s.executeUpdate("insert into GC2 values (2, 'y')");
            s.executeUpdate("insert into GC2 values (800, 'x')");
        }
    }

    @Test
    public void onDeleteCascadeWithChildWorks() throws SQLException {
        try(Statement s = conn.createStatement()) {
            createDatabaseObjects4(s, false, false);
            s.executeUpdate("delete from CP where col1 = 2");
            shouldContain("CP", new Object[][]{{1, "a", 1, 1}, {3, "c", 3, 3}});
            shouldContain("CC", new Object[][]{{400, "a", 1}, {800, "c", 3}});
        }
    }

    @Test
    public void onDeleteCascadeRecursiveWorks() throws SQLException {
        try(Statement s = conn.createStatement()) {
            createDatabaseObjects4(s, true, false);
            s.executeUpdate("delete from CP where col1 = 2");
            shouldContain("CP", new Object[][]{{1, "a", 1, 1}, {3, "c", 3, 3}});
            shouldContain("CC", new Object[][]{{400, "a", 1}, {800, "c", 3}});
            shouldContain("GC1", new Object[][]{{400, "y"}});
            shouldContain("GC2", new Object[][]{{800, "x"}});
        }
    }

    @Test
    public void onDeleteCascadeIsRolledbackProperlyIfRecursionFails() throws SQLException {
        try(Statement s = conn.createStatement()) {
            createDatabaseObjects4(s, true, true);
            try {
                s.executeUpdate("delete from CP where col1 = 2");
                Assert.fail("delete should have failed with error: Operation on table 'CC' caused a violation of foreign key constraint");
            } catch (SQLException se) {
                Assert.assertTrue(se.getMessage().contains("Operation on table 'CC' caused a violation of foreign key constraint"));
            }
            shouldContain("CP", new Object[][]{{1, "a", 1, 1}, {2, "b", 2, 2}, {3, "c", 3, 3}});
            shouldContain("CC", new Object[][]{{1, "b", 2}, {2, "b", 2}, {400, "a", 1}, {800, "c", 3}});
            shouldContain("GC1", new Object[][]{{1, "z"}, {1, "z"}, {1, "z"}, {400, "y"}});
            shouldContain("GC2", new Object[][]{{2, "y"}, {2, "y"}, {2, "y"}, {800, "x"}});
        }
    }

    @Test
    public void onDeleteCascadeWithReferencingTableWorks() throws Exception {
        try(Statement s = conn.createStatement()) {
            s.executeUpdate("create table SRT2 (a int primary key, b int, constraint fk foreign key (B) references SRT2(a) on delete cascade)");
            s.executeUpdate("insert into SRT2 values (42, null), (43, 42), (44,43), (45, null), (46,45)");
            s.executeUpdate("delete from SRT2 where a = 42");
            ResultSet rs = s.executeQuery("select * from SRT2 order by a asc");
            Assert.assertTrue(rs.next());
            Assert.assertEquals(45, rs.getInt(1));rs.getInt(2);Assert.assertTrue(rs.wasNull());
            Assert.assertTrue(rs.next());
            Assert.assertEquals(46, rs.getInt(1)); Assert.assertEquals(45, rs.getInt(2));
            Assert.assertFalse(rs.next());
        }
    }

    @Test
    public void constructingWriteContextFactoryUsingBulkWriteWorks() throws Exception {
        try(Statement s = conn.createStatement()) {
            s.executeUpdate("create table FP (col1 int, col2 varchar(2), col3 int, col4 int, primary key (col2, col4))");
            s.executeUpdate("create table FC(col1 int primary key, col2 varchar(2), col3 int, col4 timestamp, constraint CHILD_FKEY foreign key(col2, col3) references FP(col2, col4) on delete cascade)");
            s.executeUpdate("insert into FP values (1, 'a', 1, 1)");
            s.executeUpdate("insert into FC values (400, 'a', 1, '2019-09-09 10:10:10.123456')");
            s.executeUpdate("delete from FP where col1 = 1");
            try(ResultSet rs = s.executeQuery("select * from FC")) {
                Assert.assertFalse(rs.next());
            }
        }
    }

    @Test
    public void onDeleteRestrictIsSynonymToOnDeleteNoAction() throws Exception {
        try(Statement s = conn.createStatement()) {
            // RP, RC
            s.executeUpdate("create table RP (col1 int primary key)");
            s.executeUpdate("create table RC(col1 int references RP(col1) on delete restrict)");
            s.executeUpdate("insert into RP values (1)");
            s.executeUpdate("insert into RP values (2)");
            s.executeUpdate("insert into RC values (1)");
            s.executeUpdate("insert into RC values (2)");
            try {
                s.executeUpdate("delete from RP where col1 = 1");
                Assert.fail("expected an exception containing the following message: Operation on table 'RP' caused a violation of foreign key constraint");
            } catch(Exception e) {
                Assert.assertTrue(e instanceof SQLException);
                SQLException exception = (SQLException)e;
                Assert.assertEquals("23503", exception.getSQLState());
                Assert.assertTrue(exception.getMessage().contains("Operation on table 'RP' caused a violation of foreign key constraint"));
            }
        }
    }

    @Test
    public void onDeleteSetNullWithSomeNotNullableFieldsFailsProperly() throws Exception {
        try(Statement s = conn.createStatement()) {
            s.executeUpdate("create table SNP(col1 int primary key, col2 int)");
            s.executeUpdate("create table SNC(col1 int, col2 int, col3 int references SNP(col1) on delete cascade, primary key(col1, col2))");
            s.executeUpdate("create table SNGC1(col1 int primary key, col2 int, col3 int, foreign key(col2, col3) references SNC(col1, col2) on delete set null)");
            s.executeUpdate("create table SNGC2(col1 int primary key, col2 int not null, col3 int)");
            s.executeUpdate("alter table SNGC2 add constraint \"SNGC_FK\" foreign key(col2, col3) references SNC(col1, col2) on delete set null");
            s.executeUpdate("insert into SNP values (42, 42)");
            s.executeUpdate("insert into SNC values (42, 42, 42)");
            s.executeUpdate("insert into SNGC1 values (42, 42, 42)");
            s.executeUpdate("insert into SNGC2 values (42, 42, 42)");

            try{
                s.executeUpdate("delete from SNP where col1 = 42");
                fail("expected query to fail with error: foreign key SNGC_FK caused an attempt to set some not-nullable columns in table SNGC2 to null. The statement has been rolled back");
            } catch (Exception e) {
                Assert.assertTrue(e instanceof SQLException);
                SQLException sqlException = (SQLException)e;
                Assert.assertEquals("23514", sqlException.getSQLState());
                Assert.assertTrue(sqlException.getMessage().contains("foreign key SNGC_FK caused an attempt to set some not-nullable columns in table SNGC2 to null. The statement has been rolled back"));
            }
            shouldContain("SNP", new int[][]{{42,42}});
            shouldContain("SNC", new int[][]{{42,42,42}});
            shouldContain("SNGC1", new int[][]{{42,42,42}});
            shouldContain("SNGC2", new int[][]{{42,42,42}});
        }
    }

    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    //
    // test behavior of FK actions with deep hierarchies
    //
    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    private static void createDatabaseObjects5(Statement s, String leafTableFKAction) throws SQLException {
        s.executeUpdate("create table DHC1(col0 int primary key, col1 int, col2 varchar(2))");
        s.executeUpdate("create table DHC2(col0 int primary key, col1 int references DHC1(col0) on delete cascade, col2 varchar(2))");
        s.executeUpdate("create table DHC3(col0 int primary key, col1 int references DHC2(col0) on delete cascade, col2 varchar(2))");
        s.executeUpdate("create table DHC4(col0 int primary key, col1 int references DHC3(col0) on delete cascade, col2 varchar(2))");
        s.executeUpdate("create table DHC5(col0 int primary key, col1 int references DHC4(col0) on delete cascade, col2 varchar(2))");
        s.executeUpdate("create table DHC6(col0 int primary key, col1 int references DHC5(col0) on delete cascade, col2 varchar(2))");
        s.executeUpdate("create table DHC7(col0 int primary key, col1 int references DHC6(col0) on delete cascade, col2 varchar(2))");
        s.executeUpdate("create table DHC8(col0 int primary key, col1 int references DHC7(col0) on delete cascade, col2 varchar(2))");
        s.executeUpdate("create table DHC9(col0 int primary key, col1 int references DHC8(col0) on delete cascade, col2 varchar(2))");
        s.executeUpdate(String.format("create table DHC10(col0 int primary key, col1 int references DHC9(col0) on delete %s, col2 varchar(2))", leafTableFKAction));
        s.executeUpdate("insert into DHC1 values (400, 400, 'y')");
        s.executeUpdate("insert into DHC2 values (400, 400, 'y')");
        s.executeUpdate("insert into DHC3 values (400, 400, 'y')");
        s.executeUpdate("insert into DHC4 values (400, 400, 'y')");
        s.executeUpdate("insert into DHC5 values (400, 400, 'y')");
        s.executeUpdate("insert into DHC6 values (400, 400, 'y')");
        s.executeUpdate("insert into DHC7 values (400, 400, 'y')");
        s.executeUpdate("insert into DHC8 values (400, 400, 'y')");
        s.executeUpdate("insert into DHC9 values (400, 400, 'y')");
        s.executeUpdate("insert into DHC10 values (400, 400, 'y')");
    }

    @Test
    public void trackingOriginatingErrorsWorksCorrectly() throws Exception {
        try(Statement s = conn.createStatement()) {
            createDatabaseObjects5(s, "no action");
            try {
                s.executeUpdate("delete from DHC1");
                Assert.fail("expected SQLException containing: ERROR 23503: Operation on table 'DHC9' caused a violation of foreign key constraint");
            } catch(Exception se) {
                Assert.assertTrue(se instanceof SQLException);
                Assert.assertTrue(se.getMessage().contains("Operation on table 'DHC9' caused a violation of foreign key constraint"));
            }
        }
    }

    @Test
    public void cascadingDeleteWorksInDeepHierarchy() throws Exception {
        try (Statement s = conn.createStatement()) {
            createDatabaseObjects5(s, "cascade");
            s.executeUpdate("delete from DHC1");
            try (ResultSet rs = s.executeQuery("SELECT * FROM DHC10")) {
                Assert.assertFalse(rs.next());
            }
        }
    }

    @Test
    public void setNullWorksInDeepHierarchy() throws Exception {
        try (Statement s = conn.createStatement()) {
            createDatabaseObjects5(s, "set null");
            s.executeUpdate("delete from DHC1");
            try (ResultSet rs = s.executeQuery("SELECT * FROM DHC10")) {
                Assert.assertTrue(rs.next());
                Assert.assertEquals(400, rs.getInt(1));
                rs.getInt(2); Assert.assertTrue(rs.wasNull());
                Assert.assertEquals("y", rs.getString(3));
                Assert.assertFalse(rs.next());
            }
        }
    }

    @Test
    public void cacheIsCoherent() throws Exception {
        try(Statement s = conn.createStatement()) {
            s.executeUpdate("create table cct1(col1 int primary key, col2 int)");
            s.executeUpdate("create table cct2(col1 int primary key, col2 int references cct1(col1) on delete cascade)");
            s.executeUpdate("create table cct3(col1 int primary key, col2 int references cct1(col1) on delete cascade)");
            s.executeUpdate("create table cct4(col1 int primary key, col2 int)");
            s.executeUpdate("create table cct6(col1 int primary key, col2 int references cct2(col1) on delete cascade)");
            s.executeUpdate("create table cct7(col1 int primary key, col2 int references cct6(col1) on delete cascade)");
            s.executeUpdate("alter table cct7 add constraint \"ccfk1\" foreign key (col2) references cct4(col1) on delete set null");
            // we faced an issue where the cache wasn't up to date with each referencing constraint containing a list of children
            // if the cache is not working properly then this addition of the foreign key would succeed since cct1 will not have and children
            // cutting the links leading to inconsistent referential effects on table cct4 (cascade + set null) from cct1 which is wrong.
            // this test checks that the cache is always up to date with the constraint's state.
            try {
                s.executeUpdate("alter table cct4 add constraint \"ccfk2\" foreign key (col2) references cct3(col1) on delete cascade");
                Assert.fail("alter table should have failed with SQLException containing this message: Foreign  Key 'ccfk2' is invalid because " +
                                    "'adding this foreign key leads to the " +
                                    "conflicting delete actions on the table '\"FOREIGNKEYACTIONIT\".\"CCT7\"' " +
                                    "coming from this path 'PATH action: SetNull \"FOREIGNKEYACTIONIT\".\"CCT1\" " +
                                    "\"FOREIGNKEYACTIONIT\".\"CCT3\" \"FOREIGNKEYACTIONIT\".\"CCT4\" ' and this " +
                                    "path 'PATH action: Cascade \"FOREIGNKEYACTIONIT\".\"CCT1\" \"FOREIGNKEYACTIONIT\".\"CCT2\" " +
                                    "\"FOREIGNKEYACTIONIT\".\"CCT6\" ''. ");
            } catch (Exception e) {
                Assert.assertTrue(e instanceof SQLException);
                SQLException sqlException = (SQLException)e;
                Assert.assertEquals("42915", sqlException.getSQLState());
                Assert.assertTrue(sqlException.getMessage().contains("Foreign  Key 'ccfk2' is invalid because 'adding this foreign key leads to the " +
                                                                     "conflicting delete actions on the table '\"FOREIGNKEYACTIONIT\".\"CCT7\"' " +
                                                                     "coming from this path 'PATH action: SetNull \"FOREIGNKEYACTIONIT\".\"CCT1\" " +
                                                                     "\"FOREIGNKEYACTIONIT\".\"CCT3\" \"FOREIGNKEYACTIONIT\".\"CCT4\" ' and this " +
                                                                     "path 'PATH action: Cascade \"FOREIGNKEYACTIONIT\".\"CCT1\" \"FOREIGNKEYACTIONIT\".\"CCT2\" " +
                                                                     "\"FOREIGNKEYACTIONIT\".\"CCT6\" ''. "));
            }
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

    private void importData(Statement s, String childName, String fileName, boolean shouldFail) throws IOException, SQLException {
        try {
            s.execute(String.format("call SYSCS_UTIL.IMPORT_DATA(" +
                            "'%s'," +  // schema name
                            "'%s'," +  // table name
                            "null," +  // insert column list
                            "'%s'," +  // file path
                            "','," +   // column delimiter
                            "null," +  // character delimiter
                            "null," +  // timestamp format
                            "null," +  // date format
                            "null," +  // time format
                            "%d," +    // max bad records
                            "'%s'," +  // bad record dir
                            "null," +  // has one line records
                            "null)",   // char set
                    SCHEMA, childName, SpliceUnitTest.getResourceDirectory() + fileName, 0, BADDIR.getCanonicalPath()));
        } catch(SQLException se) {
            if(shouldFail) {
                Assert.assertTrue(se.getMessage().contains("Too many bad records in import, please check bad records"));
                return;
            } else {
                throw se;
            }
        }
        if(shouldFail) {
            Assert.fail("expected an exception containing this message: Too many bad records in import, please check bad records");
        }
    }

    private void shouldContain(String child, int[][] rows) throws SQLException {
        try(Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery(String.format("select * from %s order by col1 asc", child));
            for(int[] row : rows) {
                Assert.assertTrue(rs.next());
                for(int i = 0; i < row.length; ++i) {
                    Assert.assertEquals(row[i], rs.getInt(i+1));
                }
            }
            Assert.assertFalse(rs.next());
        }
    }

    private void shouldContain(String child, Object[][] rows) throws SQLException {
        try(Statement s = conn.createStatement()) {
            try (ResultSet rs = s.executeQuery(String.format("select * from %s order by col1 asc", child))) {
                for (Object[] row : rows) {
                    Assert.assertTrue(rs.next());
                    Assert.assertEquals(row[0], rs.getInt(1));
                    Assert.assertEquals(row[1], rs.getString(2));
                    if (row.length >= 3) {
                        Assert.assertEquals(row[2], rs.getInt(3));
                    }
                    if(row.length == 4) {
                        Assert.assertEquals(row[3], rs.getInt(4));
                    }
                    assert row.length < 5;
                }
                Assert.assertFalse(rs.next());
            }
        }
    }

}
