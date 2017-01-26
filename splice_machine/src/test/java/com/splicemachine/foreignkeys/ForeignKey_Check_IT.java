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
import com.splicemachine.test_tools.TableCreator;
import org.junit.*;
import org.junit.experimental.categories.Category;

import java.sql.Statement;
import java.util.regex.Pattern;

import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;
import static org.junit.Assert.*;

/**
 * Foreign key tests for *checking* that the FK constraint is enforced in various scenarios.
 */
// SPLICE-894 Remove Serial
@Category(value = {SerialTest.class})
public class ForeignKey_Check_IT {

    private static final String SCHEMA = ForeignKey_Check_IT.class.getSimpleName();

    @ClassRule
    public static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(SCHEMA);

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(SCHEMA);

    private TestConnection conn;

    @Before
    public void deleteTables() throws Exception {
        conn = methodWatcher.getOrCreateConnection();
        conn.setAutoCommit(false);
    }

    @After
    public void tearDown() throws Exception{
        conn.rollback();
    }

    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    //
    // single column foreign keys
    //
    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    @Test
    public void referencing_singleColumn_primaryKey() throws Exception {

        new TableCreator(conn)
                .withCreate("create table P (a varchar(10), b int, primary key(a))")
                .withInsert("insert into P values(?,?)")
                .withRows(rows(row("A", 100), row("B", 200), row("C", 300))).create();

        new TableCreator(conn)
                .withCreate("create table C (a varchar(10) CONSTRAINT c_fk_1 REFERENCES P, b int)")
                .withInsert("insert into C values(?,?)")
                .withRows(rows(row("A", 100), row("B", 200), row("C", 300))).create();

        assertEquals(3L, conn.count("select * from P"));
        assertEquals(3L, conn.count("select * from C"));

        assertQueryFail("insert into C values('D', 200)", "Operation on table 'C' caused a violation of foreign key constraint 'C_FK_1' for key (A).  The statement has been rolled back.");
        assertQueryFail("update C set a='Z' where a='A'", "Operation on table 'C' caused a violation of foreign key constraint 'C_FK_1' for key (A).  The statement has been rolled back.");
    }

    @Test
    public void referencing_singleColumn_uniqueIndex() throws Exception {

        new TableCreator(conn)
                .withCreate("create table P (a varchar(10) unique, b int)")
                .withInsert("insert into P values(?,?)")
                .withRows(rows(row("A", 100), row("B", 200), row("C", 300))).create();

        new TableCreator(conn)
                .withCreate("create table C (a varchar(10) CONSTRAINT c_fk_1 REFERENCES P(a), b int)")
                .withInsert("insert into C values(?,?)")
                .withRows(rows(row("A", 100), row("B", 200), row("C", 300))).create();

        assertEquals(3L, conn.count("select * from P"));
        assertEquals(3L, conn.count("select * from C"));

        assertQueryFail("insert into C values('D', 200)", "Operation on table 'C' caused a violation of foreign key constraint 'C_FK_1' for key (A).  The statement has been rolled back.");
        assertQueryFail("update C set a='Z' where a='A'", "Operation on table 'C' caused a violation of foreign key constraint 'C_FK_1' for key (A).  The statement has been rolled back.");
    }

    /* DB-694 */
    @Test
    public void childRowsCannotReferenceDeletedRowsInParent() throws Exception {
        // given -- C -> P
        try(Statement s = conn.createStatement()){
            s.executeUpdate("create table P(a int primary key)");
            s.executeUpdate("create table C(a int references P(a))");
            s.executeUpdate("insert into P values(1),(2),(3),(4)");
            s.executeUpdate("insert into C values(1),(2),(3),(4)");

            // when -- we delete rows from the parent (after deleting child rows, to allow this)
            s.executeUpdate("delete from C");
            s.executeUpdate("delete from P");
        }

        // then -- we should not be able to insert rows (that were previously there) into C
        assertQueryFailMatch("insert into C values(1),(2),(3),(4)", "Operation on table 'C' caused a violation of foreign key constraint 'SQL\\d+' for key \\(A\\).  The statement has been rolled back.");
        // then -- we should not be able to insert rows (that were NOT previously there) into C
        assertQueryFailMatch("insert into C values(5)", "Operation on table 'C' caused a violation of foreign key constraint 'SQL\\d+' for key \\(A\\).  The statement has been rolled back.");
    }

    @Test
    // DB-5437
    public void childRowsCannotReferenceDeletedRowsInParentWithFirstRowDeleted() throws Exception {
        try(Statement s = conn.createStatement()){
            s.executeUpdate("create table P(a int primary key)");
            s.executeUpdate("create table C(a int, b int references P(a))");
            s.executeUpdate("insert into P values(1),(2)");
            s.executeUpdate("insert into C values(1,1),(2,1),(3,1),(4,2),(5,2),(6,2)");
            s.executeUpdate("delete from C where a = 1");
            assertQueryFailMatch("delete from P where a = 1","Operation on table 'P' caused a violation of foreign key constraint 'SQL\\d+' for key \\(B\\).  The statement has been rolled back.");
        }

    }


    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    //
    // multi-column foreign keys
    //
    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    @Test
    public void referencing_twoColumn_primaryKey() throws Exception {

        new TableCreator(conn)
                .withCreate("create table P (a varchar(10), b int, c int, primary key(a, b))")
                .withInsert("insert into P values(?,?,?)")
                .withRows(rows(
                        row("A", 100, 1),
                        row("A", 150, 1),
                        row("B", 200, 2),
                        row("B", 250, 2),
                        row("C", 300, 3)
                )).create();

        new TableCreator(conn)
                .withCreate("create table C (a varchar(10), b int, CONSTRAINT id_fk FOREIGN KEY (a,b) REFERENCES P(a,b))")
                .withInsert("insert into C values(?,?)")
                .withRows(rows(row("A", 100), row("B", 200), row("C", 300))).create();

        assertEquals(5L, conn.count("select * from P"));
        assertEquals(3L, conn.count("select * from C"));

        /* one column value missing */
        assertQueryFail("insert into C values('C', 700)", "Operation on table 'C' caused a violation of foreign key constraint 'ID_FK' for key (A,B).  The statement has been rolled back.");
        /* two columns values missing */
        assertQueryFail("insert into C values('D', 200)", "Operation on table 'C' caused a violation of foreign key constraint 'ID_FK' for key (A,B).  The statement has been rolled back.");

        assertQueryFail("update C set a='Z' where a='A'", "Operation on table 'C' caused a violation of foreign key constraint 'ID_FK' for key (A,B).  The statement has been rolled back.");
    }

    @Test
    public void referencing_twoColumn_uniqueIndex() throws Exception {

        new TableCreator(conn)
                .withCreate("create table P (a varchar(10), b int, c int, UNIQUE(a,b))")
                .withInsert("insert into P values(?,?,?)")
                .withRows(rows(
                                row("A", 100, 1),
                                row("A", 200, 1),
                                row("B", 100, 2),
                                row("B", 200, 2),
                                row("C", 300, 3)
                        )
                ).create();

        new TableCreator(conn)
                .withCreate("create table C (a varchar(10), b int, CONSTRAINT c_fk1 FOREIGN KEY (a,b) REFERENCES P(a,b))")
                .withInsert("insert into C values(?,?)")
                .withRows(rows(row("A", 100), row("B", 200), row("C", 300))).create();

        assertEquals(5L, conn.count("select * from P"));
        assertEquals(3L, conn.count("select * from C"));

        /* one column value missing */
        assertQueryFail("insert into C values('D', 200)", "Operation on table 'C' caused a violation of foreign key constraint 'C_FK1' for key (A,B).  The statement has been rolled back.");
        /* two columns values missing */
        assertQueryFail("insert into C values('A', 300)", "Operation on table 'C' caused a violation of foreign key constraint 'C_FK1' for key (A,B).  The statement has been rolled back.");

        assertQueryFail("update C set a='Z' where a='A'", "Operation on table 'C' caused a violation of foreign key constraint 'C_FK1' for key (A,B).  The statement has been rolled back.");
    }

    @Test
    public void referencing_twoColumn_uniqueIndex_withOrderSwap() throws Exception {

        new TableCreator(conn)
                .withCreate("create table P (a int, b int, UNIQUE(a, b))")
                .withInsert("insert into P values(?,?)")
                .withRows(rows(row(100, 1), row(100, 2), row(100, 3))).create();

        new TableCreator(conn)
                .withCreate("create table C (x int, y int, CONSTRAINT fk FOREIGN KEY (y, x) REFERENCES P(a, b))")
                .withInsert("insert into C values(?,?)")
                .withRows(rows(row(1, 100), row(2, 100), row(3, 100))).create();

        assertEquals(3L, conn.count("select * from P"));
        assertEquals(3L, conn.count("select * from C"));

        /* one column value missing */
        assertQueryFail("insert into C values(4, 100)", "Operation on table 'C' caused a violation of foreign key constraint 'FK' for key (Y,X).  The statement has been rolled back.");
        /* two columns values missing */
        assertQueryFail("insert into C values(9, 900)", "Operation on table 'C' caused a violation of foreign key constraint 'FK' for key (Y,X).  The statement has been rolled back.");

        assertQueryFail("update C set x=-1 where x=1", "Operation on table 'C' caused a violation of foreign key constraint 'FK' for key (Y,X).  The statement has been rolled back.");
    }

    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    //
    // self-referencing foreign key
    //
    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    @Test
    public void referencing_self() throws Exception {

        TestConnection connection=conn;
        new TableCreator(connection)
                .withCreate("create table P (a int primary key, b int, CONSTRAINT fk FOREIGN KEY (B) REFERENCES P(a))")
                .withInsert("insert into P values(?,?)")
                .withRows(rows(row(1, null), row(2, null), row(3, 1), row(4, 1), row(5, 1), row(6, 1))).create();

        assertEquals(6L, connection.count("select * from P"));

        assertQueryFail("insert into P values (7, -1)", "Operation on table 'P' caused a violation of foreign key constraint 'FK' for key (B).  The statement has been rolled back.");

        assertQueryFail("update P set b=-1 where b=1", "Operation on table 'P' caused a violation of foreign key constraint 'FK' for key (B).  The statement has been rolled back.");
    }

    @Test
    public void referencing_self_multiple_times() throws Exception {

        TestConnection connection=conn;
        new TableCreator(connection)
                .withCreate("create table P (a int primary key, b int, c int, CONSTRAINT fk1 FOREIGN KEY (B) REFERENCES P(a), CONSTRAINT fk2 FOREIGN KEY (c) REFERENCES P(a))")
                .withInsert("insert into P values(?,?,?)")
                .withRows(rows(row(1, null, null), row(2, null, null), row(3, 1, 2), row(4, 1, 2), row(5, 1, 3), row(6, 1, 5)))
                .create();

        assertEquals(6L, connection.count("select * from P"));

        assertQueryFail("insert into P values (7, -1, 1)", "Operation on table 'P' caused a violation of foreign key constraint 'FK1' for key (B).  The statement has been rolled back.");
        assertQueryFail("insert into P values (7, 1, -1)", "Operation on table 'P' caused a violation of foreign key constraint 'FK2' for key (C).  The statement has been rolled back.");

        assertQueryFail("update P set b=-1 where B=1", "Operation on table 'P' caused a violation of foreign key constraint 'FK1' for key (B).  The statement has been rolled back.");
        assertQueryFail("update P set c=-1 where c=2", "Operation on table 'P' caused a violation of foreign key constraint 'FK2' for key (C).  The statement has been rolled back.");
    }

    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    //
    // floating-point columns in foreign key
    //
    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    @Test
    public void doubleValue_singleColumn() throws Exception {

        TestConnection connection=conn;
        new TableCreator(connection)
                .withCreate("create table P (a double primary key, b int)")
                .withInsert("insert into P values(?,?)")
                .withRows(rows(row(1.1, 1), row(0.0, 2), row(2.2, 2), row(3.9, 3), row(4.5, 3))).create();

        new TableCreator(connection)
                .withCreate("create table C (a double, b int, CONSTRAINT fk1 FOREIGN KEY (a) REFERENCES P(a))")
                .withInsert("insert into C values(?,?)")
                .withRows(rows(row(1.1, 200), row(0.0, 200), row(2.2, 200), row(3.9, 200)))
                .create();

        assertEquals(5L,connection.count("select * from P"));
        assertEquals(4L, connection.count("select * from C"));

        assertQueryFail("insert into C values (1.100001, 1.0)", "Operation on table 'C' caused a violation of foreign key constraint 'FK1' for key (A).  The statement has been rolled back.");
        assertQueryFail("insert into C values (0.000001, 4.0)", "Operation on table 'C' caused a violation of foreign key constraint 'FK1' for key (A).  The statement has been rolled back.");

        assertQueryFail("update C set a=-1.1 where a=1.1", "Operation on table 'C' caused a violation of foreign key constraint 'FK1' for key (A).  The statement has been rolled back.");
    }

    @Test
    public void doubleValue_twoColumn() throws Exception {

        new TableCreator(conn)
                .withCreate("create table P (a double, b double, c double, d double, primary key(b,c))")
                .withInsert("insert into P values(?,?,?,?)")
                .withRows(rows(row(1.0, 1.0, 1.0, 1.0), row(2.0, 2.0, 2.0, 2.0), row(3.0, 3.0, 3.0, 3.0))).create();

        new TableCreator(conn)
                .withCreate("create table C (a double, b double, c double, d double, CONSTRAINT FK1 FOREIGN KEY (b,c) REFERENCES P(b,c))")
                .withInsert("insert into C values(?,?,?,?)")
                .withRows(rows(row(1.0, 1.0, 1.0, 1.0), row(2.0, 2.0, 2.0, 2.0), row(3.0, 3.0, 3.0, 3.0))).create();

        assertEquals(3L, conn.count("select * from P"));
        assertEquals(3L, conn.count("select * from C"));

        assertQueryFail("insert into C values (1.0, 1.0, 4.0, 1.0)", "Operation on table 'C' caused a violation of foreign key constraint 'FK1' for key (B,C).  The statement has been rolled back.");
        assertQueryFail("insert into C values (1.0, 4.0, 1.0, 1.0)", "Operation on table 'C' caused a violation of foreign key constraint 'FK1' for key (B,C).  The statement has been rolled back.");

        assertQueryFail("update C set b=-1.0 where b=1.0", "Operation on table 'C' caused a violation of foreign key constraint 'FK1' for key (B,C).  The statement has been rolled back.");
        assertQueryFail("update C set c=-1.0 where c=1.0", "Operation on table 'C' caused a violation of foreign key constraint 'FK1' for key (B,C).  The statement has been rolled back.");

        // UPDATE: success
        try(Statement s = conn.createStatement()){
            assertEquals(1,s.executeUpdate("update C set b=3.0, c=3.0 where b=1.0 and c=1.0"));
        }
        assertEquals(2L, conn.count("select * from C where b=3.0 AND c=3.0 "));
        assertEquals(0L, conn.count("select * from C where b=1.0 AND c=1.0 "));
        assertEquals(3L, conn.count("select * from C"));
    }

    @Test
    public void floatValue_threeColumn() throws Exception {

        new TableCreator(conn)
                .withCreate("create table P (a float, b float, c float, d float, e float, f float, primary key(b,d,f))")
                .withInsert("insert into P values(?,?,?,?,?,?)")
                .withRows(rows(row(1.1, 1.1, 1.1, 1.1, 1.1, 1.1), row(2.2, 2.2, 2.2, 2.2, 2.2, 2.2), row(3.3, 3.3, 3.3, 3.3, 3.3, 3.3))).create();

        new TableCreator(conn)
                .withCreate("create table C (a double, b double, c double, d double, CONSTRAINT FK1 FOREIGN KEY (b,c,d) REFERENCES P(b,d,f))")
                .withInsert("insert into C values(?,?,?,?)")
                .withRows(rows(row(1.1, 1.1, 1.1, 1.1), row(2.2, 2.2, 2.2, 2.2), row(3.3, 3.3, 3.3, 3.3)))
                .create();

        assertEquals(3L, conn.count("select * from P"));
        assertEquals(3L, conn.count("select * from C"));

        assertQueryFail("insert into C values (1.0, 1.0, 4.0, 1.0)", "Operation on table 'C' caused a violation of foreign key constraint 'FK1' for key (B,C,D).  The statement has been rolled back.");
        assertQueryFail("insert into C values (1.0, 4.0, 1.0, 1.0)", "Operation on table 'C' caused a violation of foreign key constraint 'FK1' for key (B,C,D).  The statement has been rolled back.");

        assertQueryFail("update C set b=-1.1 where a=1.1", "Operation on table 'C' caused a violation of foreign key constraint 'FK1' for key (B,C,D).  The statement has been rolled back.");
    }

    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    //
    // column values having encoding with one are more bytes = 0
    //
    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    @Test
    public void intValue_withZeroByteAsPartOfEncoding() throws Exception {
        // given -- parent table with compound primary key with two values that encode with zeros
        // -2147483648           encodes as [23, -128, 0, 0, 0]
        // -9219236770852362184L encodes as [4, -128, 14, -79, 0, -91, 32, 40, 56]
        new TableCreator(conn)
                .withCreate("create table P (a int, b bigint, primary key(a,b))")
                .withInsert("insert into P values(?,?)")
                .withRows(rows(row(-2147483648, -9219236770852362184L))).create();

        // when -- child table has FK referencing parent
        new TableCreator(conn)
                .withCreate("create table C (a int, b bigint, CONSTRAINT FK1 FOREIGN KEY (a,b) REFERENCES P(a,b))").create();

        // then -- we can successfully insert zero encoding values
        try(Statement s = conn.createStatement()){
            s.executeUpdate("insert into C values(-2147483648, -9219236770852362184)");
        }
        // then -- but we cannot insert values that don't exist in parent table
        assertQueryFail("insert into C values(-1, -1)", "Operation on table 'C' caused a violation of foreign key constraint 'FK1' for key (A,B).  The statement has been rolled back.");
    }

    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    //
    // NULL -- child rows can be insert if any FK column contains a null, regardless of if a similar row exists in
    //         the parent.
    //
    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    @Test
    public void nullValues_referencing_singleColumnUniqueIndex() throws Exception {

        new TableCreator(conn)
                .withCreate("create table P (a int, b int, UNIQUE(a))")
                .withInsert("insert into P values(?,?)")
                .withRows(rows(row(100, 1), row(200, 2), row(300, 3))).create();

        new TableCreator(conn)
                .withCreate("create table C (a int, b int, CONSTRAINT fk FOREIGN KEY (a) REFERENCES P(a))")
                .withInsert("insert into C values(?,?)")
                .withRows(rows(row(null, 1), row(100, 1), row(null, -1))).create();

        // Just asserting that we were able to insert into child non-matching rows with null in FK-cols.
        assertEquals(3L, conn.count("select * from C"));

        // Verify we can update to null
        try(Statement s = conn.createStatement()){
            s.executeUpdate("update C set a=null where a=100");
            assertEquals(3L,conn.count(s,"select * from C where a is null"));
        }
    }

    @Test
    public void nullValues_referencing_twoColumnUniqueIndex() throws Exception {

        new TableCreator(conn)
                .withCreate("create table P (a int, b int, UNIQUE(a, b))")
                .withInsert("insert into P values(?,?)")
                .withRows(rows(row(100, 1), row(100, 2), row(100, 3))).create();

        new TableCreator(conn)
                .withCreate("create table C (a int, b int, CONSTRAINT fk FOREIGN KEY (a, b) REFERENCES P(a, b))")
                .withInsert("insert into C values(?,?)")
                .withRows(rows(row(1, null), row(null, 100), row(100, 3))).create();

        // Just asserting that we were able to insert into child non-matching rows with null in FK-cols.
        assertEquals(3L, conn.count("select * from C"));

        // Verify we can update to null
        try(Statement s = conn.createStatement()){
            s.executeUpdate("update C set a=null,b=null where a=100 and b=3");
            assertEquals(1L,conn.count(s,"select * from C where a is null and b is null"));
        }
    }

    @Test
    public void nullValues_referencing_twoColumnDoubleUniqueIndex() throws Exception {

        new TableCreator(conn)
                .withCreate("create table P (a double, b double, UNIQUE(a, b))")
                .withInsert("insert into P values(?,?)")
                .withRows(rows(row(100.1, 1.1), row(100.1, 2.1), row(100.1, 3.1))).create();

        new TableCreator(conn)
                .withCreate("create table C (a double, b double, CONSTRAINT fk FOREIGN KEY (a, b) REFERENCES P(a, b))")
                .withInsert("insert into C values(?,?)")
                .withRows(rows(row(1.0, null), row(null, 100.0), row(1.0, null), row(100.1, 3.1))).create();

        // Just asserting that we were able to insert into child non-matching rows with null in FK-cols.
        assertEquals(4L, conn.count("select * from C"));

        // Verify we can update to null
        try(Statement s = conn.createStatement()){
            s.executeUpdate("update C set a=null,b=null where a=100.1 and b=3.1");
            assertEquals(1L,conn.count("select * from C where a is null and b is null"));
        }
    }

    @Test
    public void nullValues_referencing_threeColumnMultiTypePrimaryKey() throws Exception {

        new TableCreator(conn)
                .withCreate("create table P (a varchar(9), b float, c int, d int, primary key(a,b,c))")
                .withInsert("insert into P values(?,?,?,?)")
                .withRows(rows(row("11", 1.1f, 1, 1.1d), row("22", 2.2f, 2, 2.2d), row("33", 3.3f, 3, 3.3d)))
                .create();

        new TableCreator(conn)
                .withCreate("create table C (c1 int, a varchar(9), b float, c int, CONSTRAINT fk FOREIGN KEY (a, b, c) REFERENCES P(a, b, c))")
                .withInsert("insert into C values(?,?,?,?)")
                .withRows(rows(
                                row(1, "11", 1.1f, 1), row(2, "22", 2.2f, 2), row(3, "22", 2.2f, 2),
                                row(1, null, 1.1f, 1), row(2, "22", null, 2), row(3, "22", 2.2f, null),
                                row(1, null, null, 1), row(2, "22", null, null), row(3, null, 2.2f, null),
                                row(3, null, null, null)
                        )
                )
                .create();

        // Just asserting that we were able to insert into child non-matching rows with null in FK-cols.
        assertEquals(10L, conn.count("select * from C"));
    }

    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    //
    // failure semantics
    //
    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    @Test
    public void failure_insert_rollsBackFailedStatementOnlyNotEntireTransaction() throws Exception {

        new TableCreator(conn).withCreate("create table P (a int, b int, CONSTRAINT p_index UNIQUE(a))").create();
        new TableCreator(conn).withCreate("create table C (a int CONSTRAINT fk1 REFERENCES P(a), b int)").create();

        try(Statement s = conn.createStatement()){
            s.executeUpdate("insert into P values(100,1),(200,2),(300,3)");
            s.executeUpdate("insert into C values(100,1)");
            s.executeUpdate("insert into C values(200,1)");
        }

        // INSERT
        assertQueryFail("insert into C values(-1,-1)", "Operation on table 'C' caused a violation of foreign key constraint 'FK1' for key (A).  The statement has been rolled back.");
        assertEquals(2L, conn.count("select * from C"));
        assertEquals(0L, conn.count("select * from C WHERE a=-1"));
        // UPDATE
        assertQueryFail("update C set a=-1 where a=100", "Operation on table 'C' caused a violation of foreign key constraint 'FK1' for key (A).  The statement has been rolled back.");
        assertEquals(2L, conn.count("select * from C"));
        assertEquals(0L, conn.count("select * from C WHERE a=-1"));
    }

    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    //
    // large-ish inserts/updates
    //
    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    /* Also verifies that FK constraint is not enforcing uniqueness in child table. */
    @Test
    public void large_oneThousandRowsInChildTable() throws Exception {

        new TableCreator(conn).withCreate("create table P (a int, b int, CONSTRAINT p_index UNIQUE(a))").create();
        new TableCreator(conn).withCreate("create table C (a int CONSTRAINT c1 REFERENCES P(a), b int)").create();

        try(Statement s = conn.createStatement()){
            s.executeUpdate("insert into P values(10,1),(20,2),(30,3),(40,4),(50,5),(60,6),(70,7),(80,8)");
            s.executeUpdate("insert into C values(10,1),(20,2),(30,3),(40,4),(50,5),(60,6),(70,7),(80,8)");

            for(int i=0;i<7;i++){
                s.executeUpdate("insert into C select * from C");
            }

            assertEquals(8L,conn.count(s,"select * from P"));
            assertEquals(1024L,conn.count(s,"select * from C"));

            // Insert 1024 rows, these should all fail
            assertQueryFail("insert into C select b,a from C","Operation on table 'C' caused a violation of foreign key constraint 'C1' for key (A).  The statement has been rolled back.");
            // Update 1024 rows, these should all fail
            assertQueryFail("update C set a=-1","Operation on table 'C' caused a violation of foreign key constraint 'C1' for key (A).  The statement has been rolled back.");

            assertEquals(1024L,conn.count("select * from C"));
        }
    }

    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    //
    // multiple FK per table
    //
    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    /* This test also tests FKs of all column types */
    @Test
    public void multipleForeignKeysOnChildTable() throws Exception {

        TestConnection connection=conn;
        new TableCreator(connection)
                .withCreate("create table P (a varchar(9), b real, c double, d int, e bigint, f smallint, g decimal(11, 2), h date, i time, j timestamp," +
                        "CONSTRAINT u0 UNIQUE(a), CONSTRAINT u1 UNIQUE(b), CONSTRAINT u2 UNIQUE(c), CONSTRAINT u3 UNIQUE(d), " +
                        "CONSTRAINT u4 UNIQUE(e), CONSTRAINT u5 UNIQUE(f), CONSTRAINT u6 UNIQUE(g), CONSTRAINT u7 UNIQUE(h)," +
                        "CONSTRAINT u8 UNIQUE(i), CONSTRAINT u9 UNIQUE(j)" +
                        ")")
                .withInsert("insert into P values(?,?,?,?,?,?,?,?,?,?)")
                .withRows(rows(
                        row("aaa", 1.0f, 3.2d, 3, 6L, 126, 333333333.33, "2015-01-27", "09:15:30", "2000-02-02 02:02:02.002"),
                        row("bbb", 6.2f, 6.4d, 6, 12L, 127, 444444444.44, "2015-01-28", "13:15:30", "2001-02-02 02:02:02.002")
                ))
                .create();

        new TableCreator(connection)
                .withCreate("create table C (a varchar(9), b real, c double, d int, e bigint, f smallint, g decimal(11, 2), h date, i time, j timestamp," +
                        "CONSTRAINT fk0 FOREIGN KEY(a) REFERENCES P(a)," +
                        "CONSTRAINT fk1 FOREIGN KEY(b) REFERENCES P(b)," +
                        "CONSTRAINT fk2 FOREIGN KEY(c) REFERENCES P(c)," +
                        "CONSTRAINT fk3 FOREIGN KEY(d) REFERENCES P(d)," +
                        "CONSTRAINT fk4 FOREIGN KEY(e) REFERENCES P(e)," +
                        "CONSTRAINT fk5 FOREIGN KEY(f) REFERENCES P(f)," +
                        "CONSTRAINT fk6 FOREIGN KEY(g) REFERENCES P(g)," +
                        "CONSTRAINT fk7 FOREIGN KEY(h) REFERENCES P(h)," +
                        "CONSTRAINT fk8 FOREIGN KEY(i) REFERENCES P(i)," +
                        "CONSTRAINT fk9 FOREIGN KEY(j) REFERENCES P(j)" +
                        ")")
                .withInsert("insert into C values(?,?,?,?,?,?,?,?,?,?)")
                .withRows(rows(
                        row("aaa", 1.0f, 3.2d, 3, 6L, 126, 333333333.33, "2015-01-27", "09:15:30", "2000-02-02 02:02:02.002"),
                        row("bbb", 6.2f, 6.4d, 6, 12L, 127, 444444444.44, "2015-01-28", "09:15:30", "2000-02-02 02:02:02.002")
                ))
                .create();

        // Just asserting that we were able to insert into child non-matching rows with null in FK-cols.
        assertEquals(2L, conn.count("select * from C"));

        // this works
        try(Statement s = conn.createStatement()){
            s.execute("insert into C values ('aaa', 1.0, 3.2, 3, 6, 126, 333333333.33, '2015-01-27', '09:15:30', '2000-02-02 02:02:02.002')");
        }

        assertQueryFail("insert into C values ('ZZZ', 1.0, 3.2, 3, 6, 126, 333333333.33, '2015-01-27', '09:15:30', '2000-02-02 02:02:02.002')", "Operation on table 'C' caused a violation of foreign key constraint 'FK0' for key (A).  The statement has been rolled back.");
        assertQueryFail("insert into C values ('aaa', -1.0, 3.2, 3, 6, 126, 333333333.33, '2015-01-27', '09:15:30', '2000-02-02 02:02:02.002')", "Operation on table 'C' caused a violation of foreign key constraint 'FK1' for key (B).  The statement has been rolled back.");
        assertQueryFail("insert into C values ('aaa', 1.0, -3.2, 3, 6, 126, 333333333.33, '2015-01-27', '09:15:30', '2000-02-02 02:02:02.002')", "Operation on table 'C' caused a violation of foreign key constraint 'FK2' for key (C).  The statement has been rolled back.");
        assertQueryFail("insert into C values ('aaa', 1.0, 3.2, -3, 6, 126, 333333333.33, '2015-01-27', '09:15:30', '2000-02-02 02:02:02.002')", "Operation on table 'C' caused a violation of foreign key constraint 'FK3' for key (D).  The statement has been rolled back.");
        assertQueryFail("insert into C values ('aaa', 1.0, 3.2, 3, -6, 126, 333333333.33, '2015-01-27', '09:15:30', '2000-02-02 02:02:02.002')", "Operation on table 'C' caused a violation of foreign key constraint 'FK4' for key (E).  The statement has been rolled back.");
        assertQueryFail("insert into C values ('aaa', 1.0, 3.2, 3,  6, -126, 333333333.33, '2015-01-27', '09:15:30', '2000-02-02 02:02:02.002')", "Operation on table 'C' caused a violation of foreign key constraint 'FK5' for key (F).  The statement has been rolled back.");
        assertQueryFail("insert into C values ('aaa', 1.0, 3.2, 3,  6, 126, -333333333.33, '2015-01-27', '09:15:30', '2000-02-02 02:02:02.002')", "Operation on table 'C' caused a violation of foreign key constraint 'FK6' for key (G).  The statement has been rolled back.");
        assertQueryFail("insert into C values ('aaa', 1.0, 3.2, 3,  6, 126, 333333333.33, '1999-12-31', '09:15:30', '2000-02-02 02:02:02.002')", "Operation on table 'C' caused a violation of foreign key constraint 'FK7' for key (H).  The statement has been rolled back.");
        assertQueryFail("insert into C values ('aaa', 1.0, 3.2, 3,  6, 126, 333333333.33, '2015-01-27', '01:01:01', '2000-02-02 02:02:02.002')", "Operation on table 'C' caused a violation of foreign key constraint 'FK8' for key (I).  The statement has been rolled back.");

        assertQueryFail("update C set a='ZZZ'", "Operation on table 'C' caused a violation of foreign key constraint 'FK0' for key (A).  The statement has been rolled back.");
        assertQueryFail("update C set b=-1", "Operation on table 'C' caused a violation of foreign key constraint 'FK1' for key (B).  The statement has been rolled back.");
        assertQueryFail("update C set c=-1", "Operation on table 'C' caused a violation of foreign key constraint 'FK2' for key (C).  The statement has been rolled back.");
        assertQueryFail("update C set d=-1", "Operation on table 'C' caused a violation of foreign key constraint 'FK3' for key (D).  The statement has been rolled back.");
        assertQueryFail("update C set e=-1", "Operation on table 'C' caused a violation of foreign key constraint 'FK4' for key (E).  The statement has been rolled back.");
        assertQueryFail("update C set f=-1", "Operation on table 'C' caused a violation of foreign key constraint 'FK5' for key (F).  The statement has been rolled back.");
        assertQueryFail("update C set g=-1", "Operation on table 'C' caused a violation of foreign key constraint 'FK6' for key (G).  The statement has been rolled back.");
        assertQueryFail("update C set h='2007-01-01'", "Operation on table 'C' caused a violation of foreign key constraint 'FK7' for key (H).  The statement has been rolled back.");
        assertQueryFail("update C set i='02:02:02'", "Operation on table 'C' caused a violation of foreign key constraint 'FK8' for key (I).  The statement has been rolled back.");
        assertQueryFail("update C set j='1999-12-12 12:12:12.012'", "Operation on table 'C' caused a violation of foreign key constraint 'FK9' for key (J).  The statement has been rolled back.");
    }

    /* When there are multiple FKs per column (not just per table) they share the same backing index */
    @Test
    public void multipleForeignKeysPerColumn() throws Exception {
        // given -- three parent tables and one child that references all three
        try(Statement s=conn.createStatement()){
            s.executeUpdate("create table P1(a int primary key)");
            s.executeUpdate("create table P2(a int primary key)");
            s.executeUpdate("create table P3(a int primary key)");

            s.executeUpdate("insert into P1 values(1),(9)");
            s.executeUpdate("insert into P2 values(2),(9)");
            s.executeUpdate("insert into P3 values(3),(9)");

            s.executeUpdate("create table C(a int, "+
                    "    CONSTRAINT fk1 FOREIGN KEY (a) REFERENCES P1(a),"+
                    "    CONSTRAINT fk2 FOREIGN KEY (a) REFERENCES P2(a),"+
                    "    CONSTRAINT fk3 FOREIGN KEY (a) REFERENCES P3(a)"+
                    ")");

            // then - we can insert into the child a value present all three
            s.executeUpdate("insert into C values(9)");
        }

        // then - we cannot insert any value NOT present in all three
        assertQueryFailMatch("insert into C values(1)", "Operation on table 'C' caused a violation of foreign key constraint 'FK[2|3]' for key \\(A\\).  The statement has been rolled back.");
        assertQueryFailMatch("insert into C values(2)", "Operation on table 'C' caused a violation of foreign key constraint 'FK[1|3]' for key \\(A\\).  The statement has been rolled back.");
        assertQueryFailMatch("insert into C values(3)", "Operation on table 'C' caused a violation of foreign key constraint 'FK[1|2]' for key \\(A\\).  The statement has been rolled back.");
    }

    /* When there are multiple FKs per column (not just per table) they share the same backing index */
    @Test
    public void multipleForeignKeysPerColumn_fkAddedByAlterTable() throws Exception {
        // given -- three parent tables and one child
        try(Statement s = conn.createStatement()){
            s.executeUpdate("create table P1(a int primary key)");
            s.executeUpdate("create table P2(a int primary key)");
            s.executeUpdate("create table P3(a int primary key)");

            s.executeUpdate("insert into P1 values(1),(9)");
            s.executeUpdate("insert into P2 values(2),(9)");
            s.executeUpdate("insert into P3 values(3),(9)");

            s.executeUpdate("create table C(a int, "+
                    "    CONSTRAINT fk1 FOREIGN KEY (a) REFERENCES P1(a)"+
                    ")");

            // when - make sure the write context for C is initialized
            s.executeUpdate("insert into C values(9)");

            // when - alter table add FK after write context is initialized
            s.executeUpdate("ALTER table C add constraint FK2 FOREIGN KEY (a) REFERENCES P2(a)");
            s.executeUpdate("ALTER table C add constraint FK3 FOREIGN KEY (a) REFERENCES P3(a)");

            // then - we can insert into the child a value present all three
            s.executeUpdate("insert into C values(9)");
        }

        // then - we cannot insert any value NOT present in all three
        assertQueryFailMatch("insert into C values(1)", "Operation on table 'C' caused a violation of foreign key constraint 'FK[2|3]' for key \\(A\\).  The statement has been rolled back.");
        assertQueryFailMatch("insert into C values(2)", "Operation on table 'C' caused a violation of foreign key constraint 'FK[1|3]' for key \\(A\\).  The statement has been rolled back.");
        assertQueryFailMatch("insert into C values(3)", "Operation on table 'C' caused a violation of foreign key constraint 'FK[1|2]' for key \\(A\\).  The statement has been rolled back.");
    }

    @Test
    public void multipleTablesReferencingSameTable() throws Exception {
        new TableCreator(conn)
                .withCreate("create table P (a int primary key, b int)")
                .withInsert("insert into P values(?,?)")
                .withRows(rows(row(100, 100), row(200, 200), row(300, 300))).create();

        new TableCreator(conn)
                .withCreate("create table C1 (a int CONSTRAINT c_fk_1 REFERENCES P, b int)")
                .withInsert("insert into C1 values(?,?)")
                .withRows(rows(row(100, 100), row(200, 200), row(300, 300))).create();

        new TableCreator(conn)
                .withCreate("create table C2 (a int CONSTRAINT c_fk_2 REFERENCES P, b int)")
                .withInsert("insert into C2 values(?,?)")
                .withRows(rows(row(100, 100), row(200, 200), row(300, 300))).create();

        assertQueryFail("insert into C1 values (-100, 100)", "Operation on table 'C1' caused a violation of foreign key constraint 'C_FK_1' for key (A).  The statement has been rolled back.");
        assertQueryFail("insert into C2 values (-100, 100)", "Operation on table 'C2' caused a violation of foreign key constraint 'C_FK_2' for key (A).  The statement has been rolled back.");

        assertQueryFail("update C1 set a=-1", "Operation on table 'C1' caused a violation of foreign key constraint 'C_FK_1' for key (A).  The statement has been rolled back.");
        assertQueryFail("update C2 set a=-1", "Operation on table 'C2' caused a violation of foreign key constraint 'C_FK_2' for key (A).  The statement has been rolled back.");
    }


    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    //
    // FK is UNIQUE in child table -- this is a very distinct case because we will reuse the unique index in the
    //                                child table rather than creating a new non-unique backing index for the FK
    //                                constraint.
    //
    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    @Test
    public void uniqueConstraintOnForeignKeyConstraintCols() throws Exception {
        new TableCreator(conn)
                .withCreate("create table P (a varchar(10), b int, primary key(a))")
                .withInsert("insert into P values(?,?)")
                .withRows(rows(row("A", 100), row("B", 200), row("C", 300))).create();

        new TableCreator(conn)
                .withCreate("create table C (a varchar(10), b int, CONSTRAINT c_fk_1 FOREIGN KEY(a) REFERENCES P, CONSTRAINT c_u_idx UNIQUE(a))")
                .withInsert("insert into C values(?,?)")
                .withRows(rows(row("A", 100), row("B", 200), row("C", 300))).create();

        assertEquals(3L, conn.count("select * from P"));
        assertEquals(3L, conn.count("select * from C"));

        assertQueryFail("insert into C values('D', 200)", "Operation on table 'C' caused a violation of foreign key constraint 'C_FK_1' for key (A).  The statement has been rolled back.");
        assertQueryFail("update C set a='Z' where a='A'", "Operation on table 'C' caused a violation of foreign key constraint 'C_FK_1' for key (A).  The statement has been rolled back.");

        // verify that the unique constraint is enforced, DB-3100
        assertQueryFail("insert into C values('B', 200)", "The statement was aborted because it would have caused a duplicate key value in a unique or primary key constraint or unique index identified by 'C_U_IDX' defined on 'C'.");
    }


    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    //
    // helper methods
    //
    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    private void assertQueryFail(String sql, String expectedExceptionMessage) {
        try(Statement s = conn.createStatement()){
            s.executeUpdate(sql);
            fail(String.format("query '%s', did not fail/throw. Expected exception is '%s'", sql, expectedExceptionMessage));
        } catch (Exception e) {
            assertEquals(expectedExceptionMessage, e.getMessage());
        }
    }

    private void assertQueryFailMatch(String sql, String expectedExceptionMessagePattern) {
        try(Statement s = conn.createStatement()){
            s.executeUpdate(sql);
            fail(String.format("query '%s', did not fail/throw. Expected exception pattern is '%s'", sql, expectedExceptionMessagePattern));
        } catch (Exception e) {
            assertTrue(String.format("exception '%s' did not match expected pattern '%s'", e.getMessage(), expectedExceptionMessagePattern),
                    Pattern.compile(expectedExceptionMessagePattern).matcher(e.getMessage()).matches());
        }
    }

}