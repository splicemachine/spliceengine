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

package com.splicemachine.derby.impl.sql.execute.operations.joins;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.homeless.TestUtils;
import com.splicemachine.test.SerialTest;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.ResultSet;

import static org.junit.Assert.assertEquals;

/**
 * Integration tests for NestedLoopJoinOperation.
 */
//@Category(SerialTest.class) //in Serial category because of the NestedLoopIteratorClosesStatements test
public class NestedLoopJoinOperationIT extends SpliceUnitTest {

    private static final String SCHEMA = NestedLoopJoinOperationIT.class.getSimpleName().toUpperCase();
    @ClassRule
    public static SpliceSchemaWatcher schemaWatcher = new SpliceSchemaWatcher(SCHEMA);
    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(SCHEMA);

    /* Regression test for DB-1027 */
    @Test
    public void testCanJoinTwoTablesWithViewAndQualifiedSinkOperation() throws Exception {
        // B
        methodWatcher.executeUpdate("create table B(c1 int, c2 int, c3 char(1), c4 int, c5 int, c6 int)");
        // B2
        methodWatcher.executeUpdate("create table B2 (c1 int, c2 int, c3 char(1), c4 int, c5 int,c6 int)");
        methodWatcher.executeUpdate("insert into B2 (c5,c1,c3,c4,c6) values (3,4, 'F',43,23)");
        // B3
        methodWatcher.executeUpdate("create table B3(c8 int, c9 int, c5 int, c6 int)");
        methodWatcher.executeUpdate("insert into B3 (c5,c8,c9,c6) values (2,3,19,28)");
        // B4
        methodWatcher.executeUpdate("create table B4(c7 int, c4 int, c6 int)");
        methodWatcher.executeUpdate("insert into B4 (c7,c4,c6) values (4, 42, 31)");
        // VIEW
        methodWatcher.executeUpdate("create view bvw (c5,c1,c2,c3,c4) as select c5,c1,c2,c3,c4 from B2 union select c5,c1,c2,c3,c4 from B");

        ResultSet rs = methodWatcher.executeQuery("select B3.* from B3 join BVW on (B3.c8 = BVW.c5) join B4 on (BVW.c1 = B4.c7) where B4.c4 = 42");
        assertEquals("" +
                "C8 |C9 |C5 |C6 |\n" +
                "----------------\n" +
                " 3 |19 | 2 |28 |", toString(rs));
    }

    @Test
    public void joinEmptyRightSideOnConstant() throws Exception {
        methodWatcher.executeUpdate("create table DB4003(a int)");
        methodWatcher.executeUpdate("create table EMPTY_TABLE(e int)");
        methodWatcher.executeUpdate("insert into DB4003 values 1,2,3");
        ResultSet rs = methodWatcher.executeQuery("select count(*) from DB4003 join (select 1 r from EMPTY_TABLE) foo --SPLICE-PROPERTIES joinStrategy=NESTEDLOOP\n on TRUE");
        assertEquals("" +
                "1 |\n" +
                "----\n" +
                " 0 |", toString(rs));

    }

    private String toString(ResultSet rs) throws Exception {
        return TestUtils.FormattedResult.ResultFactory.toString(rs);
    }

    // DB-4883 (Wells)
    // See JoinWithTrimIT for additional coverage
    @Test
    public void validateNoTrimOnVarchar() throws Exception {
        methodWatcher.executeUpdate("create table left1 (col1 int, col2 varchar(25))");
        methodWatcher.executeUpdate("create table right1 (col1 int, col2 varchar(25))");
        methodWatcher.executeUpdate("insert into left1 values (1,'123')");
        methodWatcher.executeUpdate("insert into right1 values (1,'123')");
        methodWatcher.executeUpdate("insert into right1 values (1,'123 ')");
        methodWatcher.executeUpdate("insert into right1 values (1,'123  ')");
        ResultSet rs = methodWatcher.executeQuery("select * from left1 left outer join right1 --splice-properties joinStrategy=NESTEDLOOP\n" +
                " on left1.col2 = right1.col2");
        Assert.assertEquals("NestedLoop Returned Extra Row",1,SpliceUnitTest.resultSetSize(rs)); //DB-4883
        rs = methodWatcher.executeQuery("select * from left1 left outer join right1 --splice-properties joinStrategy=BROADCAST\n" +
                " on left1.col2 = right1.col2");
        Assert.assertEquals("Broadcast Returned Extra Row",1,SpliceUnitTest.resultSetSize(rs)); //DB-4883
    }

}