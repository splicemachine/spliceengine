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

package com.splicemachine.db.impl.sql.compile;


import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.homeless.TestUtils;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import splice.com.google.common.collect.Lists;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLSyntaxErrorException;
import java.sql.Types;
import java.util.Collection;
import java.util.List;

import static org.junit.Assert.*;
import static org.hamcrest.Matchers.containsString;

/**
 * Test predicate with tuples
 */
@RunWith(Parameterized.class)
public class PredicateTupleIT extends SpliceUnitTest {

    private Boolean useSpark;

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        Collection<Object[]> params = Lists.newArrayListWithCapacity(2);
        params.add(new Object[]{true});
        params.add(new Object[]{false});
        return params;
    }
    private static final String SCHEMA = PredicateTupleIT.class.getSimpleName();

    @ClassRule
    public static SpliceSchemaWatcher schemaWatcher = new SpliceSchemaWatcher(SCHEMA);

    @ClassRule
    public static SpliceWatcher classWatcher = new SpliceWatcher(SCHEMA);

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(SCHEMA);

    @BeforeClass
    public static void createSharedTables() throws Exception {
        TestUtils.executeSqlFile(classWatcher.getOrCreateConnection(), "subquery/PredSimplTestTables.sql", "");
    }

    @AfterClass
    public static void dropUDF() throws Exception {
        try {
            TestUtils.executeSqlFile(classWatcher.getOrCreateConnection(), "subquery/PredSimplTestCleanup.sql", "");
        }
        catch (Exception e) {
            // Don't error out if the UDF we want to drop does not exist.
        }
    }

    public PredicateTupleIT(Boolean useSpark) {
        this.useSpark = useSpark;
    }


    private void testQuery(String sqlText, String expected) throws Exception {
        ResultSet rs = null;
        try {
            rs = methodWatcher.executeQuery(sqlText);
            assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        }
        finally {
            if (rs != null)
                rs.close();
        }
    }

    @Test
    public void testSimpleTuple() throws Exception {
        String query = format("select * from A --SPLICE-PROPERTIES useSpark=%s\n" +
                "where (a1, a2) = (1,10)", useSpark);
        String expected = "A1 |A2 |A3  |\n" +
                "-------------\n" +
                " 1 |10 |100 |";

        testQuery(query, expected);
    }

    @Test
    public void testTupleWithPreparedStatement() throws Exception {
        String query = format("select * from D --splice-properties useSpark=%s\n" +
                "where (d1,d2,d3) = (?,?,?)", useSpark);
        String expected = "D1 |D2 |D3  |\n" +
                "-------------\n" +
                " 5 |50 |500 |";
        ResultSet rs = null;
        try {
            PreparedStatement ps = methodWatcher.prepareStatement(query);
            ps.setInt(1, 5);
            ps.setInt(2, 50);
            ps.setInt(3, 500);

            rs = ps.executeQuery();
            assertEquals("\n" + query + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        }
        finally {
            if (rs != null)
                rs.close();
        }
    }

    @Test
    public void testTupleDifferentSize() throws Exception {
        String query = format("select count(*) from D --splice-properties useSpark=%s\n" +
                "where (d1,d2,d3) = (0,1)", useSpark);
        try {
            testQuery(query, "");
            Assert.fail("This test should fail");
        } catch (SQLSyntaxErrorException e) {
            assertThat(e.getMessage(), containsString("Cannot compare tuples of different sizes"));
        }
    }

    @Test
    public void testTupleOrTuple() throws Exception {
        String query = format("select * from A --splice-properties useSpark=%s\n" +
                "where (a1,a2) = (0,0) or (a1,a2) = (1,10) order by 1", useSpark);
        String expected = "A1 |A2 |A3  |\n" +
                "-------------\n" +
                " 0 | 0 | 0  |\n" +
                " 1 |10 |100 |";
        testQuery(query, expected);
    }

    @Test
    public void testTupleOrTupleDifferentColumns() throws Exception {
        String query = format("select * from A --splice-properties useSpark=%s\n" +
                "where (a1,a2) = (1,10) or (a2,a3) = (20,200) order by 1", useSpark);
        String expected = "A1 |A2 |A3  |\n" +
                "-------------\n" +
                " 1 |10 |100 |\n" +
                " 2 |20 |200 |";
        testQuery(query, expected);
    }
}
