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

package com.splicemachine.subquery;

import com.splicemachine.homeless.TestUtils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;

public class SubqueryITUtil {

    /**
     * Constants to make usage of assertSubqueryNodeCount method a little more readable.
     */
    public static final int ZERO_SUBQUERY_NODES = 0;
    public static final int ONE_SUBQUERY_NODE = 1;
    public static final int TWO_SUBQUERY_NODES = 2;

    /* See SubqueryFlatteningTestTables.sql.  Queries that select from A with predicates that do not eliminate
     * any rows expect this result. */
    public static final String RESULT_ALL_OF_A = "" +
            "A1  | A2  |\n" +
            "------------\n" +
            "  0  |  0  |\n" +
            "  1  | 10  |\n" +
            " 11  | 110 |\n" +
            " 12  | 120 |\n" +
            " 12  | 120 |\n" +
            " 13  |  0  |\n" +
            " 13  |  1  |\n" +
            "  2  | 20  |\n" +
            "  3  | 30  |\n" +
            "  4  | 40  |\n" +
            "  5  | 50  |\n" +
            "  6  | 60  |\n" +
            "  7  | 70  |\n" +
            "NULL |NULL |";

    /**
     * Assert that the query executes with the expected result and that it executes using the expected number of
     * subqueries
     */
    public static void assertUnorderedResult(Connection connection,
                                             String query,
                                             int expectedSubqueryCountInPlan,
                                             String expectedResult) throws Exception {
        ResultSet rs = connection.createStatement().executeQuery(query);
        assertEquals(expectedResult, TestUtils.FormattedResult.ResultFactory.toString(rs));

        assertSubqueryNodeCount(connection, query, expectedSubqueryCountInPlan);
    }

    /**
     * Assert that the plan for the specified query has the expected number of Subquery nodes.
     */
    public static void assertSubqueryNodeCount(Connection connection,
                                               String query,
                                               int expectedSubqueryCountInPlan) throws Exception {
        ResultSet rs2 = connection.createStatement().executeQuery("explain " + query);
        String explainPlanText = TestUtils.FormattedResult.ResultFactory.toString(rs2);
        assertEquals(expectedSubqueryCountInPlan, countSubqueriesInPlan(explainPlanText));
    }

    /**
     * Counts the number of Subquery nodes that appear in the explain plan text for a given query.
     */
    private static int countSubqueriesInPlan(String a) {
        Pattern pattern = Pattern.compile("^.*?Subquery\\s*\\(", Pattern.MULTILINE | Pattern.CASE_INSENSITIVE);
        int count = 0;
        Matcher matcher = pattern.matcher(a);
        while (matcher.find()) {
            count++;

        }
        return count;
    }

}
