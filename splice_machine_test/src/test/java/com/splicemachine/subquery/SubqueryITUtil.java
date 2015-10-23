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
