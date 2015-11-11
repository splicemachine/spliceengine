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
        ResultSet resultSet = connection.createStatement().executeQuery("explain " + query);
        String explainPlanText = TestUtils.FormattedResult.ResultFactory.toString(resultSet);
        assertEquals(expectedSubqueryCountInPlan, countNodesInPlan(explainPlanText, "Subquery"));
    }

    /**
     * Assert that the plan for the specified query has the expected number of Distinct nodes.
     */
    public static void assertDistinctNodeCount(Connection connection,
                                               String query,
                                               int expectedDistinctCountInPlan) throws Exception {
        ResultSet resultSet = connection.createStatement().executeQuery("explain " + query);
        String explainPlanText = TestUtils.FormattedResult.ResultFactory.toString(resultSet);
        assertEquals(expectedDistinctCountInPlan, countNodesInPlan(explainPlanText, "Distinct"));
    }

    /**
     * Counts the number of nodes that appear in the explain plan text for a given query.
     */
    private static int countNodesInPlan(String explainPlanOutput, String nodeName) {
        Pattern pattern = Pattern.compile("^.*?" + nodeName + "\\s*\\(", Pattern.MULTILINE | Pattern.CASE_INSENSITIVE);
        int count = 0;
        Matcher matcher = pattern.matcher(explainPlanOutput);
        while (matcher.find()) {
            count++;

        }
        return count;
    }

}
