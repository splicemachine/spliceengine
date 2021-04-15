// mimicking com.splicemachine.dbTesting.junit.BaseJDBCTestCase
// step-by-step addressing DB-10995
package com.splicemachine.derby.dbTesting;

import java.sql.SQLException;
import java.sql.Statement;
import static org.junit.Assert.fail;

public class BaseJDBCTestCase {

    public static String format(String format, Object...args) {
        return String.format(format, args);
    }

    protected static void testFail(String expectedErrorCode,
                                   String sqlText,
                                   Statement s) throws AssertionError {

        try {
            s.execute(sqlText);
            String failMsg = format("SQL not expected to succeed.\n%s", sqlText);
            fail(failMsg);
        }
        catch (Exception e) {
            boolean found = false;
            String extraText = "";
            if (e instanceof SQLException) {
                found = ((SQLException) e).getSQLState().equals(expectedErrorCode);
                if (!found)
                    extraText = format("found error code: %s", ((SQLException) e).getSQLState());
            }
            if (!found) {
                fail(format("\n + Expected error code: %s, ", expectedErrorCode) + extraText);
            }
        }
    }

    protected static void assertStatementError(String expectedErrorCode,
                                               Statement s,
                                               String sqlText) throws AssertionError {
        testFail(expectedErrorCode, sqlText, s);
    }
}
