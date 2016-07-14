/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.util;

import org.junit.Assert;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;

/**
 * Utility class for correctly performing different actions with a JDBC statement object
 * @author Scott Fines
 *         Date: 6/2/16
 */
public class StatementUtils{

    /**
     * Get the only long contained in this query. Enforces the assumption that the query should only
     * return 1 row, and the only column should be a long, and the contents of that column is non-null.
     *
     * @param s the statement to use
     * @param query the query to run
     * @return the long contained in the first column of the <em>only</em> row returned
     * @throws SQLException if the SQL fails
     * @throws AssertionError if any of the following occur:
     *                         1. There are no rows returned
     *                         2. The value in the column is null
     *                         3. More than one row is returned
     */
    public static long onlyLong(Statement s,String query) throws SQLException{
        try(ResultSet rs = s.executeQuery(query)){
            Assert.assertTrue("No rows returned!",rs.next());
            long c = rs.getLong(1);
            Assert.assertFalse("Returned null for count!",rs.wasNull());
            Assert.assertFalse("Too many rows returned!",rs.next());
            return c;
        }
    }

    /**
     * Get the only int contained in this query. Enforces the assumption that the query should only
     * return 1 row, and the only column should be a int, and the contents of that column is non-null.
     *
     * @param s the statement to use
     * @param query the query to run
     * @return the int contained in the first column of the <em>only</em> row returned
     * @throws SQLException if the SQL fails
     * @throws AssertionError if any of the following occur:
     *                         1. There are no rows returned
     *                         2. The value in the column is null
     *                         3. More than one row is returned
     */
    public static int onlyInt(Statement s,String query) throws SQLException{
        try(ResultSet rs = s.executeQuery(query)){
            Assert.assertTrue("No rows returned!",rs.next());
            int c = rs.getInt(1);
            Assert.assertFalse("Returned null for count!",rs.wasNull());
            Assert.assertFalse("Too many rows returned!",rs.next());
            return c;
        }
    }

    /**
     * Get the only string contained in this query. Enforces the assumption that the query should only
     * return 1 row, and the only column should be a string, and the contents of that column is non-null.
     *
     * @param s the statement to use
     * @param query the query to run
     * @return the string contained in the first column of the <em>only</em> row returned
     * @throws SQLException if the SQL fails
     * @throws AssertionError if any of the following occur:
     *                         1. There are no rows returned
     *                         2. The value in the column is null
     *                         3. More than one row is returned
     */
    public static String onlyString(Statement s,String query) throws SQLException{
        try(ResultSet rs = s.executeQuery(query)){
            Assert.assertTrue("No rows returned!",rs.next());
            String c = rs.getString(1);
            Assert.assertFalse("Returned null for count!",rs.wasNull());
            Assert.assertFalse("Too many rows returned!",rs.next());
            return c;
        }
    }

    /**
     * Get the only timestamp contained in this query. Enforces the assumption that the query should only
     * return 1 row, and the only column should be a timestamp, and the contents of that column is non-null.
     *
     * @param s the statement to use
     * @param query the query to run
     * @return the long contained in the first column of the <em>only</em> row returned
     * @throws SQLException if the SQL fails
     * @throws AssertionError if any of the following occur:
     *                         1. There are no rows returned
     *                         2. The value in the column is null
     *                         3. More than one row is returned
     */
    public static Timestamp onlyTimestamp(Statement s,String query) throws SQLException{
        try(ResultSet rs = s.executeQuery(query)){
            Assert.assertTrue("No rows returned!",rs.next());
            Timestamp ts = rs.getTimestamp(1);
            Assert.assertFalse("Null returned!",rs.wasNull());
            Assert.assertFalse("Too many rows returned!",rs.next());
            return ts;
        }
    }

}
