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
