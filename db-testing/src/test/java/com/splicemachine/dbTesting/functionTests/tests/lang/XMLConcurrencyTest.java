/*
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 *
 * Some parts of this source code are based on Apache Derby, and the following notices apply to
 * Apache Derby:
 *
 * Apache Derby is a subproject of the Apache DB project, and is licensed under
 * the Apache License, Version 2.0 (the "License"); you may not use these files
 * except in compliance with the License. You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * Splice Machine, Inc. has modified the Apache Derby code in this file.
 *
 * All such Splice Machine modifications are Copyright 2012 - 2017 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */
package com.splicemachine.dbTesting.functionTests.tests.lang;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import junit.framework.Test;
import junit.framework.TestSuite;
import com.splicemachine.dbTesting.junit.BaseJDBCTestCase;
import com.splicemachine.dbTesting.junit.CleanDatabaseTestSetup;
import com.splicemachine.dbTesting.junit.JDBC;
import com.splicemachine.dbTesting.junit.TestConfiguration;
import com.splicemachine.dbTesting.junit.XML;

/**
 * Test that XML operators can be invoked by multiple threads concurrently.
 * Regression test case for DERBY-3870.
 */
public class XMLConcurrencyTest extends BaseJDBCTestCase {

    /** Create an instance of this test case. */
    public XMLConcurrencyTest(String name) {
        super(name);
    }

    /** Create a suite of all test cases in this class. */
    public static Test suite() {
        // XML operators are engine functionality, so run this test in
        // embedded mode only.
        if (XML.classpathMeetsXMLReqs()) {
            return new CleanDatabaseTestSetup(
                TestConfiguration.embeddedSuite(XMLConcurrencyTest.class)) {
                    protected void decorateSQL(Statement s)
                            throws SQLException {
                        createTestTable(s);
                    }
                };
        } else {
            return new TestSuite("XMLConcurrencyTest - empty");
        }
    }

    /**
     * Start four threads that execute queries that use all the XML operators.
     * If each thread manages 100 iterations without failing, running
     * concurrently with the other threads, the test case passes.
     */
    public void testConcurrency() throws Exception {
        WorkerThread[] allThreads = new WorkerThread[4];
        for (int i = 0; i < allThreads.length; i++) {
            allThreads[i] = new WorkerThread(openDefaultConnection(), 100);
            allThreads[i].start();
        }

        for (int i = 0; i < allThreads.length; i++) {
            allThreads[i].join();
            Throwable t = allThreads[i].throwable;
            if (t != null) {
                fail("Worker thread failed", t);
            }
        }
    }

    /**
     * A thread class that does the actual work in the test.
     */
    private class WorkerThread extends Thread {
        final Connection conn;
        final int iterations;
        Throwable throwable;

        WorkerThread(Connection conn, int iterations) {
            this.conn = conn;
            this.iterations = iterations;
        }

        public void run() {
            try {
                runXMLTest(conn, iterations);
            } catch (Throwable t) {
                throwable = t;
            }
        }
    }

    /**
     * <p>
     * Create a table with test data. The table contains three columns:
     * <p>
     *
     * <ol>
     * <li>
     * An ID column used to identify the rows and to give a stable ordering.
     * </li>
     * <li>
     * A VARCHAR column holding the string representation of an XML document.
     * </li>
     * <li>
     * An XML column holding the XML representation of the document in the
     * VARCHAR column.
     * </li>
     * </ol>
     */
    private static void createTestTable(Statement s) throws SQLException {
        s.executeUpdate("create table t (id int primary key " +
                "generated always as identity, vc varchar(100), " +
                "x generated always as " +
                "(xmlparse(document vc preserve whitespace)))");

        PreparedStatement ins = s.getConnection().prepareStatement(
                "insert into t(vc) values ?");

        String[] docs = {
            "<doc><a x='1'>abc</a><b x='2'>def</b></doc>",
            "<doc><a x='2'>abc</a><b x='3'>def</b></doc>",
            "<doc/>",
            "<a/>",
            null,
        };

        for (int i = 0; i < docs.length; i++) {
            ins.setString(1, docs[i]);
            ins.executeUpdate();
        }

        ins.close();
    }

    /**
     * Do the work for one of the worker threads. Perform queries that use
     * all the XML operators. Repeat the queries the specified number of times.
     *
     * @param conn the connection on which to execute the queries
     * @param iterations the number of times each query should be executed
     */
    private static void runXMLTest(Connection conn, int iterations)
            throws SQLException {
        // Query that tests XMLQUERY and XMLSERIALIZE. Count the number of
        // nodes with an attribute named x with a value greater than 1.
        PreparedStatement ps1 = conn.prepareStatement(
            "select id, xmlserialize(" +
            "xmlquery('count(//*[@x>1])' passing by ref x empty on empty) " +
            "as varchar(100)) from t order by id");

        String[][] expected1 = {
            {"1", "1"}, {"2", "2"}, {"3", "0"}, {"4", "0"}, {"5", null}
        };

        // Query that tests XMLEXISTS. Find all documents containing a "doc"
        // node with a nested "a" node whose x attribute is 2.
        PreparedStatement ps2 = conn.prepareStatement(
            "select id from t where " +
            "xmlexists('/doc/a[@x=2]' passing by ref x) " +
            "order by id");

        String expected2 = "2";

        // Query that tests XMLPARSE and XMLSERIALIZE.
        PreparedStatement ps3 = conn.prepareStatement(
            "select count(*) from t where " +
            "xmlserialize(xmlparse(document vc preserve whitespace) " +
            "as varchar(100)) = " +
            "xmlserialize(x as varchar(100))");

        String expected3 = "4";

        for (int i = 0; i < iterations; i++) {
            JDBC.assertFullResultSet(ps1.executeQuery(), expected1);
            JDBC.assertSingleValueResultSet(ps2.executeQuery(), expected2);
            JDBC.assertSingleValueResultSet(ps3.executeQuery(), expected3);
        }

        ps1.close();
        ps2.close();
        ps3.close();
        conn.close();
    }
}
