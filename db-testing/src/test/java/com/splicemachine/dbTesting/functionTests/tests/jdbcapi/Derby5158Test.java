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
package com.splicemachine.dbTesting.functionTests.tests.jdbcapi;

import com.splicemachine.dbTesting.junit.BaseJDBCTestCase;
import com.splicemachine.dbTesting.junit.TestConfiguration;

import junit.framework.Test;
import junit.framework.TestSuite;

import java.sql.Statement;
import java.sql.SQLException;
import java.sql.ResultSet;


public class Derby5158Test extends BaseJDBCTestCase
{

    public Derby5158Test(String name)
    {
        super(name);
    }

    protected static Test makeSuite(String name)
    {
        TestSuite suite = new TestSuite(name);

        Test cst = TestConfiguration.defaultSuite(Derby5158Test.class);

        suite.addTest(cst);

        return suite;
    }

    public static Test suite()
    {
        String testName = "Derby5158Repro";

        return makeSuite(testName);
    }

    protected void setUp()
            throws java.lang.Exception {
        super.setUp();
        setAutoCommit(false);
    }


    /**
     * DERBY-5158
     */
    public void testCommitRollbackAfterShutdown() throws SQLException {

        Statement s = createStatement();
        ResultSet rs = s.executeQuery("select 1 from sys.systables");
        rs.close();
        s.close(); // just so we have a transaction, otherwise the commit is
                   // short-circuited in the client.

        TestConfiguration.getCurrent().shutdownDatabase();

        try {
            commit();
        } catch (SQLException e) {
            if (usingEmbedded()) {
                assertSQLState("08003", e);
            } else {
                // Before DERBY-5158, we saw "58009" instead with c/s.
                assertSQLState("08006", e);
            }
        }


        // bring db back up and start a transaction
        s = createStatement();
        rs = s.executeQuery("select 1 from sys.systables");
        rs.close();
        s.close(); 

        TestConfiguration.getCurrent().shutdownDatabase();

        try {
            rollback();
        } catch (SQLException e) {
            if (usingEmbedded()) {
                assertSQLState("08003", e);
            } else {
                // Before DERBY-5158, we saw "58009" instead with c/s.
                assertSQLState("08006", e);
            }
        }
    }
}
