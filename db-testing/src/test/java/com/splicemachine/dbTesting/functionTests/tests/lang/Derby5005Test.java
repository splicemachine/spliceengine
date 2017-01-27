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

import java.sql.SQLException;
import java.sql.Statement;
import junit.framework.Test;
import junit.framework.TestSuite;
import com.splicemachine.dbTesting.junit.CleanDatabaseTestSetup;
import com.splicemachine.dbTesting.junit.BaseJDBCTestCase;
import com.splicemachine.dbTesting.junit.JDBC;

public class Derby5005Test extends BaseJDBCTestCase {

    public Derby5005Test(String name) {
        super(name);
    }

    /**
     * Construct top level suite in this JUnit test
     *
     * @return A suite containing embedded and client suites.
     */
    public static Test suite()
    {
        TestSuite suite = new TestSuite("Derby5005Test");

        suite.addTest(makeSuite());
        // suite.addTest(
        //      TestConfiguration.clientServerDecorator(makeSuite()));

        return suite;
    }

    /**
     * Construct suite of tests
     *
     * @return A suite containing the test cases.
     */
    private static Test makeSuite()
    {
        return new CleanDatabaseTestSetup(
            new TestSuite(Derby5005Test.class)) {
                protected void decorateSQL(Statement s)
                        throws SQLException {
                    getConnection().setAutoCommit(false);

                    s.execute("create table app.a (a integer)");
                    s.execute("create view app.v as select * from app.a");
                    s.execute("insert into app.a (a) values(1)");

                    getConnection().commit();
                }
            };
    }

    public void testInsertSelectOrderBy5005() throws SQLException {

        Statement s = createStatement();

        JDBC.assertFullResultSet(
            s.executeQuery("select app.a.a from app.a where app.a.a <> 2 " +
                           "order by app.a.a asc"),
            new String[][]{{"1"}});

        JDBC.assertFullResultSet(
            s.executeQuery("select app.v.a from app.v where app.v.a <> 2 " +
                           "order by v.a asc"),
            new String[][]{{"1"}});

        // Next query fails in DERBY-5005:
        JDBC.assertFullResultSet(
            s.executeQuery("select v.a from app.v where v.a <> 2 " +
                           "order by app.v.a asc"),
            new String[][]{{"1"}});
    }
}
