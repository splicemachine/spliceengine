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
import com.splicemachine.dbTesting.junit.BaseJDBCTestCase;
import com.splicemachine.dbTesting.junit.DatabasePropertyTestSetup;

/**
 * This class tests SQL referential actions.
 */
public class ReferentialActionsTest extends BaseJDBCTestCase {

    public ReferentialActionsTest(String name) {
        super(name);
    }

    public static Test suite() {
        TestSuite suite = new TestSuite("ReferentialActionsTest");

        // DERBY-2353: Need to set db.language.logQueryPlan to expose the
        // bug (got a NullPointerException when writing the plan to db.log)
        suite.addTest(DatabasePropertyTestSetup.singleProperty(
                new ReferentialActionsTest("onDeleteCascadeWithLogQueryPlan"),
                "derby.language.logQueryPlan", "true", true));

        return suite;
    }

    /**
     * Test that cascading delete works when db.language.logQueryPlan is
     * set to true - DERBY-2353.
     */
    public void onDeleteCascadeWithLogQueryPlan() throws SQLException {
        setAutoCommit(false);
        Statement s = createStatement();
        s.execute("create table a (a1 int primary key)");
        s.execute("insert into a values 1");
        s.execute("create table b (b1 int references a on delete cascade)");
        s.execute("insert into b values 1");
        // The next line used to cause a NullPointerException
        s.execute("delete from a");
    }
}
