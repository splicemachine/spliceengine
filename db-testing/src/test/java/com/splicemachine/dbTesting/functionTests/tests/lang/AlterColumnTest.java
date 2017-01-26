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
import java.sql.PreparedStatement;

import junit.framework.Test;
import com.splicemachine.dbTesting.junit.BaseJDBCTestCase;
import com.splicemachine.dbTesting.junit.TestConfiguration;
import com.splicemachine.dbTesting.junit.JDBC;

/**
 * Test cases for altering columns.
 */
public class AlterColumnTest extends BaseJDBCTestCase {

    public AlterColumnTest(String name) {
        super(name);
    }

    public static Test suite() {
        return TestConfiguration.defaultSuite(AlterColumnTest.class);
    }

    /**
     * Test ALTER COLUMN default
     */
    public void testAlterDefault() throws SQLException {
        Statement s = createStatement();
        s.execute("create table t(i int default 0)");
        PreparedStatement pd = prepareStatement("delete from t");
        PreparedStatement pi = prepareStatement("insert into t values default");
        PreparedStatement ps = prepareStatement("select * from t order by i");

        pi.executeUpdate();
        JDBC.assertFullResultSet(
            ps.executeQuery(), new String[][]{ {"0"} });

        /*
         * Try different syntaxes allowed
         */
        s.execute("alter table t alter COLUMN i DEFAULT 1");
        tryAndExpect(pd, pi, ps, "1");

        s.execute("alter table t alter COLUMN i WITH DEFAULT 2");
        tryAndExpect(pd, pi, ps, "2");

        // Standard SQL syntax added in DERBY-4013
        s.execute("alter table t alter COLUMN i SET DEFAULT 3");
        tryAndExpect(pd, pi, ps, "3");

        s.execute("alter table t alter i DEFAULT 4");
        tryAndExpect(pd, pi, ps, "4");

        s.execute("alter table t alter i WITH DEFAULT 5");
        tryAndExpect(pd, pi, ps, "5");

        // Standard SQL syntax added in DERBY-4013
        s.execute("alter table t alter i SET DEFAULT 6");
        tryAndExpect(pd, pi, ps, "6");

        s.execute("alter table t alter i SET DEFAULT null");
        tryAndExpect(pd, pi, ps, null);

        s.execute("alter table t alter i SET DEFAULT 1");
        tryAndExpect(pd, pi, ps, "1");

        // Standard SQL syntax added in DERBY-4013
        s.execute("alter table t alter i DROP DEFAULT");
        tryAndExpect(pd, pi, ps, null);

        s.close();
        pd.close();
        pi.close();
        ps.close();
    }

    /**
     * Auxiliary method: Execute the delete statement d to clean table, then
     * the insert statement i to exercise the default mechanism, then check via
     * the select statement s, that the value inserted is e.
     *
     * @param d delete statement
     * @param i insert statement
     * @param s select statement
     * @param e expected value as a string
     */
    private static void tryAndExpect(
        PreparedStatement d,
        PreparedStatement i,
        PreparedStatement s,
        String e) throws SQLException {

        d.executeUpdate();
        i.executeUpdate();
        JDBC.assertSingleValueResultSet(s.executeQuery(), e);
    }
}
