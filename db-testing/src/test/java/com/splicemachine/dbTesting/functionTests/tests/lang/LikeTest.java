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
 * All such Splice Machine modifications are Copyright 2012 - 2020 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.dbTesting.functionTests.tests.lang;

import java.sql.SQLException;
import java.sql.Statement;
import junit.framework.Test;
import com.splicemachine.dbTesting.junit.BaseJDBCTestCase;
import com.splicemachine.dbTesting.junit.JDBC;
import com.splicemachine.dbTesting.junit.SQLUtilities;
import com.splicemachine.dbTesting.junit.TestConfiguration;

/**
 * Tests for statements with a LIKE clause.
 */
public class LikeTest extends BaseJDBCTestCase {
    public LikeTest(String name) {
        super(name);
    }

    public static Test suite() {
        return TestConfiguration.defaultSuite(LikeTest.class);
    }

    /**
     * Test that LIKE expressions are optimized and use indexes to limit the
     * scan if the arguments are concatenated string literals. DERBY-4791.
     */
    public void testOptimizeConcatenatedStringLiterals() throws SQLException {
        setAutoCommit(false);
        Statement s = createStatement();
        s.execute("create table t (x varchar(128) primary key, y int)");
        s.execute("insert into t(x) values " +
                  "'abc', 'def', 'ghi', 'ab', 'de', 'gh'");
        s.execute("call SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1)");

        // Check that an optimizable LIKE predicate (one that doesn't begin
        // with a wildcard) with a string literal picks an index scan.
        String[][] expectedRows = { {"ab", null}, {"abc", null} };
        JDBC.assertUnorderedResultSet(
                s.executeQuery("select * from t where x like 'ab%'"),
                expectedRows);
        assertTrue(SQLUtilities.getRuntimeStatisticsParser(s).usedIndexScan());

        // Now do the same test, but concatenate two string literals instead
        // of using a single string literal. This should be optimized the
        // same way.
        JDBC.assertUnorderedResultSet(
                s.executeQuery("select * from t where x like 'a'||'b'||'%'"),
                expectedRows);
        assertTrue(SQLUtilities.getRuntimeStatisticsParser(s).usedIndexScan());
    }
}
