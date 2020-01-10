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
import com.splicemachine.dbTesting.junit.TestConfiguration;

/**
 *Test case for case.sql.
 */
public class MixedCaseExpressionTest extends BaseJDBCTestCase {

    public MixedCaseExpressionTest(String name) {
        super(name);
    }

    public static Test suite(){
        return TestConfiguration.defaultSuite(MixedCaseExpressionTest.class);
    }

    /**
     *  This test is for keyword case insensitivity.
     * @throws SQLException
     */
    public void testKeywordsWithMixedCase() throws SQLException{
        Statement st = createStatement();

        String sql = "cReAtE tAbLe T (x InT)";
        st.executeUpdate(sql);

        sql = "CrEaTe TaBlE s (X iNt)";
        st.executeUpdate(sql);

        sql = "iNsErT iNtO t VaLuEs (1)";
        assertEquals("Mistake in inserting table", 1, st.executeUpdate(sql));

        sql = "InSeRt InTo S vAlUeS (2)";
        assertEquals("Mistake in inserting table", 1, st.executeUpdate(sql));

        sql = "sElEcT * fRoM t";
        JDBC.assertSingleValueResultSet(st.executeQuery(sql), "1");

        sql = "SeLeCt * FrOm s";
        JDBC.assertSingleValueResultSet(st.executeQuery(sql), "2");

        dropTable("s");

        dropTable("t");
    }
}
