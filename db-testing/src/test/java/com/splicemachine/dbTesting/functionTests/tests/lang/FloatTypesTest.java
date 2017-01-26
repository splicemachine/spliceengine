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

import java.sql.PreparedStatement;
import java.sql.SQLException;
import junit.framework.Test;
import com.splicemachine.dbTesting.junit.BaseJDBCTestCase;
import com.splicemachine.dbTesting.junit.JDBC;
import com.splicemachine.dbTesting.junit.TestConfiguration;

/**
 * Test floating point data types.
 */
public class FloatTypesTest extends BaseJDBCTestCase {

    public FloatTypesTest(String name) {
        super(name);
    }

    public static Test suite() {
        return TestConfiguration.defaultSuite(FloatTypesTest.class);
    }

    /**
     * Test that normalization of negative zero to positive zero works for
     * floats. In some JVMs this used to give wrong results after runtime
     * optimization. See DERBY-2447 and
     * <a href="http://bugs.sun.com/view_bug.do?bug_id=6833879">CR6833879</a>
     * in Sun's bug database.
     */
    public void testNegativeZeroFloatJvmBug() throws SQLException {
        PreparedStatement ps = prepareStatement("values -cast(? as real)");
        ps.setFloat(1, 0.0f);
        // Execute the statement many times so that the JVM is likely to
        // produce native, optimized code.
        for (int i = 0; i < 7000; i++) {
            JDBC.assertSingleValueResultSet(ps.executeQuery(), "0.0");
        }
    }

    /**
     * Test that normalization of negative zero to positive zero works for
     * doubles. In some JVMs this used to give wrong results after runtime
     * optimization. See DERBY-2447 and
     * <a href="http://bugs.sun.com/view_bug.do?bug_id=6833879">CR6833879</a>
     * in Sun's bug database.
     */
    public void testNegativeZeroDoubleJvmBug() throws SQLException {
        PreparedStatement ps = prepareStatement("values -cast(? as double)");
        ps.setDouble(1, 0.0d);
        // Execute the statement many times so that the JVM is likely to
        // produce native, optimized code.
        for (int i = 0; i < 7000; i++) {
            JDBC.assertSingleValueResultSet(ps.executeQuery(), "0.0");
        }
    }
}
