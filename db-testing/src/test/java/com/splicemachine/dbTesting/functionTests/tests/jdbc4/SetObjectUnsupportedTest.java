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

package com.splicemachine.dbTesting.functionTests.tests.jdbc4;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Types;
import junit.framework.Test;
import junit.framework.TestSuite;

import com.splicemachine.dbTesting.junit.BaseJDBCTestCase;
import com.splicemachine.dbTesting.junit.TestConfiguration;

/**
 * Tests that calling <code>setObject()</code> and <code>setNull()</code> with
 * <code>sqlTargetType</code> set to an unsupported type fails with
 * <code>SQLFeatureNotSupportedException</code>.
 */
public class SetObjectUnsupportedTest extends BaseJDBCTestCase {
    /** Name and id of the target type used in the test. */
    private final TypeInfo typeInfo;
    /** Flag indicating whether the test should use a
     * CallableStatement instead of a PreparedStatement. */
    private final boolean callable;

    /**
     * Creates a new <code>SetObjectUnsupportedTest</code> instance.
     *
     * @param name name of the test
     * @param typeInfo description of the target type to use in the test
     * @param callable if <code>true</code>, use a
     * <code>CallableStatement</code> instead of a
     * <code>PreparedStatement</code>.
     */
    private SetObjectUnsupportedTest(String name, TypeInfo typeInfo,
                                     boolean callable) {
        super(name);
        this.typeInfo = typeInfo;
        this.callable = callable;
    }

    /**
     * Returns the name of the test.
     */
    public String getName() {
        return super.getName() + "F" + typeInfo.name;
    }

    /**
     * Prepares a <code>PreparedStatement</code> or a
     * <code>CallableStatement</code> to use in the test.
     *
     * @return a statement (prepared or callable)
     * @exception SQLException if a database error occurs
     */
    private PreparedStatement prepare() throws SQLException {
        String sql = "values (CAST (? AS VARCHAR(128)))";
        return callable ? prepareCall(sql) : prepareStatement(sql);
    }

    /**
     * Test that <code>setObject()</code> with the specified
     * <code>sqlTargetType</code> throws
     * <code>SQLFeatureNotSupportedException</code>.
     *
     * @exception SQLException if a database error occurs
     */
    public void testUnsupportedSetObject() throws SQLException {
        PreparedStatement ps = prepare();
        try {
            ps.setObject(1, null, typeInfo.type);
            fail("No exception thrown.");
        } catch (SQLFeatureNotSupportedException e) {
            // expected exception
        }
        ps.close();
    }

    /**
     * Test that <code>setObject()</code> with the specified
     * <code>sqlTargetType</code> throws
     * <code>SQLFeatureNotSupportedException</code>.
     *
     * @exception SQLException if a database error occurs
     */
    public void testUnsupportedSetObjectWithScale() throws SQLException {
        PreparedStatement ps = prepare();
        try {
            ps.setObject(1, null, typeInfo.type, 0);
            fail("No exception thrown.");
        } catch (SQLFeatureNotSupportedException e) {
            // expected exception
        }
        ps.close();
    }

    /**
     * Test that <code>setNull()</code> with the specified
     * <code>sqlTargetType</code> throws
     * <code>SQLFeatureNotSupportedException</code>.
     *
     * @exception SQLException if a database error occurs
     */
    public void testUnsupportedSetNull() throws SQLException {
        PreparedStatement ps = prepare();
        try {
            ps.setNull(1, typeInfo.type);
            fail("No exception thrown.");
        } catch (SQLFeatureNotSupportedException e) {
            // expected exception
        }
        ps.close();
    }

    /**
     * Test that <code>setNull()</code> with the specified
     * <code>sqlTargetType</code> throws
     * <code>SQLFeatureNotSupportedException</code>.
     *
     * @exception SQLException if a database error occurs
     */
    public void testUnsupportedSetNullWithTypeName() throws SQLException {
        PreparedStatement ps = prepare();
        try {
            ps.setNull(1, typeInfo.type, typeInfo.name);
            fail("No exception thrown.");
        } catch (SQLFeatureNotSupportedException e) {
            // expected exception
        }
        ps.close();
    }

    /**
     * The target types to test.
     */
    private static final TypeInfo[] TYPES = {
        new TypeInfo("ARRAY", Types.ARRAY),
        new TypeInfo("DATALINK", Types.DATALINK),
        new TypeInfo("NCHAR", Types.NCHAR),
        new TypeInfo("NCLOB", Types.NCLOB),
        new TypeInfo("NVARCHAR", Types.NVARCHAR),
        new TypeInfo("LONGNVARCHAR", Types.LONGNVARCHAR),
        new TypeInfo("REF", Types.REF),
        new TypeInfo("ROWID", Types.ROWID),
        new TypeInfo("SQLXML", Types.SQLXML),
        new TypeInfo("STRUCT", Types.STRUCT),
    };

    /**
     * Create a suite with all tests.
     */
    public static Test suite() {
        TestSuite suite = new TestSuite("SetObjectUnsupportedTest suite");
        suite.addTest(baseSuite(false, "SetObjectUnsupportedTest:prepared"));
        suite.addTest(baseSuite(true, "SetObjectUnsupportedTest:callable"));
        TestSuite client = new TestSuite("SetObjectUnsupportedTest:client");
        client.addTest(baseSuite(false, "SetObjectUnsupportedTest:prepared"));
        client.addTest(baseSuite(true, "SetObjectUnsupportedTest:callable"));
        suite.addTest(TestConfiguration.clientServerDecorator(client));
        return suite;
    }

    /**
     * Build a test suite which tests <code>setObject()</code> with
     * each of the types in <code>TYPES</code>.
     *
     * @param callable if <code>true</code>, test with a
     * <code>CallableStatement</code>; otherwise, test with a
     * <code>PreparedStatement</code>
     * @return a test suite
     */
    static Test baseSuite(boolean callable, String name) {
        TestSuite suite = new TestSuite(name);
        for (TypeInfo typeInfo : TYPES) {
            suite.addTest(new SetObjectUnsupportedTest
                          ("testUnsupportedSetObject", typeInfo, callable));
            suite.addTest(new SetObjectUnsupportedTest
                          ("testUnsupportedSetObjectWithScale",
                           typeInfo, callable));
            suite.addTest(new SetObjectUnsupportedTest
                          ("testUnsupportedSetNull", typeInfo, callable));
            suite.addTest(new SetObjectUnsupportedTest
                          ("testUnsupportedSetNullWithTypeName",
                           typeInfo, callable));
        }
        return suite;
    }

    /** Class with name and id for the target type used in a test. */
    private static class TypeInfo {
        final String name;
        final int type;
        TypeInfo(String name, int type) {
            this.name = name;
            this.type = type;
        }
    }
}
