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

import java.sql.DatabaseMetaData;
import java.sql.Types;
import com.splicemachine.db.shared.common.reference.JDBC40Translation;
import com.splicemachine.dbTesting.junit.BaseTestCase;
import com.splicemachine.dbTesting.junit.TestConfiguration;
import junit.framework.Test;

/**
 * JUnit test which checks that the constants in JDBC40Translation are
 * correct. Each constant in JDBC40Translation should have a test
 * method comparing it to the value in the java.sql interface.
 */
public class JDBC40TranslationTest extends BaseTestCase {

    public JDBC40TranslationTest(String name) {
        super(name);
    }

    public static Test suite() {
        return TestConfiguration.defaultSuite(JDBC40TranslationTest.class);
    }

    public void testDatabaseMetaDataFUNCTION_PARAMETER_UNKNOWN() {
        assertEquals(DatabaseMetaData.functionColumnUnknown,
                     JDBC40Translation.FUNCTION_PARAMETER_UNKNOWN);
    }

    public void testDatabaseMetaDataFUNCTION_PARAMETER_IN() {
        assertEquals(DatabaseMetaData.functionColumnIn,
                     JDBC40Translation.FUNCTION_PARAMETER_IN);
    }

    public void testDatabaseMetaDataFUNCTION_PARAMETER_INOUT() {
        assertEquals(DatabaseMetaData.functionColumnInOut,
                     JDBC40Translation.FUNCTION_PARAMETER_INOUT);
    }

    public void testDatabaseMetaDataFUNCTION_PARAMETER_OUT() {
        assertEquals(DatabaseMetaData.functionColumnOut,
                     JDBC40Translation.FUNCTION_PARAMETER_OUT);
    }

    public void testDatabaseMetaDataFUNCTION_RETURN() {
        assertEquals(DatabaseMetaData.functionReturn,
                     JDBC40Translation.FUNCTION_RETURN);
    }

    public void testDatabaseMetaDataFUNCTION_RESULT_UNKNOWN() {
        assertEquals(DatabaseMetaData.functionResultUnknown,
                     JDBC40Translation.FUNCTION_RESULT_UNKNOWN);
    }

    public void testDatabaseMetaDataFUNCTION_NO_TABLE() {
        assertEquals(DatabaseMetaData.functionNoTable,
                     JDBC40Translation.FUNCTION_NO_TABLE);
    }

    public void testDatabaseMetaDataFUNCTION_RETURNS_TABLE() {
        assertEquals(DatabaseMetaData.functionReturnsTable,
                     JDBC40Translation.FUNCTION_RETURNS_TABLE);
    }

    public void testDatabaseMetaDataFUNCTION_NO_NULLS() {
        assertEquals(DatabaseMetaData.functionNoNulls,
                     JDBC40Translation.FUNCTION_NO_NULLS);
    }

    public void testDatabaseMetaDataFUNCTION_NULLABLE() {
        assertEquals(DatabaseMetaData.functionNullable,
                     JDBC40Translation.FUNCTION_NULLABLE);
    }

    public void testDatabaseMetaDataFUNCTION_NULLABLE_UNKNOWN() {
        assertEquals(DatabaseMetaData.functionNullableUnknown,
                     JDBC40Translation.FUNCTION_NULLABLE_UNKNOWN);
    }

    public void testTypesNCHAR() {
        assertEquals(Types.NCHAR, JDBC40Translation.NCHAR);
    }

    public void testTypesNVARCHAR() {
        assertEquals(Types.NVARCHAR, JDBC40Translation.NVARCHAR);
    }

    public void testTypesLONGNVARCHAR() {
        assertEquals(Types.LONGNVARCHAR, JDBC40Translation.LONGNVARCHAR);
    }

    public void testTypesNCLOB() {
        assertEquals(Types.NCLOB, JDBC40Translation.NCLOB);
    }

    public void testTypesROWID() {
        assertEquals(Types.ROWID, JDBC40Translation.ROWID);
    }

    public void testTypesSQLXML() {
        assertEquals(Types.SQLXML, JDBC40Translation.SQLXML);
    }
}
