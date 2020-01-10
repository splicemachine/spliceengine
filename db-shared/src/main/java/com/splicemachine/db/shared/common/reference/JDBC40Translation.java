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

package com.splicemachine.db.shared.common.reference;

/**
        This class contains public statics that map directly to the
        new public statics in the jdbc 4.0 interfaces.  By providing
        an intermediary class with hard-coded copies of constants that
        will be available in jdbc 4.0, it becomes possible to refer to
        these constants when compiling against older jdk versions.

        <P>The test <code>jdbc4/JDBC40TranslationTest.junit</code>,
        which is compiled against jdk16, contains tests that verifies
        that these hard coded constants are in fact equal to those
        found in jdk16.

        <P>
        This class should not be shipped with the product.

        <P>
        This class has no methods, all it contains are constants
        are public, static and final since they are declared in an interface.
*/

public interface JDBC40Translation {
    /*
    ** public statics from 4.0 version of java.sql.DatabaseMetaData
    */
    int FUNCTION_PARAMETER_UNKNOWN = 0;
    int FUNCTION_PARAMETER_IN      = 1;
    int FUNCTION_PARAMETER_INOUT   = 2;
    int FUNCTION_PARAMETER_OUT     = 3;
    int FUNCTION_RETURN            = 4;
    int FUNCTION_COLUMN_RESULT            = 5;
    
    int FUNCTION_NO_NULLS          = 0;
    int FUNCTION_NULLABLE          = 1;
    int FUNCTION_NULLABLE_UNKNOWN  = 2;

    int FUNCTION_RESULT_UNKNOWN          = 0;
    int FUNCTION_NO_TABLE          = 1;
    int FUNCTION_RETURNS_TABLE  = 2;

    // constants from java.sql.Types
    int NCHAR = -15;
    int NVARCHAR = -9;
    int LONGNVARCHAR = -16;
    int NCLOB = 2011;
    int ROWID = -8;
    int SQLXML = 2009;
}
