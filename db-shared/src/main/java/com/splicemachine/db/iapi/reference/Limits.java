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

package com.splicemachine.db.iapi.reference;

public interface Limits
{
	/**
        * Various fixed Limits. DB2 related limits are prefixed with "DB2_".
        */

	int DB2_MAX_TRIGGER_RECURSION = 16; /* Maximum nesting level for triggers */

	/** Maximum number of indexes on a table */
	int DB2_MAX_INDEXES_ON_TABLE = 32767;
	/* Maximum number of columns in a table */
	int DB2_MAX_COLUMNS_IN_TABLE =1<<17 ; //set to 131K

	/* Maximum number of columns in a view */
	int DB2_MAX_COLUMNS_IN_VIEW = 5000;

	/* Maximum number of parameters in a stored procedure */
	int DB2_MAX_PARAMS_IN_STORED_PROCEDURE = 90;

	/* Maximum number of elements in a select list */
	int DB2_MAX_ELEMENTS_IN_SELECT_LIST = DB2_MAX_COLUMNS_IN_TABLE;
	/* Maximum number of columns in a group by list */
	int DB2_MAX_ELEMENTS_IN_GROUP_BY = 32677;
	/* Maximum number of columns in an order by list */
	int DB2_MAX_ELEMENTS_IN_ORDER_BY = 1012;



	// Warning. Changing this value will affect upgrade and the creation of the
	// SQLCAMESSAGE procedure. See com.splicemachine.db.impl.sql.catalog.
	int DB2_JCC_MAX_EXCEPTION_PARAM_LENGTH = 2400;

        /* Identifiers (Constraint, Cursor, Function/Procedure, Index,
         * Trigger, Column, Schema, Savepoint, Table and View names)
         * are limited to 128 */
		int MAX_IDENTIFIER_LENGTH = 128;

	int	DB2_CHAR_MAXWIDTH = 254;
	int	DB2_VARCHAR_MAXWIDTH = 32672;
	int DB2_LOB_MAXWIDTH = 2147483647;
	int	DB2_LONGVARCHAR_MAXWIDTH = 32700;
    int DB2_CONCAT_VARCHAR_LENGTH = 4000;
	int DB2_MAX_FLOATINGPOINT_LITERAL_LENGTH = 30; // note, this value 30 is also contained in err msg 42820
	int DB2_MAX_CHARACTER_LITERAL_LENGTH = 32672;
	int DB2_MAX_HEX_LITERAL_LENGTH = 16336;

	int DB2_MIN_COL_LENGTH_FOR_CURRENT_USER = 8;
	int DB2_MIN_COL_LENGTH_FOR_CURRENT_SCHEMA = 128;

    /**
     * DB2 TABLESPACE page size limits
     */
	int DB2_MIN_PAGE_SIZE = 4096;   //  4k
    int DB2_MAX_PAGE_SIZE = 32768;  // 32k

    /**
     * DECIMAL type limits
     */

	int DB2_MAX_DECIMAL_PRECISION_SCALE = 38;
	int DB2_DEFAULT_DECIMAL_PRECISION   = 5;
	int DB2_DEFAULT_DECIMAL_SCALE       = 0;

    /**
     * REAL/DOUBLE range limits
     */

	float DB2_SMALLEST_REAL = -3.402E+38f;
    float DB2_LARGEST_REAL  = +3.402E+38f;
    float DB2_SMALLEST_POSITIVE_REAL = +1.175E-37f;
    float DB2_LARGEST_NEGATIVE_REAL  = -1.175E-37f;

    double DB2_SMALLEST_DOUBLE = -1.79769E+308d;
    double DB2_LARGEST_DOUBLE  = +1.79769E+308d;
    double DB2_SMALLEST_POSITIVE_DOUBLE = +2.225E-307d;
    double DB2_LARGEST_NEGATIVE_DOUBLE  = -2.225E-307d;

    // Limits on the length of the return values for the procedures in
    // LOBStoredProcedure.

    /**
     * The maximum length of the data returned from the BLOB stored procedures.
     * <p>
     * This value is currently dictated by the maximum length of
     * VARCHAR/VARBINARY, because these are the return types of the stored
     * procedures.
     */
    int MAX_BLOB_RETURN_LEN = Limits.DB2_VARCHAR_MAXWIDTH;

    /**
     * The maximum length of the data returned from the CLOB stored procedures.
     * <p>
     * This value is currently dictated by the maximum length of
     * VARCHAR/VARBINARY, because these are the return types of the stored
     * procedures, and the modified UTF8 encoding used for CLOB data. This
     * threshold value could be higher (equal to {@code MAX_BLOB_RETURN_LEN}),
     * but then the procedure fetching data from the CLOB must be rewritten to
     * have more logic.
     * <p>
     * For now we use the defensive assumption that all characters are
     * represented by three bytes.
     */
    int MAX_CLOB_RETURN_LEN = MAX_BLOB_RETURN_LEN / 3;
    
}
