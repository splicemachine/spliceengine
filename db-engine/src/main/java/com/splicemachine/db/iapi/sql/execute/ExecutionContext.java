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

package com.splicemachine.db.iapi.sql.execute;

import com.splicemachine.db.iapi.services.context.Context;

/**
 * ExecutionContext stores the factories that are to be used by
 * the current connection. It also provides execution services
 * for statement atomicity.
 *
 */
public interface ExecutionContext extends Context {

	/**
	 * this is the ID we expect execution contexts
	 * to be stored into a context manager under.
	 */
	String CONTEXT_ID = "ExecutionContext";
	
	
	/* Constants for scan isolation levels. */
	int UNSPECIFIED_ISOLATION_LEVEL = 0;
	int READ_UNCOMMITTED_ISOLATION_LEVEL = 1;
	int READ_COMMITTED_ISOLATION_LEVEL = 2;
	int REPEATABLE_READ_ISOLATION_LEVEL = 3;
	int SERIALIZABLE_ISOLATION_LEVEL = 4;

    /**
     * Map from Derby transaction isolation constants to
     * JDBC constants.
     */
	int[] CS_TO_JDBC_ISOLATION_LEVEL_MAP = {
		java.sql.Connection.TRANSACTION_NONE,				// UNSPECIFIED_ISOLATION_LEVEL
		java.sql.Connection.TRANSACTION_READ_UNCOMMITTED,	// READ_UNCOMMITTED_ISOLATION_LEVEL
		java.sql.Connection.TRANSACTION_READ_COMMITTED,		// READ_COMMITTED_ISOLATION_LEVEL
		java.sql.Connection.TRANSACTION_REPEATABLE_READ,	// REPEATABLE_READ_ISOLATION_LEVEL		
		java.sql.Connection.TRANSACTION_SERIALIZABLE		// SERIALIZABLE_ISOLATION_LEVEL
	};

    /**
     * Map from Derby transaction isolation constants to
     * text values used in SQL. Note that the text
     * "REPEATABLE READ" or "RR" maps to SERIALIZABLE_ISOLATION_LEVEL
     * as a hang over from DB2 compatibility and now to preserve
     * backwards compatability.
     */
	String[][] CS_TO_SQL_ISOLATION_MAP = {
		{ "  "},					// UNSPECIFIED_ISOLATION_LEVEL
		{ "UR", "DIRTY READ", "READ UNCOMMITTED"},
		{ "CS", "CURSOR STABILITY", "READ COMMITTED"},
		{ "RS"},		// read stability	
		{ "RR", "REPEATABLE READ", "SERIALIZABLE"}
	};

	/**
	 * Get the ExecutionFactory from this ExecutionContext.
	 *
	 * @return	The Execution factory associated with this
	 *		ExecutionContext
	 */
	ExecutionFactory getExecutionFactory();
}
