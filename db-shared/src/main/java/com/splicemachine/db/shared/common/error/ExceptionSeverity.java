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

package com.splicemachine.db.shared.common.error;

/**
 * Severity constants for SQLExceptions.
 * 
 * These constants are used in the ErrorCode available on a SQLException
 * to provide information about the severity of the error.
 *
 * @see java.sql.SQLException
 */
public interface ExceptionSeverity
{
	/*
	 * Use NO_APPLICABLE_SEVERITY for internal errors and unit
	 * tests that don't need to report or worry about severities.
	 */
	/**
	 * NO_APPLICABLE_SEVERITY occurs only when the system was
	 * unable to determine the severity.
	 */
	public static final int NO_APPLICABLE_SEVERITY = 0;
	/**
	 * WARNING_SEVERITY is associated with SQLWarnings.
	 */
	public static final int WARNING_SEVERITY = 10000;
	/**
	 * STATEMENT_SEVERITY is associated with errors which
	 * cause only the current statement to be aborted.
	 */
	public static final int STATEMENT_SEVERITY = 20000;
	/**
	 * TRANSACTION_SEVERITY is associated with those errors which
	 * cause the current transaction to be aborted.
	 */
	public static final int TRANSACTION_SEVERITY = 30000;
	/**
	 * SESSION_SEVERITY is associated with errors which
	 * cause the current connection to be closed.
	 */
	public static final int SESSION_SEVERITY = 40000;
	/**
	 * DATABASE_SEVERITY is associated with errors which
	 * cause the current database to be closed.
	 */
	public static final int DATABASE_SEVERITY = 45000;
	/**
	 * SYSTEM_SEVERITY is associated with internal errors which
	 * cause the system to shut down.
	 */
	public static final int SYSTEM_SEVERITY = 50000;
}


