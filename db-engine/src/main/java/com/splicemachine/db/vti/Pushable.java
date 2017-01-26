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

package com.splicemachine.db.vti;

import java.sql.SQLException;

/**
	Support for pushing SQL statement information
	down into a virtual table.

  A read-write virtual tables (one that implements java.sql.PreparedStatement)
  implements this interface to support pushing information into the VTI.

  <BR>
  Read-only VTIs (those that implement java.sql.ResultSet) do not support the Pushable interface.
*/
public interface Pushable {


	/**
		Indicates the columns that must be returned by a read-write VTI's ResultSet.
		This method is called only during the runtime execution of the VTI, after it has been
		constructed and before the executeQuery() method is called.
		At compile time the VTI needs to describe the complete set of columns it can return.
		<BR>
		The column identifiers contained in projectedColumns
		map to the columns described by the VTI's PreparedStatement's
		ResultSetMetaData. The ResultSet returned by
		PreparedStatement.executeQuery() must contain
		these columns in the order given. Column 1 in this
		ResultSet maps the the column of the VTI identified
		by projectedColumns[0], column 2 maps to projectedColumns[1] etc.
		<BR>
		Any additional columns contained in the ResultSet are ignored
		by the database engine. The ResultSetMetaData returned by
		ResultSet.getMetaData() must match the ResultSet.
		<P>
		PreparedStatement's ResultSetMetaData column list {"id", "desc", "price", "tax", "brand"}
		<BR>
		projectedColumns = { 2, 3, 5}
		<BR>
		results in a ResultSet containing at least these 3 columns
		{"desc", "price", "brand"}


		The  JDBC column numbering scheme (1 based) ise used for projectedColumns.


		@exception SQLException Error processing the request.
	*/
	public boolean pushProjection(VTIEnvironment vtiEnvironment, int[] projectedColumns)
		throws SQLException;

}
