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

package com.splicemachine.db.impl.jdbc;

import java.sql.ParameterMetaData;
import java.sql.SQLException;


/**
 * This class extends the EmbedCallableStatement20
 * in order to support new methods and classes that come with JDBC 3.0.

  <P><B>Supports</B>
   <UL>
   <LI> JDBC 3.0 - dependency on java.sql.ParameterMetaData introduced in JDBC 3.0 
   </UL>

  *
 * @see EmbedCallableStatement
 *
 */
public class EmbedCallableStatement30 extends EmbedCallableStatement20
{

	//////////////////////////////////////////////////////////////
	//
	// CONSTRUCTORS
	//
	//////////////////////////////////////////////////////////////
	public EmbedCallableStatement30 (EmbedConnection conn, String sql,
								   int resultSetType,
								   int resultSetConcurrency,
								   int resultSetHoldability)
		throws SQLException
	{
		super(conn, sql, resultSetType, resultSetConcurrency, resultSetHoldability);
	}

	/*
	 * Note: all the JDBC 3.0 Prepared statement methods are duplicated
	 * in here because this class inherits from Local20/EmbedCallableStatement, which
	 * inherits from Local/EmbedCallableStatement.  This class should inherit from a
	 * local30/PreparedStatement.  Since java does not allow multiple inheritance,
	 * duplicate the code here.
	 */

	/**
    * JDBC 3.0
    *
    * Retrieves the number, types and properties of this PreparedStatement
    * object's parameters.
    *
    * @return a ParameterMetaData object that contains information about the
    * number, types and properties of this PreparedStatement object's parameters.
    * @exception SQLException if a database access error occurs
	*/
	public ParameterMetaData getParameterMetaData()
    throws SQLException
	{
		checkStatus();
		if (preparedStatement == null)
			return null;
		
		return new EmbedParameterMetaData30( getParms(), preparedStatement.getParameterTypes());
	}



}











