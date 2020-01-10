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

package com.splicemachine.db.authentication;

import java.util.Properties;
import java.sql.SQLException;

/**
 * The UserAuthenticator interface provides operations to authenticate
 * a user's credentials in order to successfully connect to a database.
 * 
 * Any user authentication schemes could be implemented using this interface
 * and registered at start-up time.
 * <p>
 * If an application requires its own authentication scheme, then it can
 * implement this interface and register as the authentication scheme
 * that Derby should call upon connection requests to the system.
   See the dcoumentation for the property <I>db.authentication.provider</I>
 * <p>
 * A typical example would be to implement user authentication using
 * LDAP, Sun NIS+, or even Windows User Domain, using this interface.
 * <p>
 * <i>Note</i>: Additional connection attributes can be specified on the 
 * database connection URL and/or Properties object on jdbc connection. Values
 * for these attributes can be retrieved at runtime by the (specialized)
 * authentication scheme to further help user authentication, if one needs
 * additional info other than user, password, and database name.
 *
 *
 */

public interface UserAuthenticator
{
	
	/**
	 * Authenticate a user's credentials.
     * <BR>
     * E.g. if connection url is 
     * <code>jdbc:splice:testdb;user=Fred;password=ScT7dmM2</code>
     * then the userName will be Fred and within the Derby user authorization 
     * system, Fred becomes a case-insensitive authorization identifier and 
     * is known as FRED
     *<BR>
     * if connection url is 
     * <code>jdbc:splice:testdb;user="Fred";password=ScT7dmM2</code>
     * then the userName will be "Fred" and within the Derby user authorization
     * system, Fred becomes a case-sensitive authorization identifier and is
     * known as Fred
     * <BR>
	 *
	 * @param userName		The user's name for the connection request. May be 
     *                      null.  The user name is passed in as is from the 
     *                      connection request.
     *                      Derby will pass in the user name that is set on
     *                      connection request as is, without changing the 
     *                      casing and without removing the delimiter quotes 
     *                      if any.
     *
	 * @param userPassword	The user's password for the connection request. 
     *                      May be null.
     *
	 * @param databaseName	The database that the user wants to connect to.
	 *						Will be null if this is system level authentication.
     *
	 * @param info			A Properties object that contains additional 
     *                      connection information, that can help to 
     *                      authenticate the user. It has properties of the 
     *                      'info' object passed as part of 
     *                      DriverManager.getConnection() call and any 
     *                      attributes set on the JDBC URL.
     *
     * @return	false if the connection request should be denied, true if the 
     *          connection request should proceed.  If false is returned the 
     *          connection attempt will receive a SQLException with SQL State 
     *          08004.
	 *
	 * @exception java.sql.SQLException An exception processing the request, 
     *            connection request will be denied.  The SQL exception will 
     *            be returned to the connection attempt.
	 */
	public String	authenticateUser(String userName,
								 String userPassword,
								 String databaseName,
								 Properties info
								)
		throws SQLException;
}
