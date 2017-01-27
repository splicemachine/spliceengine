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

package com.splicemachine.db.jdbc;

import java.sql.SQLException;

/* -- New jdbc 20 extension types --- */
import javax.sql.PooledConnection;

/** 
	EmbeddedConnectionPoolDataSource is Derby's ConnectionPoolDataSource
	implementation for the JDBC3.0 environment.
	

	<P>A ConnectionPoolDataSource is a factory for PooledConnection
	objects. An object that implements this interface will typically be
	registered with a JNDI service.
	<P>
	EmbeddedConnectionPoolDataSource automatically supports the correct JDBC specification version
	for the Java Virtual Machine's environment.
	<UL>
	<LI> JDBC 3.0 - Java 2 - JDK 1.4, J2SE 5.0
	</UL>

	<P>EmbeddedConnectionPoolDataSource is serializable and referenceable.

	<P>See EmbeddedDataSource for DataSource properties.

 */
public class EmbeddedConnectionPoolDataSource extends EmbeddedDataSource
		implements	javax.sql.ConnectionPoolDataSource
{

	private static final long serialVersionUID = 7852784308039674160L;

	/**
		No-arg constructor.
	 */
	public EmbeddedConnectionPoolDataSource() {
		super();
	}

	/*
	 * ConnectionPoolDataSource methods
	 */

	/**
		Attempt to establish a database connection.

		@return a Connection to the database

		@exception SQLException if a database-access error occurs.
	*/
	public final PooledConnection getPooledConnection() throws SQLException { 
		return createPooledConnection (getUser(), getPassword(), false);
	}

	/**
		Attempt to establish a database connection.

		@param username the database user on whose behalf the Connection is being made
		@param password the user's password

		@return a Connection to the database

		@exception SQLException if a database-access error occurs.
	*/
	public final PooledConnection getPooledConnection(String username, 
												String password)
		 throws SQLException
	{
		return createPooledConnection (username, password, true);
	}
        
    /**
     * Create and return an EmbedPooledConnection from this instance
     * of EmbeddedConnectionPoolDataSource.
     */
    protected PooledConnection createPooledConnection (String user,
        String password, boolean requestPassword) throws SQLException
    {
        /* This object (EmbeddedConnectionPoolDataSource) is a JDBC 2
         * and JDBC 3 implementation of ConnectionPoolDatSource.  However,
         * it's possible that we are running with a newer driver (esp.
         * JDBC 4) in which case we should return a PooledConnection that
         * implements the newer JDBC interfaces--even if "this" object
         * does not itself satisfy those interfaces.  As an example, if
         * we have a JDK 6 application then even though this specific
         * object doesn't implement JDBC 4 (it only implements JDBC 2
         * and 3), we should still return a PooledConnection object that
         * *does* implement JDBC 4 because that's what a JDK 6 app
         * expects.
         *
         * By calling "findDriver()" here we will get the appropriate
         * driver for the JDK in use (ex. if using JDK 6 then findDriver()
         * will return the JDBC 4 driver).  If we then ask the driver to
         * give us a pooled connection, we will get a connection that
         * corresponds to whatever driver/JDBC implementation is being
         * used--which is what we want.  So for a JDK 6 application we
         * will correctly return a JDBC 4 PooledConnection. DERBY-2488.
         *
         * This type of scenario can occur if an application that was
         * previously running with an older JVM (ex. JDK 1.4/1.5) starts
         * running with a newer JVM (ex. JDK 6), in which case the app
         * is probably still using the "old" data source (ex. is still
         * instantiating EmbeddedConnectionPoolDataSource) instead of
         * the newer one (EmbeddedConnectionPoolDataSource40).
         */
        return ((Driver30) findDriver()).getNewPooledConnection(
            this, user, password, requestPassword);
    }

}





