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
package com.splicemachine.dbTesting.junit;

import java.sql.*;

import junit.framework.Test;

/**
 * Base class for JDBC JUnit test decorators.
 */
public abstract class BaseJDBCTestSetup
    extends BaseTestSetup {
	
	public BaseJDBCTestSetup(Test test) {
		super(test);
	}
	
	/**
	 * Maintain a single connection to the default
	 * database, opened at the first call to getConnection.
	 * Typical setup will just require a single connection.
	 * @see BaseJDBCTestSetup#getConnection()
	 */
	private Connection conn;
	
    /**
     * Return the current configuration for the test.
     */
    public final TestConfiguration getTestConfiguration()
    {
    	return TestConfiguration.getCurrent();
    }
    
    /**
     * Obtain the connection to the default database.
     * This class maintains a single connection returned
     * by this class, it is opened on the first call to
     * this method. Subsequent calls will return the same
     * connection object unless it has been closed. In that
     * case a new connection object will be returned.
     * <P>
     * The tearDown method will close the connection if
     * it is open.
     * @see TestConfiguration#openDefaultConnection()
     */
    public final Connection getConnection() throws SQLException
    {
    	if (conn != null)
    	{
    		if (!conn.isClosed())
    			return conn;
    		conn = null;
    	}
    	return conn = getTestConfiguration().openDefaultConnection();
    }
    
    /**
     * Print debug string.
     * @param text String to print
     */
    public void println(final String text) {
        if (getTestConfiguration().isVerbose()) {
            System.out.println("DEBUG: " + text);
        }
    }
    
    /**
     * Tear down this fixture, sub-classes should call
     * super.tearDown(). This cleanups & closes the connection
     * if it is open.
     */
    protected void tearDown()
    throws java.lang.Exception
    {
        clearConnection();
    }

    /**
     * Close the default connection and null out the reference to it.
     * Typically only called from {@code tearDown()}.
     */
    void clearConnection() throws SQLException {
        JDBC.cleanup(conn);
        conn = null;
    }
}