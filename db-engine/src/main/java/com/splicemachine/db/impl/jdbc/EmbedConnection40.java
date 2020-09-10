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

package com.splicemachine.db.impl.jdbc;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.jdbc.EngineConnection40;
import com.splicemachine.db.iapi.jdbc.FailedProperties40;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.jdbc.InternalDriver;

import java.sql.*;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

public class EmbedConnection40
        extends EmbedConnection30 implements EngineConnection40 {
    
    /** Creates a new instance of EmbedConnection40 */
    public EmbedConnection40(EmbedConnection inputConnection) {
        super(inputConnection);
    }
    
    public EmbedConnection40(
        InternalDriver driver,
        String url,
        Properties info)
        throws SQLException {
        super(driver, url, info);
    }
    
    /*
     *-------------------------------------------------------
     * JDBC 4.0
     *-------------------------------------------------------
     */
    
    public Array createArrayOf(String typeName, Object[] elements)
        throws SQLException {
        throw Util.notImplemented();
    }
    
    public NClob createNClob() throws SQLException {
        throw Util.notImplemented();
    }
    
    public SQLXML createSQLXML() throws SQLException {
        throw Util.notImplemented();
    }
    
    public Struct createStruct(String typeName, Object[] attributes)
        throws SQLException {
        throw Util.notImplemented();
    }
    
    /**
     * Checks if the connection has not been closed and is still valid. 
     * The validity is checked by checking that the connection is not closed.
     *
     * @param timeout This should be the time in seconds to wait for the 
     * database operation used to validate the connection to complete 
     * (according to the JDBC4 JavaDoc). This is currently not supported/used.
     *
     * @return true if the connection is valid, false otherwise
     * @exception SQLException if the parameter value is illegal or if a
     * database error has occured
     */
    public boolean isValid(int timeout) throws SQLException {
        // Validate that the timeout has a legal value
        if (timeout < 0) {
            throw Util.generateCsSQLException(SQLState.INVALID_API_PARAMETER,
                    timeout, "timeout",
                                              "java.sql.Connection.isValid");
        }

        // Use the closed status for the connection to determine if the
        // connection is valid or not
        return !isClosed();
    }

    /**
     * <code>setClientInfo</code> will always throw a
     * <code>SQLClientInfoException</code> since Derby does not support
     * any properties.
     *
     * @param name a property key <code>String</code>
     * @param value a property value <code>String</code>
     * @exception SQLClientInfoException unless both name and value are null
     */
    public void setClientInfo(String name, String value)
    throws SQLClientInfoException{
        Properties p = FailedProperties40.makeProperties(name,value);
        try { checkIfClosed(); }
        catch (SQLException se) {
            FailedProperties40 fp = new FailedProperties40(p);
            throw new SQLClientInfoException(se.getMessage(), 
                                             se.getSQLState(), 
                                             se.getErrorCode(),
                                             fp.getProperties());
        }
        // Allow null to simplify compliance testing through
        // reflection, (test all methods in an interface with null
        // arguments)
        if (name == null && value == null) {
            return;
        }
        setClientInfo(p);
    }
    
    /**
     * <code>setClientInfo</code> will throw a
     * <code>SQLClientInfoException</code> uless the <code>properties</code>
     * paramenter is empty, since Derby does not support any
     * properties. All the property keys in the
     * <code>properties</code> parameter are added to failedProperties
     * of the exception thrown, with REASON_UNKNOWN_PROPERTY as the
     * value. 
     *
     * @param properties a <code>Properties</code> object with the
     * properties to set
     * @exception SQLClientInfoException unless properties parameter
     * is null or empty
     */
    public void setClientInfo(Properties properties)
    throws SQLClientInfoException {
        FailedProperties40 fp = verifyClientInfo(properties);
        
        try { checkIfClosed(); }
        catch (SQLException se) {
            throw new SQLClientInfoException(se.getMessage(), se.getSQLState(),
            		se.getErrorCode(), fp.getProperties());
        }

        // Allow properties == null to simplify compliance testing through
        // reflection (test all methods in an interface with null arguments).
        // An empty properties object is meaningless, but allowed.

        if (!fp.isEmpty()) {
            StandardException se = StandardException.newException(SQLState.PROPERTY_UNSUPPORTED_CHANGE,
                    fp.getFirstKey(), fp.getFirstValue());
            throw new SQLClientInfoException(se.getMessage(),
                    se.getSQLState(),
                    se.getErrorCode(),
                    fp.getProperties());
        }
    }
    
    /**
     * <code>getClientInfo</code> always returns a
     * <code>null String</code> since Derby doesn't support
     * ClientInfoProperties.
     *
     * @param name a <code>String</code> value
     * @return a <code>null String</code> value
     * @exception SQLException if the connection is closed.
     */
    public String getClientInfo(String name)
    throws SQLException{
        checkIfClosed();
        return null;
    }
    
    /**
     * <code>getClientInfo</code> always returns an empty
     * <code>Properties</code> object since Derby doesn't support
     * ClientInfoProperties.
     *
     * @return an empty <code>Properties</code> object
     * @exception SQLException if the connection is closed.
     */
    public Properties getClientInfo()
    throws SQLException{
        checkIfClosed();
        return new Properties();
    }

    /**
     * Returns the type map for this connection.
     *
     * @return type map for this connection
     * @exception SQLException if a database access error occurs
     */
    @SuppressWarnings("unchecked")
    public final Map<String, Class<?>> getTypeMap() throws SQLException {
        // Return the map from the super class. The method is overridden
        // just to get the generic signature and prevent an unchecked warning
        // at compile time.
        return super.getTypeMap();
    }
    
    /**
     * Returns false unless <code>interfaces</code> is implemented 
     * 
     * @param  interfaces             a Class defining an interface.
     * @return true                   if this implements the interface or 
     *                                directly or indirectly wraps an object 
     *                                that does.
     * @throws java.sql.SQLException  if an error occurs while determining 
     *                                whether this is a wrapper for an object 
     *                                with the given interface.
     */
    public boolean isWrapperFor(Class<?> interfaces) throws SQLException {
        checkIfClosed();
        return interfaces.isInstance(this);
    }
    
    /**
     * Returns <code>this</code> if this class implements the interface
     *
     * @param  interfaces a Class defining an interface
     * @return an object that implements the interface
     * @throws java.sql.SQLExption if no object if found that implements the 
     * interface
     */
    public <T> T unwrap(java.lang.Class<T> interfaces) 
                            throws SQLException{
        checkIfClosed();
        //Derby does not implement non-standard methods on 
        //JDBC objects
        //hence return this if this class implements the interface 
        //or throw an SQLException
        try {
            return interfaces.cast(this);
        } catch (ClassCastException cce) {
            throw newSQLException(SQLState.UNABLE_TO_UNWRAP,interfaces);
        }
    }
    
    ////////////////////////////////////////////////////////////////////
    //
    // INTRODUCED BY JDBC 4.1 IN JAVA 7
    //
    ////////////////////////////////////////////////////////////////////

    public  void    abort( Executor executor )  throws SQLException
    {
        // NOP if called on a closed connection.
        if ( isClosed() ) { return; }
        // Null executor not allowed.
        if ( executor == null )
        {
            throw newSQLException( SQLState.UU_INVALID_PARAMETER, "executor", "null" );
        }

        //
        // Must have privilege to invoke this method.
        //
        // The db jars should be granted this permission. We deliberately
        // do not wrap this check in an AccessController.doPrivileged() block.
        // If we did so, that would absolve outer code blocks of the need to
        // have this permission granted to them too. It is critical that the
        // outer code blocks enjoy this privilege. That is what allows
        // connection pools to prevent ordinary code from calling abort()
        // and restrict its usage to privileged tools.
        //
        SecurityManager securityManager = System.getSecurityManager();
        if ( securityManager != null )
        { securityManager.checkPermission( new SQLPermission( "callAbort" ) ); }

        // Mark the Connection as closed. Set the "aborting" flag to allow internal
        // processing in close() to proceed.
        beginAborting();

        //
        // Now pass the Executor a Runnable which does the real work.
        //
        executor.execute
            (
             new Runnable()
             {
                 public void run()
                 {
                     try {
                         rollback();
                         close(exceptionClose);
                     } catch (SQLException se) { Util.logSQLException( se ); }
                 }
             }
             );
    }
    
    public int getNetworkTimeout() throws SQLException
    {
        throw Util.notImplemented();
    }
    
    public void setNetworkTimeout( Executor executor, int milliseconds ) throws SQLException
    {
        throw Util.notImplemented();
    }

    protected FailedProperties40 verifyClientInfo(Properties properties) {
        FailedProperties40 badProperties = new FailedProperties40(null);
        if (properties == null)
            return badProperties;

        for (Object k : properties.keySet()) {
            String key = (String)k;
            switch (key) {
                case "ApplicationName":
                case "ClientUser":
                case "ClientHostname":
                    break;
                default:
                    badProperties.addProperty(key, properties.getProperty(key),
                            ClientInfoStatus.REASON_UNKNOWN_PROPERTY);
                    break;
            }
        }
        return badProperties;
    }
}
