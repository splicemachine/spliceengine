/*
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
 * Splice Machine, Inc. has modified this file.
 *
 * All Splice Machine modifications are Copyright 2012 - 2020 Splice Machine, Inc.,
 * and are licensed to you under the License; you may not use this file except in
 * compliance with the License.
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 */

package com.splicemachine.db.client.net;

import com.splicemachine.db.client.ClientPooledConnection;
import com.splicemachine.db.client.am.ClientMessageId;
import com.splicemachine.db.client.am.FailedProperties40;
import com.splicemachine.db.client.am.SQLExceptionFactory;
import com.splicemachine.db.client.am.SqlException;
import com.splicemachine.db.shared.common.reference.SQLState;

import java.sql.*;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

public class  NetConnection40 extends com.splicemachine.db.client.net.NetConnection {
    /**
     * Prepared statement that is used each time isValid() is called on this
     * connection. The statement is created the first time isValid is called
     * and closed when the connection is closed (by the close call).
     */
    private PreparedStatement isValidStmt = null;

    /*
     *-------------------------------------------------------
     * JDBC 4.0 
     *-------------------------------------------------------
    */

    public NetConnection40(NetLogWriter netLogWriter,
                         String databaseName,
                         java.util.Properties properties) throws SqlException {
	super(netLogWriter,databaseName,properties);
    }
    public NetConnection40(NetLogWriter netLogWriter,
                         com.splicemachine.db.jdbc.ClientBaseDataSource dataSource,
                         String user,
                         String password) throws SqlException {
	super(netLogWriter,dataSource,user,password);
    }
     public NetConnection40(NetLogWriter netLogWriter,
                         int driverManagerLoginTimeout,
                         String serverName,
                         int portNumber,
                         String databaseName,
                         java.util.Properties properties) throws SqlException{
	super(netLogWriter,driverManagerLoginTimeout,serverName,portNumber,databaseName,properties);
     }
     public NetConnection40(NetLogWriter netLogWriter,
                         String user,
                         String password,
                         com.splicemachine.db.jdbc.ClientBaseDataSource dataSource,
                         int rmId,
                         boolean isXAConn) throws SqlException{
	super(netLogWriter,user,password,dataSource,rmId,isXAConn);
    }
    public NetConnection40(NetLogWriter netLogWriter,
                         String ipaddr,
                         int portNumber,
                         com.splicemachine.db.jdbc.ClientBaseDataSource dataSource,
                         boolean isXAConn) throws SqlException{
        super(netLogWriter,ipaddr,portNumber,dataSource,isXAConn);
    }
    
    
    /**
     * The constructor for the NetConnection40 class which contains 
     * implementations of JDBC 4.0 specific methods in the java.sql.Connection
     * interface. This constructor is called from the ClientPooledConnection object 
     * to enable the NetConnection to pass <code>this</code> on to the associated 
     * prepared statement object thus enabling the prepared statement object 
     * to inturn  raise the statement events to the ClientPooledConnection object.
     *
     * @param netLogWriter NetLogWriter object associated with this connection.
     * @param user         user id for this connection.
     * @param password     password for this connection.
     * @param dataSource   The DataSource object passed from the PooledConnection 
     *                     object from which this constructor was called.
     * @param rmId         The Resource manager ID for XA Connections
     * @param isXAConn     true if this is a XA connection.
     * @param cpc          The ClientPooledConnection object from which this 
     *                     NetConnection constructor was called. This is used
     *                     to pass StatementEvents back to the pooledConnection
     *                     object.
     * @throws             SqlException
     */
    
    public NetConnection40(NetLogWriter netLogWriter,
                         String user,
                         String password,
                         com.splicemachine.db.jdbc.ClientBaseDataSource dataSource,
                         int rmId,
                         boolean isXAConn,
                         ClientPooledConnection cpc) throws SqlException{
	super(netLogWriter,user,password,dataSource,rmId,isXAConn,cpc);
    }
    

    
    public Array createArrayOf(String typeName, Object[] elements)
        throws SQLException {
        throw SQLExceptionFactory.notImplemented ("createArrayOf(String,Object[])");
    }

    
    public NClob createNClob() throws SQLException {
        throw SQLExceptionFactory.notImplemented ("createNClob ()");
    }

    public SQLXML createSQLXML() throws SQLException {
        throw SQLExceptionFactory.notImplemented ("createSQLXML ()");
    }

    public Struct createStruct(String typeName, Object[] attributes)
        throws SQLException {
        throw SQLExceptionFactory.notImplemented ("createStruct(String,Object[])");
    }

    /**
     * Checks if the connection has not been closed and is still valid. 
     * The validity is checked by running a simple query against the 
     * database.
     *
     * The timeout specified by the caller is implemented as follows:
     * On the server: uses the queryTimeout functionality to make the
     * query time out on the server in case the server has problems or
     * is highly loaded.
     * On the client: uses a timeout on the socket to make sure that 
     * the client is not blocked forever in the cases where the server
     * is "hanging" or not sending the reply.
     *
     * @param timeout The time in seconds to wait for the database
     * operation used to validate the connection to complete. If the 
     * timeout period expires before the operation completes, this 
     * method returns false. A value of 0 indicates a timeout is not 
     * applied to the database operation.
     * @return true if the connection is valid, false otherwise
     * @exception SQLException if the parameter value is illegal or if a
     * database error has occured
     */
    public boolean isValid(int timeout) throws SQLException {
        // Validate that the timeout has a legal value
        if (timeout < 0) {
            throw new SqlException(agent_.logWriter_,
                               new ClientMessageId(SQLState.INVALID_API_PARAMETER),
                               timeout, "timeout",
                               "java.sql.Connection.isValid" ).getSQLException();
        }

        // Check if the connection is closed
        if (isClosed()) {
            return false;
        }

        // Do a simple query against the database
        synchronized(this) {
            try {
                // Save the current network timeout value
                int oldTimeout = netAgent_.getTimeout();

                // Set the required timeout value on the network connection
                netAgent_.setTimeout(timeout);

                // If this is the first time this method is called on this 
                // connection we prepare the query 
                if (isValidStmt == null) {
                    isValidStmt = prepareStatement("VALUES (1)");
                }

                // Set the query timeout
                isValidStmt.setQueryTimeout(timeout);

                // Run the query against the database
                ResultSet rs = isValidStmt.executeQuery();
                rs.close();

                // Restore the previous timeout value
                netAgent_.setTimeout(oldTimeout);
            } catch(SQLException e) {
                // If an SQL exception is thrown the connection is not valid,
                // we ignore the exception and return false.
                return false;
            }
	 }

        return true;  // The connection is valid
    }

    /**
     * Close the connection and release its resources. 
     * @exception SQLException if a database-access error occurs.
     */
    synchronized public void close() throws SQLException {
        // Release resources owned by the prepared statement used by isValid
        if (isValidStmt != null) {
            isValidStmt.close();
            isValidStmt = null;
        }
        synchronized (sideConnectionLock) {
            if (sideConnection_ != null) {
                try {
                    sideConnection_.close();
                } catch (Exception e) {
                  // ignore, continue closing
                }
            }
        }
        super.close();
    }

    /**
     * <code>setClientInfo</code> will always throw a
     * <code>SQLClientInfoException</code> since Derby does not support
     * any properties.
     *
     * @param name a property key <code>String</code>
     * @param value a property value <code>String</code>
     * @exception SQLException always.
     */
    public void setClientInfo(String name, String value)
    throws SQLClientInfoException{
        Properties p = FailedProperties40.makeProperties(name,value); 
	try { checkForClosedConnection(); }
	catch (SqlException se) {
            throw new SQLClientInfoException
                (se.getMessage(), se.getSQLState(),
                		se.getErrorCode(),
                		new FailedProperties40(p).getProperties());
        }

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
     * properties to set.
     * @exception SQLClientInfoException unless the properties
     * parameter is null or empty.
     */
    public void setClientInfo(Properties properties)
    throws SQLClientInfoException {
	FailedProperties40 fp = verifyClientInfo(properties);
	try { checkForClosedConnection(); } 
	catch (SqlException se) {
	    throw new SQLClientInfoException(se.getMessage(), se.getSQLState(),
	    		se.getErrorCode(),
	    		fp.getProperties());
	}

	if (!fp.isEmpty()) {
        SqlException se = new SqlException(agent_.logWriter_,
                new ClientMessageId(SQLState.PROPERTY_UNSUPPORTED_CHANGE),
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
	try { 
	    checkForClosedConnection(); 
	    return null;
	}
	catch (SqlException se) { throw se.getSQLException(); }
    }
    
    /**
     * <code>getClientInfo</code> always returns an empty
     * <code>Properties</code> object since Derby doesn't support
     * ClientInfoProperties.
     *
     * @return an empty <code>Properties</code> object.
     * @exception SQLException if the connection is closed.
     */
    public Properties getClientInfo()
    throws SQLException{
	try {
	    checkForClosedConnection();
	    return new Properties();
	} 
	catch (SqlException se) { throw se.getSQLException(); }
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
        try {
            checkForClosedConnection();
        } catch (SqlException se) {
            throw se.getSQLException();
        }
        return interfaces.isInstance(this);
    }
    
    /**
     * Returns <code>this</code> if this class implements the interface
     *
     * @param  interfaces a Class defining an interface
     * @return an object that implements the interface
     * @throws java.sql.SQLException if no object if found that implements the 
     * interface
     */
    public <T> T unwrap(java.lang.Class<T> interfaces)
                                   throws SQLException {
        try { 
            checkForClosedConnection();
            return interfaces.cast(this);
        } catch (ClassCastException cce) {
            throw new SqlException(null,
                new ClientMessageId(SQLState.UNABLE_TO_UNWRAP),
                interfaces).getSQLException();
        } catch (SqlException se) {
            throw se.getSQLException();
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
        if ( !open_ ) { return; }
        // Null executor not allowed.
        if ( executor == null )
        {
            ClientMessageId cmi = new ClientMessageId( SQLState.UU_INVALID_PARAMETER  );
            SqlException se = new SqlException( agent_.logWriter_, cmi, "executor", "null" );

            throw se.getSQLException();
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
                    () -> {
                        try {
                            rollback();
                            close();
                        } catch (SQLException se) { se.printStackTrace( agent_.getLogWriter() ); }
                    }
            );
    }

    public int getNetworkTimeout() throws SQLException
    {
        throw SQLExceptionFactory.notImplemented ("getNetworkTimeout");
    }
    
    public void setNetworkTimeout( Executor executor, int milliseconds ) throws SQLException
    {
        throw SQLExceptionFactory.notImplemented ("setNetworkTimeout");
    }

    private Object sideConnectionLock = new Object();
    public NetConnection getSideConnection() throws SqlException {
        synchronized (sideConnectionLock) {
            if (sideConnection_ == null || !sideConnection_.isOpen())
                sideConnection_ = new NetConnection40(null, 0,
                        serverNameIP_, portNumber_, databaseName_,
                        properties_);
            return sideConnection_;
        }
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
