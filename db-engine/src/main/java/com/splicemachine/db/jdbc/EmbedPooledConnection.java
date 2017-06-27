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

import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.reference.Property;
import com.splicemachine.db.iapi.error.ExceptionSeverity;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;

/* import impl class */
import com.splicemachine.db.impl.jdbc.Util;
import com.splicemachine.db.impl.jdbc.EmbedConnection;
import com.splicemachine.db.iapi.jdbc.BrokeredConnection;
import com.splicemachine.db.iapi.jdbc.BrokeredConnectionControl;
import com.splicemachine.db.iapi.jdbc.EngineConnection;
import com.splicemachine.db.impl.jdbc.EmbedPreparedStatement;
import com.splicemachine.db.impl.jdbc.EmbedCallableStatement;


import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.CallableStatement;

import java.util.ArrayList;
import java.util.Iterator;

/* -- New jdbc 20 extension types --- */
import javax.sql.ConnectionEventListener;
import javax.sql.ConnectionEvent;
import javax.sql.StatementEventListener;

/** 
	A PooledConnection object is a connection object that provides hooks for
	connection pool management.

	<P>This is Derby's implementation of a PooledConnection for use in
	the following environments:
	<UL>
	<LI> JDBC 3.0 - Java 2 - JDK 1.4, J2SE 5.0
	<LI> JDBC 2.0 - Java 2 - JDK 1.2,1.3
	</UL>

 */
class EmbedPooledConnection implements javax.sql.PooledConnection, BrokeredConnectionControl
{
    /** the connection string */
    private String connString;

    /**
     * The list of {@code ConnectionEventListener}s. It is initially {@code
     * null} and will be initialized lazily when the first listener is added.
     */
    private ArrayList eventListener;

    /**
     * The number of iterators going through the list of connection event
     * listeners at the current time. Only one thread may be iterating over the
     * list at any time (because of synchronization), but a single thread may
     * have multiple iterators if for instance an event listener performs
     * database calls that trigger a new event.
     */
    private int eventIterators;

	EmbedConnection realConnection;
	int defaultIsolationLevel;
	private boolean defaultReadOnly;
	BrokeredConnection currentConnectionHandle;

	// set up once by the data source
	final ReferenceableDataSource dataSource;
	private final String username;
	private final String password;
	/**
		True if the password was passed in on the connection request, false if it came from the data source property.
	*/
	private final boolean requestPassword;

	protected boolean isActive;
    
    /**
     * getter function for isActive
     * @return boolean is isActive is true
     **/
    public boolean isActive() {
        return isActive;
    }
    
    EmbedPooledConnection(ReferenceableDataSource ds, String u, String p,
            boolean requestPassword) throws SQLException
	{
		dataSource = ds;
		username = u;
		password = p;
		this.requestPassword = requestPassword;
		isActive = true;

		// open the connection up front in order to do authentication
		openRealConnection();

	}

	String getUsername()
	{
		if (username == null || username.isEmpty())
			return Property.DEFAULT_USER_NAME;
		else
			return username;
	}

	String getPassword()
	{
		if (password == null)
			return "";
		else
			return password;
	}


	/** 
		Create an object handle for a database connection.

		@return a Connection object

		@exception SQLException - if a database-access error occurs.
	*/
	public synchronized Connection getConnection() throws SQLException
	{
		checkActive();

		// RealConnection is not null if the app server yanks a local
		// connection from one client and give it to another.  In this case,
		// the real connection is ready to be used.  Otherwise, set it up
		if (realConnection == null)
		{
			// first time we establish a connection
			openRealConnection();
		}
		else
		{
			resetRealConnection();
		}

        // Need to do this in case the connection is forcibly removed without
        // first being closed. Must be performed after resetRealConnection(),
        // otherwise closing the logical connection may fail if the transaction
        // is not idle.
        closeCurrentConnectionHandle();

		// now make a brokered connection wrapper and give this to the user
		// we reuse the EmbedConnection(ie realConnection).
		return getNewCurrentConnectionHandle();
	}

	final void openRealConnection() throws SQLException {
		// first time we establish a connection
		Connection rc = dataSource.getConnection(username, password, requestPassword);

		this.realConnection = (EmbedConnection) rc;
		defaultIsolationLevel = rc.getTransactionIsolation();
		defaultReadOnly = rc.isReadOnly();
		if (currentConnectionHandle != null)
			realConnection.setApplicationConnection(currentConnectionHandle);
	}

	final Connection getNewCurrentConnectionHandle() throws SQLException {
		Connection applicationConnection = currentConnectionHandle =
			((Driver20) (realConnection.getLocalDriver())).newBrokeredConnection(this);
		realConnection.setApplicationConnection(applicationConnection);
		return applicationConnection;

	}

	/**
		In this case the Listeners are *not* notified. JDBC 3.0 spec section 11.4
	*/
	private void closeCurrentConnectionHandle() throws SQLException {
		if (currentConnectionHandle != null)
		{
			ArrayList tmpEventListener = eventListener;
			eventListener = null;

			try {
				currentConnectionHandle.close();
			} finally {
				eventListener = tmpEventListener;
			}

			currentConnectionHandle = null;
		}
	}

	void resetRealConnection() throws SQLException {

		// ensure any outstanding changes from the previous
		// user are rolledback.
		realConnection.rollback();

		// clear any warnings that are left over
		realConnection.clearWarnings();

		// need to reset transaction isolation, autocommit, readonly, holdability states
		if (realConnection.getTransactionIsolation() != defaultIsolationLevel) {

			realConnection.setTransactionIsolation(defaultIsolationLevel);
		}

		if (!realConnection.getAutoCommit())
			realConnection.setAutoCommit(true);

		if (realConnection.isReadOnly() != defaultReadOnly)
			realConnection.setReadOnly(defaultReadOnly);

		if (realConnection.getHoldability() != ResultSet.HOLD_CURSORS_OVER_COMMIT)
			realConnection.setHoldability(ResultSet.HOLD_CURSORS_OVER_COMMIT);

		// reset any remaining state of the connection
		realConnection.resetFromPool();
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(realConnection.transactionIsIdle(),
			"real connection should have been idle at this point"); 			
		}
	}

	/**
		Close the Pooled connection.

		@exception SQLException - if a database-access error occurs.
	 */
	public synchronized void close() throws SQLException
	{
		if (!isActive)
			return;

		closeCurrentConnectionHandle();
		try {
			if (realConnection != null) {
				if (!realConnection.isClosed())
					realConnection.close();
			}

		} finally {

			realConnection = null;	// make sure I am not accessed again.
			isActive = false;
			eventListener = null;
		}
	}

	/**
		Add an event listener.
	 */
	public final synchronized void addConnectionEventListener(ConnectionEventListener listener) 
	{
		if (!isActive)
			return;
		if (listener == null)
			return;
        if (eventListener == null) {
            eventListener = new ArrayList();
        } else if (eventIterators > 0) {
            // DERBY-3401: Someone is iterating over the ArrayList, and since
            // we were able to synchronize on this, that someone is us. Clone
            // the list of listeners in order to prevent invalidation of the
            // iterator.
            eventListener = (ArrayList) eventListener.clone();
        }
        eventListener.add(listener);
	}

	/**
		Remove an event listener.
	 */
	public final synchronized void removeConnectionEventListener(ConnectionEventListener listener)
	{
        if (listener == null || eventListener == null) {
			return;
        }
        if (eventIterators > 0) {
            // DERBY-3401: Someone is iterating over the ArrayList, and since
            // we were able to synchronize on this, that someone is us. Clone
            // the list of listeners in order to prevent invalidation of the
            // iterator.
            eventListener = (ArrayList) eventListener.clone();
        }
        eventListener.remove(listener);
	}

//	@Override
	public void addStatementEventListener(StatementEventListener listener){
		throw new UnsupportedOperationException();
	}

//	@Override
	public void removeStatementEventListener(StatementEventListener listener){
		throw new UnsupportedOperationException();
	}

	/*
	 * class specific method
	 */

	// called by ConnectionHandle when it needs to forward things to the
	// underlying connection
	public synchronized EngineConnection getRealConnection()
       throws SQLException
	{
		checkActive();

		return realConnection;
	}

    /**
     * @return The underlying language connection.
     */
	public synchronized LanguageConnectionContext getLanguageConnection()
       throws SQLException
	{
		checkActive();

		return realConnection.getLanguageConnection();
	}


	// my conneciton handle has caught an error (actually, the real connection
	// has already handled the error, we just need to nofity the listener an
	// error is about to be thrown to the app).
	public synchronized void notifyError(SQLException exception)
	{
		// only report fatal error to the connection pool manager 
		if (exception.getErrorCode() < ExceptionSeverity.SESSION_SEVERITY)
			return;

		// tell my listeners an exception is about to be thrown
        fireConnectionEventListeners(exception);
	}

    /**
     * Fire all the {@code ConnectionEventListener}s registered. Callers must
     * synchronize on {@code this} to prevent others from modifying the list of
     * listeners.
     *
     * @param exception the exception that caused the event, or {@code null} if
     * it is a close event
     */
    private void fireConnectionEventListeners(SQLException exception) {
        if (eventListener != null && !eventListener.isEmpty()) {
            ConnectionEvent event = new ConnectionEvent(this, exception);
            eventIterators++;
            try {
                for (Object anEventListener : eventListener) {
                    ConnectionEventListener l =
                            (ConnectionEventListener) anEventListener;
                    if (exception == null) {
                        l.connectionClosed(event);
                    } else {
                        l.connectionErrorOccurred(event);
                    }
                }
            } finally {
                eventIterators--;
            }
        }
    }

	final void checkActive() throws SQLException {
		if (!isActive)
			throw Util.noCurrentConnection();
	}


	/*
	** BrokeredConnectionControl api
	*/

	/**
		Returns true if isolation level has been set using either JDBC api or SQL
	 */
	public boolean isIsolationLevelSetUsingSQLorJDBC() throws SQLException {
        return realConnection != null && realConnection.getLanguageConnection().isIsolationLevelSetUsingSQLorJDBC();
    }

	/**
		Reset the isolation level flag used to keep state in 
		BrokeredConnection. It will get set to true when isolation level 
		is set using JDBC/SQL. It will get reset to false at the start
		and the end of a global transaction.
	*/
	public void resetIsolationLevelFlag() throws SQLException {
		realConnection.getLanguageConnection().resetIsolationLevelFlagUsedForSQLandJDBC();
	}

    /** @see BrokeredConnectionControl#isInGlobalTransaction() */
    public boolean isInGlobalTransaction() {
    	return false;
    }	
	
	/**
		Notify the control class that a SQLException was thrown
		during a call on one of the brokered connection's methods.
	*/
	public void notifyException(SQLException sqle) {
		this.notifyError(sqle);
	}


	/**
		Allow control over setting auto commit mode.
	*/
	public void checkAutoCommit(boolean autoCommit) throws SQLException {
	}

	/**
		Are held cursors allowed.
	*/
	public int checkHoldCursors(int holdability, boolean downgrade)
        throws SQLException
    {
        return holdability;
	}

	/**
		Allow control over creating a Savepoint (JDBC 3.0)
	*/
	public void checkSavepoint() throws SQLException {
	}

	/**
		Allow control over calling rollback.
	*/
	public void checkRollback() throws SQLException {
	}

	/**
		Allow control over calling commit.
	*/
	public void checkCommit() throws SQLException {
	}

    /** @see BrokeredConnectionControl#checkClose() */
    public void checkClose() throws SQLException {
        if (realConnection != null) {
            realConnection.checkForTransactionInProgress();
        }
    }

	/**
		Close called on BrokeredConnection. If this call
		returns true then getRealConnection().close() will be called.
		
	
	Notify listners that connection is closed.
		Don't close the underlying real connection as
		it is pooled.
	*/
	public synchronized boolean closingConnection() throws SQLException {	    
		//DERBY-2142-Null out the connection handle BEFORE notifying listeners.
		//At time of the callback the PooledConnection must be 
		//disassociated from its previous logical connection.
		//If not there is a risk that the Pooled
		//Connection could be returned to the pool, ready for pickup by a 
		//new thread. This new thread then might obtain a java.sql.Connection 
		//whose reference might get assigned to the currentConnectionHandle 
		//field, meanwhile the previous thread completes the close making 
		//the newly assigned currentConnectionHandle null, resulting in an NPE.
		currentConnectionHandle = null;
		// tell my listeners I am closed 
        fireConnectionEventListeners(null);

		return false;
	}

	/**
		No need to wrap statements for PooledConnections.
	*/
	public Statement wrapStatement(Statement s) throws SQLException {
		return s;
	}
	/**
         * Call the setBrokeredConnectionControl method inside the 
         * EmbedPreparedStatement class to set the BrokeredConnectionControl 
         * variable to this instance of EmbedPooledConnection
         * This will then be used to call the onStatementErrorOccurred
         * and onStatementClose events when the corresponding events
         * occur on the PreparedStatement
         *
         * @param  ps            PreparedStatment to be wrapped
         * @param  sql           String
         * @param  generatedKeys Object
         * @return returns the wrapped PreparedStatement
         * @throws java.sql.SQLException
	 */
	public PreparedStatement wrapStatement(PreparedStatement ps, String sql, Object generatedKeys) throws SQLException {
               /*
                    
                */
                EmbedPreparedStatement ps_ = (EmbedPreparedStatement)ps;
                ps_.setBrokeredConnectionControl(this);
		return (PreparedStatement)ps_;
	}
        
        /**
         * Call the setBrokeredConnectionControl method inside the 
         * EmbedCallableStatement class to set the BrokeredConnectionControl 
         * variable to this instance of EmbedPooledConnection
         * This will then be used to call the onStatementErrorOccurred
         * and onStatementClose events when the corresponding events
         * occur on the CallableStatement
         *
         * @param  cs            CallableStatment to be wrapped
         * @param  sql           String
         * @return returns the wrapped CallableStatement
         * @throws java.sql.SQLException
	 */
	public CallableStatement wrapStatement(CallableStatement cs, String sql) throws SQLException {
                EmbedCallableStatement cs_ = (EmbedCallableStatement)cs;
                cs_.setBrokeredConnectionControl(this);
		return (CallableStatement)cs_;
	}
    
    /** 
     * Get the string representation of this pooled connection.
     *
     * A pooled connection is assigned a separate id from a physical 
     * connection. When a container calls PooledConnection.toString(), 
     * it gets the string representation of this id. This is useful for 
     * developers implementing connection pools when they are trying to
     * debug pooled connections. 
     *
     * @return a string representation of the uniquie id for this pooled
     *    connection.
     *
     */
    public String toString()
    {
        if ( connString == null )
        {
            String physicalConnString = isActive ?
                realConnection.toString() : "<none>";
            
            connString = 
              this.getClass().getName() + "@" + this.hashCode() + " " +
                "Physical Connection = " + physicalConnString;
        }    
        
        return connString;
    }
    
    /*-----------------------------------------------------------------*/
    /*
     * These methods are from the BrokeredConnectionControl interface. 
     * These methods are needed to provide StatementEvent support for 
     * db.
     * They are actually implemented in EmbedPooledConnection40 but have
     * a dummy implementation here so that the compilation wont fail when they
     * are compiled with jdk1.4
     */
    
    /**
     * Dummy implementation for the actual methods found in 
     * com.splicemachine.db.jdbc.EmbedPooledConnection40
     * @param statement PreparedStatement
     */
    public void onStatementClose(PreparedStatement statement) {
        
    }
    
    /**
     * Dummy implementation for the actual methods found in 
     * com.splicemachine.db.jdbc.EmbedPooledConnection40
     * @param statement PreparedStatement
     * @param sqle      SQLException 
     */
    public void onStatementErrorOccurred(PreparedStatement statement,
            SQLException sqle) {
        
    }
}
