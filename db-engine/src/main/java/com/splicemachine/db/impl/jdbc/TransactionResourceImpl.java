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
 * All such Splice Machine modifications are Copyright 2012 - 2019 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.db.impl.jdbc;

import com.splicemachine.db.iapi.db.Database;
import com.splicemachine.db.iapi.error.ExceptionSeverity;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.Attribute;
import com.splicemachine.db.iapi.reference.Property;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.services.context.ContextManager;
import com.splicemachine.db.iapi.services.context.ContextService;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.compile.CompilerContext;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.util.IdUtil;
import com.splicemachine.db.iapi.util.InterruptStatus;
import com.splicemachine.db.iapi.util.StringUtil;
import com.splicemachine.db.jdbc.InternalDriver;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/** 
 *	An instance of a TransactionResourceImpl is a bundle of things that
 *	connects a connection to the database - it is the transaction "context" in
 *	a generic sense.  It is also the object of synchronization used by the
 *	connection object to make sure only one thread is accessing the underlying
 *	transaction and context.
 *
 *  <P>TransactionResourceImpl not only serves as a transaction "context", it
 *	also takes care of: <OL>
 *	<LI>context management: the pushing and popping of the context manager in
 *		and out of the global context service</LI>
 *	<LI>transaction demarcation: all calls to commit/abort/prepare/close a
 *		transaction must route thru the transaction resource.
 *	<LI>error handling</LI>
 *	</OL>
 *
 *  <P>The only connection that have access to the TransactionResource is the
 *  root connection, all other nested connections (called proxyConnection)
 *  accesses the TransactionResource via the root connection.  The root
 *  connection may be a plain EmbedConnection, or a DetachableConnection (in
 *  case of a XATransaction).  A nested connection must be a ProxyConnection.
 *  A proxyConnection is not detachable and can itself be a XA connection -
 *  although an XATransaction may start nested local (proxy) connections.
 *
 *	<P> this is an example of how all the objects in this package relate to each
 *		other.  In this example, the connection is nested 3 deep.  
 *		DetachableConnection.  
 *	<P><PRE>
 *
 *      lcc  cm   database  jdbcDriver
 *       ^    ^    ^         ^ 
 *       |    |    |         |
 *      |======================|
 *      | TransactionResource  |
 *      |======================|
 *             ^  |
 *             |  |
 *             |  |      |---------------rootConnection----------|
 *             |  |      |                                       |
 *             |  |      |- rootConnection-|                     |
 *             |  |      |                 |                     |
 *             |  V      V                 |                     |
 *|========================|      |=================|      |=================|
 *|    EmbedConnection     |      | EmbedConnection |      | EmbedConnection |
 *|                        |<-----|                 |<-----|                 |
 *| (DetachableConnection) |      | ProxyConnection |      | ProxyConnection |
 *|========================|      |=================|      |=================|
 *   ^                 | ^             ^                        ^
 *   |                 | |             |                        |
 *   ---rootConnection-- |             |                        |
 *                       |             |                        |
 *                       |             |                        |
 * |======================|  |======================|  |======================|
 * | ConnectionChild |  | ConnectionChild |  | ConnectionChild |
 * |                      |  |                      |  |                      |
 * |  (EmbedStatement)    |  |  (EmbedResultSet)    |  |  (...)               |
 * |======================|  |======================|  |======================|
 *
 * </PRE>
 * <P>A plain local connection <B>must</B> be attached (doubly linked with) to a
 * TransactionResource at all times.  A detachable connection can be without a
 * TransactionResource, and a TransactionResource for an XATransaction
 * (called  XATransactionResource) can be without a connection.
 *
 *
 */
public final class TransactionResourceImpl
{
	/*
	** instance variables set up in the constructor.
	*/
	// conn is only present if TR is attached to a connection
	protected ContextManager cm;
	protected ContextService csf;
	protected String username;

	private String dbname;
	private InternalDriver driver;
	private String url;
	private String drdaID;
	private String rdbIntTkn;
    private CompilerContext.DataSetProcessorType useSpark;
    private boolean skipStats;
    private double defaultSelectivityFactor;
	private String ipAddress;
	private String defaultSchema;
	// set these up after constructor, called by EmbedConnection
	protected Database database;
	protected LanguageConnectionContext lcc;
	private Properties sessionProperties;

	// Set this when LDAP has groupname
	protected List<String> groupuserlist;

	/**
	 *
	 * create a brand new connection for a brand new transaction
	 */
	TransactionResourceImpl(
							InternalDriver driver, 
							String url,
							Properties info) throws SQLException 
	{
		this.driver = driver;
		this.sessionProperties = info;
		csf = driver.getContextServiceFactory();
		dbname = InternalDriver.getDatabaseName(url, info);
		this.url = url;

		// the driver manager will push a user name
		// into the properties if its getConnection(url, string, string)
		// interface is used.  Thus, we look there first.
		// Default to SPLICE.
		username = IdUtil.getUserNameFromURLProps(info);

		drdaID = info.getProperty(Attribute.DRDAID_ATTR, null);
		rdbIntTkn = info.getProperty(Attribute.RDBINTTKN_ATTR, null);
		ipAddress = info.getProperty(Property.IP_ADDRESS, null);
		defaultSchema = info.getProperty("schema", null);
        String useSparkString = info.getProperty("useSpark",null);
        if (useSparkString != null) {
            try {
                useSpark = Boolean.parseBoolean(StringUtil.SQLToUpperCase(useSparkString))?CompilerContext.DataSetProcessorType.FORCED_SPARK:CompilerContext.DataSetProcessorType.FORCED_CONTROL;
            } catch (Exception sparkE) {
                throw new SQLException(StandardException.newException(SQLState.LANG_INVALID_FORCED_SPARK,useSparkString));
            }
        } else
            useSpark = CompilerContext.DataSetProcessorType.DEFAULT_CONTROL;

        String skipStatsString = info.getProperty("skipStats", null);
        if (skipStatsString != null) {
			try {
				skipStats = Boolean.parseBoolean(StringUtil.SQLToUpperCase(skipStatsString));
			} catch (Exception skipStatsE) {
				throw new SQLException(StandardException.newException(SQLState.LANG_INVALID_FORCED_SKIPSTATS, skipStatsString).getMessage(), SQLState.LANG_INVALID_FORCED_SKIPSTATS);
			}
		} else
			skipStats = false;

        String selectivityFactorString = info.getProperty("defaultSelectivityFactor", null);
        if (selectivityFactorString != null) {
			try {
				skipStats = true;
				defaultSelectivityFactor = Double.parseDouble(selectivityFactorString);
			} catch (Exception parseDoubleE) {
				throw new SQLException(StandardException.newException(SQLState.LANG_INVALID_SELECTIVITY, selectivityFactorString).getMessage(), SQLState.LANG_INVALID_SELECTIVITY);
			}
			if (defaultSelectivityFactor <= 0 || defaultSelectivityFactor > 1.0)
				throw new SQLException(StandardException.newException(SQLState.LANG_INVALID_SELECTIVITY, selectivityFactorString).getMessage(),
						SQLState.LANG_INVALID_SELECTIVITY);
		} else
			defaultSelectivityFactor = -1d;

		// make a new context manager for this TransactionResource

		// note that the Database API requires that the 
		// getCurrentContextManager call return the context manager
		// associated with this session.  The JDBC driver assumes
		// responsibility (for now) for tracking and installing
		// this context manager for the thread, each time a database
		// call is made.
		cm = csf.newContextManager();
	}

	/**
	 * Called only in EmbedConnection construtor.
	 * The Local Connection sets up the database in its constructor and sets it
	 * here.
	 */
	void setDatabase(Database db)
	{
		if (SanityManager.DEBUG)
			SanityManager.ASSERT(database == null, 
				"setting database when it is not null"); 

		database = db;
	}

	/*
	 * Called only in EmbedConnection constructor.  Create a new transaction
	 * by creating a lcc.
	 *
	 * The arguments are not used by this object, it is used by
	 * XATransactionResoruceImpl.  Put them here so that there is only one
	 * routine to start a local connection.
	 */
	void startTransaction() throws StandardException, SQLException
	{
		// setting up local connection
		lcc = database.setupConnection(cm, username, groupuserlist, drdaID, dbname, rdbIntTkn, useSpark,
                skipStats, defaultSelectivityFactor, ipAddress, defaultSchema, sessionProperties);
	}

	/**
	 * Return instance variables to EmbedConnection.  RESOLVE: given time, we
	 * should perhaps stop giving out reference to these things but instead use
	 * the transaction resource itself.
	 */
	InternalDriver getDriver() {
		return driver;
	}
	ContextService getCsf() {
		return  csf;
	}

	/**
	 * need to be public because it is in the XATransactionResource interface
	 */
	ContextManager getContextManager() {
		return  cm;
	}

	LanguageConnectionContext getLcc() {
		return  lcc;
	}
	String getDBName() {
		return  dbname;
	}
	String getUrl() {
		return  url;
	}
	Database getDatabase() {
		return  database;
	}

	StandardException shutdownDatabaseException() {
		StandardException se = StandardException.newException(SQLState.SHUTDOWN_DATABASE, getDBName());
		se.setReport(StandardException.REPORT_NEVER);
		return se;
	}

	/**
	 * local transaction demarcation - note that global or xa transaction
	 * cannot commit thru the connection, they can only commit thru the
	 * XAResource, which uses the xa_commit or xa_rollback interface as a 
	 * safeguard. 
	 */
	void commit() throws StandardException
	{
		lcc.userCommit();
	}		

	void rollback() throws StandardException
	{
		// lcc may be null if this is called in close.
		if (lcc != null)
			lcc.userRollback();
	}

	/*
	 * context management
	 */

	/**
	 * An error happens in the constructor, pop the context.
	 */
	void clearContextInError()
	{
		csf.resetCurrentContextManager(cm);
		csf.removeContext(cm);
		cm = null;
	}

	/**
	 * Clean up server state.
	 */
	void clearLcc()
	{
        try {
			// Session temp tables are cleaned up
            lcc.resetFromPool();
        } catch (StandardException e) {
            // log but don't throw, closing
			Util.logSQLException(e);
        }
        lcc = null;
	}

	final void setupContextStack()
	{
		if (SanityManager.DEBUG) {
			SanityManager.ASSERT(cm != null, "setting up null context manager stack");
		}

			csf.setCurrentContextManager(cm);
	}

	final void restoreContextStack() {

		if ((csf == null) || (cm == null))
			return;
		csf.resetCurrentContextManager(cm);
	}

	/*
	 * exception handling
	 */

	/**
	 * clean up the error and wrap the real exception in some SQLException.
	 */
	final SQLException handleException(Throwable thrownException,
									   boolean autoCommit,	
									   boolean rollbackOnAutoCommit)
			throws SQLException 
	{
		try {
			if (SanityManager.DEBUG)
				SanityManager.ASSERT(thrownException != null);

			/*
				just pass SQL exceptions right back. We assume that JDBC driver
				code has cleaned up sufficiently. Not passing them through would mean
				that all cleanupOnError methods would require knowledge of Utils.
			 */
			if (thrownException instanceof SQLException) {

                InterruptStatus.restoreIntrFlagIfSeen();
				return (SQLException) thrownException;

			} 

			boolean checkForShutdown = false;
			if (thrownException instanceof StandardException)
			{
				StandardException se = (StandardException) thrownException;
				int severity = se.getSeverity();
				if (severity <= ExceptionSeverity.STATEMENT_SEVERITY)
				{
					/*
					** If autocommit is on, then do a rollback
					** to release locks if requested.  We did a stmt 
					** rollback in the cleanupOnError above, but we still
					** may hold locks from the stmt.
					*/
					if (autoCommit && rollbackOnAutoCommit)
					{
						se.setSeverity(ExceptionSeverity.TRANSACTION_SEVERITY);
					}
				} else if (SQLState.CONN_INTERRUPT.equals(se.getMessageId())) {
					// an interrupt closed the connection.
					checkForShutdown = true;
				}
			}
			// if cm is null, we don't have a connection context left,
			// it was already removed.  all that's left to cleanup is
			// JDBC objects.
			if (cm!=null) {
			    //diagActive should be passed to cleanupOnError
			    //only if a session is active, Login errors are a special case where
			    // the database is active but the session is not.
				boolean sessionActive = (database != null) && database.isActive() && 
					!isLoginException(thrownException);
				boolean isShutdown = cleanupOnError(thrownException, sessionActive);
				if (checkForShutdown && isShutdown) {
					// Change the error message to be a known shutdown.
					thrownException = shutdownDatabaseException();
				}
			}

            InterruptStatus.restoreIntrFlagIfSeen();

			return wrapInSQLException(thrownException);

		} catch (Throwable t) {

            if (cm != null) { // something to let us cleanup?
                cm.cleanupOnError(t, database != null && isActive());
			}

            InterruptStatus.restoreIntrFlagIfSeen();

			/*
			   We'd rather throw the Throwable,
			   but then javac complains...
			   We assume if we are in this degenerate
			   case that it is actually a java exception
			 */
			throw wrapInSQLException(t);
			//throw t;
		}

	}

    /**
     * Determine if the exception thrown is a login exception.
     * Needed for DERBY-5427 fix to prevent inappropriate thread dumps
     * and javacores. This exception is special because it is 
     * SESSION_SEVERITY and database.isActive() is true, but the 
     * session hasn't started yet,so it is not an actual crash and 
     * should not report extended diagnostics.
     * 
     * @param thrownException
     * @return true if this is a login failure exception
     */
    private boolean isLoginException(Throwable thrownException) {
       if (thrownException instanceof StandardException) {
           ((StandardException) thrownException).getSQLState().equals(SQLState.LOGIN_FAILED);
           return true;
       }
       return false;
    }
    
    /**
     * Wrap a <code>Throwable</code> in an <code>SQLException</code>.
     *
     * @param thrownException a <code>Throwable</code>
     * @return <code>thrownException</code>, if it is an
     * <code>SQLException</code>; otherwise, an <code>SQLException</code> which
     * wraps <code>thrownException</code>
     */
	public static SQLException wrapInSQLException(Throwable thrownException) {

		if (thrownException == null)
			return null;

		if (thrownException instanceof SQLException) {
            // Server side JDBC can end up with a SQLException in the nested
            // stack. Return the exception with no wrapper.
            return (SQLException) thrownException;
		}

        if (thrownException instanceof StandardException) {

			StandardException se = (StandardException) thrownException;

            if (SQLState.CONN_INTERRUPT.equals(se.getSQLState())) {
                Thread.currentThread().interrupt();
            }

            if (se.getCause() == null) {
                // se is a single, unchained exception. Just convert it to an
                // SQLException.
                return Util.generateCsSQLException(se);
            }

            // se contains a non-empty exception chain. We want to put all of
            // the exceptions (including Java exceptions) in the next-exception
            // chain. Therefore, call wrapInSQLException() recursively to
            // convert the cause chain into a chain of SQLExceptions.
            return Util.seeNextException(se.getMessageId(),
                        se.getArguments(), wrapInSQLException(se.getCause()));
        }

        // thrownException is a Java exception
        return Util.javaException(thrownException);
	}

	/*
	 * TransactionResource methods
	 */

	String getUserName() {
		return  username;
	}

	void setGroupUserName(String user) {
		groupuserlist = Arrays.asList(user.split(","));
	}

	List<String> getGroupUserName() {
		return groupuserlist;
	}

    /**
     * clean up error and print it to db.log if diagActive is true
     * @param e the error we want to clean up
     * @param diagActive
     *        true if extended diagnostics should be considered, 
     *        false not interested of extended diagnostic information
     * @return true if the context manager is shutdown, false otherwise.
     */
    boolean cleanupOnError(Throwable e, boolean diagActive)
	{
		if (SanityManager.DEBUG)
			SanityManager.ASSERT(cm != null, "cannot cleanup on error with null context manager");

        //DERBY-4856 thread dump
        boolean result = cm.cleanupOnError(e, diagActive);
        csf.removeContext(cm);
        return result;
	}

	boolean isIdle()
	{
		// If no lcc, there is no transaction.
		return (lcc == null || lcc.getTransactionExecute().isIdle());
	}


	/*
	 * class specific methods
	 */


	/* 
	 * is the underlaying database still active?
	 */
	boolean isActive()
	{
		// database is null at connection open time
		return (driver.isActive() && ((database == null) || database.isActive()));
	}

	// Indicate whether the client whose transaction this is
	// supports reading of decimals with 38 digits of precision.
	public void setClientSupportsDecimal38(boolean newVal) {
		if (lcc != null)
			lcc.setClientSupportsDecimal38(newVal);
    	}
	public String getIpAddress() {
		return ipAddress;
	}
}


