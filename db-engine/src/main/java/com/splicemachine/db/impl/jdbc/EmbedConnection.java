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
 * All such Splice Machine modifications are Copyright 2012 - 2018 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.db.impl.jdbc;

import com.splicemachine.db.iapi.db.Database;
import com.splicemachine.db.iapi.error.ExceptionSeverity;
import com.splicemachine.db.iapi.error.SQLWarningFactory;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.jdbc.AuthenticationService;
import com.splicemachine.db.iapi.jdbc.EngineConnection;
import com.splicemachine.db.iapi.jdbc.EngineLOB;
import com.splicemachine.db.iapi.jdbc.ExceptionFactory;
import com.splicemachine.db.iapi.reference.Attribute;
import com.splicemachine.db.iapi.reference.MessageId;
import com.splicemachine.db.iapi.reference.Property;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.services.context.ContextManager;
import com.splicemachine.db.iapi.services.i18n.MessageService;
import com.splicemachine.db.iapi.services.memory.LowMemory;
import com.splicemachine.db.iapi.services.monitor.Monitor;
import com.splicemachine.db.iapi.services.property.PropertyUtil;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.sql.execute.ExecutionContext;
import com.splicemachine.db.iapi.store.access.XATransactionController;
import com.splicemachine.db.iapi.util.InterruptStatus;
import com.splicemachine.db.impl.jdbc.authentication.NoneAuthenticationServiceImpl;
import com.splicemachine.db.jdbc.InternalDriver;
import com.splicemachine.db.security.DatabasePermission;

import java.io.IOException;
import java.security.AccessControlException;
import java.security.Permission;
import java.sql.*;
import java.util.*;

/* can't import due to name overlap:
import java.sql.Connection;
import java.sql.ResultSet;
*/

/**
 * Local implementation of Connection for a JDBC driver in 
 * the same process as the database.
 * <p> 
 * There is always a single root (parent) connection.  The
 * initial JDBC connection is the root connection. A
 * call to <I>getCurrentConnection()</I> or with the URL 
 * <I>jdbc:default:connection</I> yields a nested connection that shares
 * the same root connection as the parent.  A nested connection
 * is implemented using this class.  The nested connection copies the 
 * state of the parent connection and shares some of the same 
 * objects (e.g. ContextManager) that are shared across all
 * nesting levels.  The proxy also maintains its own
 * state that is distinct from its parent connection (e.g.
 * autocommit or warnings).
 * <p>
 * <B>SYNCHRONIZATION</B>: Just about all JDBC actions are
 * synchronized across all connections stemming from the
 * same root connection.  The synchronization is upon
 * the a synchronized object return by the rootConnection.
   <P><B>Supports</B>
   <UL>
  <LI> JDBC 2.0
   </UL>
 * 
 *
 * @see TransactionResourceImpl
 *
 */
public abstract class EmbedConnection implements EngineConnection
{

	protected static final StandardException exceptionClose = StandardException.closeException();
    public static final String INTERNAL_CONNECTION = "SPLICE_INTERNAL_CONNECTION";
    
    /**
     * Static exception to be thrown when a Connection request can not
     * be fulfilled due to lack of memory. A static exception as the lack
     * of memory would most likely cause another OutOfMemoryException and
     * if there is not enough memory to create the OOME exception then something
     * like the VM dying could occur. Simpler just to throw a static.
     */
    public static final SQLException NO_MEM =
        Util.generateCsSQLException(SQLState.LOGIN_FAILED, "java.lang.OutOfMemoryError");
    
    /**
     * Low memory state object for connection requests.
     */
    public static final LowMemory memoryState = new LowMemory();

	//////////////////////////////////////////////////////////
	// OBJECTS SHARED ACROSS CONNECTION NESTING
	//////////////////////////////////////////////////////////
	DatabaseMetaData dbMetadata;

	TransactionResourceImpl tr; // always access tr thru getTR()

	private HashMap lobHashMap = null;
	private int lobHMKey = 0;

    /**
     * Map to keep track of all the lobs associated with this
     * connection. These lobs will be cleared after the transaction
     * is no longer valid or when connection is closed
     */
    private WeakHashMap lobReferences = null;

    // Set to keep track of the open LOBFiles, so they can be closed at the end of 
    // the transaction. This would normally happen as lobReferences are freed as they
    // get garbage collected after being removed from the WeakHashMap, but it is 
    // possible that finalization will not have occurred before the user tries to 
    // remove the database (DERBY-3655).  Therefore we keep this set so that we can
    // explicitly close the files.
    private HashSet lobFiles;
    
	//////////////////////////////////////////////////////////
	// STATE (copied to new nested connections, but nesting
	// specific)
	//////////////////////////////////////////////////////////
	private boolean	active;
    private boolean aborting = false;
	boolean	autoCommit = true;
	boolean	needCommit;

	// Set to true if NONE authentication is being used
	private boolean usingNoneAuth;

	/*
     following is a new feature in JDBC3.0 where you can specify the holdability
     of a resultset at the end of the transaction. This gets set by the
	 new method setHoldability(int) in JDBC3.0
     * 
	 */
	private int	connectionHoldAbility = ResultSet.HOLD_CURSORS_OVER_COMMIT;


	//////////////////////////////////////////////////////////
	// NESTING SPECIFIC OBJECTS
	//////////////////////////////////////////////////////////
	/*
	** The root connection is the base connection upon
	** which all actions are synchronized.  By default,
	** we are the root connection unless we are created
	** by copying the state from another connection.
	*/
	final EmbedConnection rootConnection;
	private SQLWarning 		topWarning;
	/**	
		Factory for JDBC objects to be created.
	*/
	private InternalDriver factory;

	/**
		The Connection object the application is using when accessing the
		database through this connection. In most cases this will be equal
		to this. When Connection pooling is being used, then it will
		be set to the Connection object handed to the application.
		It is used for the getConnection() methods of various JDBC objects.
	*/
	private java.sql.Connection applicationConnection;

	/**
		An increasing counter to assign to a ResultSet on its creation.
		Used for ordering ResultSets returned from a procedure, always
		returned in order of their creation. Is maintained at the root connection.
	*/
	private int resultSetId;
    
    /** Cached string representation of the connection id */
    private String connString;

    private boolean internal;

    private String authUser;

	//////////////////////////////////////////////////////////
	// CONSTRUCTORS
	//////////////////////////////////////////////////////////

	// create a new Local Connection, using a new context manager
	//
	public EmbedConnection(InternalDriver driver, String url, Properties info)
		 throws SQLException
	{
		internal = info.getProperty(INTERNAL_CONNECTION) != null;
		// Create a root connection.
		applicationConnection = rootConnection = this;
		factory = driver;


		tr = new TransactionResourceImpl(driver, url, info);

		active = true;

		// register this thread and its context manager with
		// the global context service
		setupContextStack();

		try {

			// stick my context into the context manager
			EmbedConnectionContext context = pushConnectionContext(tr.getContextManager());

			// if we are shutting down don't attempt to boot or create the database
			boolean shutdown = Boolean.valueOf(info.getProperty(Attribute.SHUTDOWN_ATTR)).booleanValue();

			// see if database is already booted
			Database database = (Database) Monitor.findService(Property.DATABASE_MODULE, tr.getDBName());

			// See if user wants to create a new database.
			boolean	createBoot = createBoot(info);	

			// DERBY-2264: keeps track of whether we do a plain boot before an
			// (re)encryption or hard upgrade boot to (possibly) authenticate
			// first. We can not authenticate before we have booted, so in
			// order to enforce data base owner powers over encryption or
			// upgrade, we need a plain boot, then authenticate, then, if all
			// is well, boot with (re)encryption or upgrade.  Encryption at
			// create time is not checked.
			boolean isTwoPhaseEncryptionBoot = (!createBoot &&
												isEncryptionBoot(info));
			boolean isTwoPhaseUpgradeBoot = (!createBoot &&
											 isHardUpgradeBoot(info));
			boolean isStartSlaveBoot = isStartReplicationSlaveBoot(info);
            // Set to true if startSlave command is attempted on an
            // already booted database. Will raise an exception when
            // credentials have been verified
            boolean slaveDBAlreadyBooted = false;

            boolean isFailoverMasterBoot = false;
            final boolean dropDatabase = isDropDatabase(info);

            // Don't allow both the shutdown and the drop attribute.
            if (shutdown && dropDatabase) {
                throw newSQLException(
                        SQLState.CONFLICTING_BOOT_ATTRIBUTES,
                        Attribute.SHUTDOWN_ATTR + ", " + Attribute.DROP_ATTR);
            }

			// Save original properties if we modified them for
			// two phase encryption or upgrade boot.
			Properties savedInfo = null;

			if (database != null)
			{
				// database already booted by someone else
				tr.setDatabase(database);
				isTwoPhaseEncryptionBoot = false;
				isTwoPhaseUpgradeBoot = false;
			}
			else if (!shutdown)
			{
				if (isTwoPhaseEncryptionBoot || isTwoPhaseUpgradeBoot) {
					savedInfo = info;
					info = removePhaseTwoProps((Properties)info.clone());
				}

				// Return false iff the monitor cannot handle a service of the
				// type indicated by the proptocol within the name.  If that's
				// the case then we are the wrong driver.

				if (!bootDatabase(info, isTwoPhaseUpgradeBoot))
				{
					tr.clearContextInError();
					setInactive();
					return;
				}
			}

           if (createBoot && !shutdown && !dropDatabase)
			{
				// if we are shutting down don't attempt to boot or create the
				// database

				if (tr.getDatabase() != null) {
					addWarning(SQLWarningFactory.newSQLWarning(SQLState.DATABASE_EXISTS, getDBName()));
				} else {

                    //
					// Check for user's credential and authenticate the user
					// with the system level authentication service.
                    //
                    checkUserCredentials( true, null, info );
					
					// Process with database creation
					database = createDatabase(tr.getDBName(), info);
					tr.setDatabase(database);
				}
			}


			if (tr.getDatabase() == null) {
				handleDBNotFound();
			}


			// Check User's credentials and if it is a valid user of
			// the database
			//
            try {
                checkUserCredentials( false, tr.getDBName(), info );
            } catch (SQLException sqle) {
                if (isStartSlaveBoot && !slaveDBAlreadyBooted) {
                    // Failing credentials check on a previously
                    // unbooted db should not leave the db booted
                    // for startSlave command.

                    // tr.startTransaction is needed to get the
                    // Database context. Without this context,
                    // handleException will not shutdown the
                    // database
                    tr.startTransaction();
                    handleException(tr.shutdownDatabaseException());
                }
                throw sqle;
            }

            if (authUser != null && !tr.getUserName().equalsIgnoreCase(authUser)) {
				// If we have an authenticated group username from LDAP, use that
				tr.setGroupUserName(authUser.toUpperCase());
			}

			// Make a real connection into the database, setup lcc, tc and all
			// the rest.
			tr.startTransaction();


			// now we have the database connection, we can shut down
			if (shutdown) {
				if (!usingNoneAuth &&
						getLanguageConnection().usesSqlAuthorization()) {
					// DERBY-2264: Only allow database owner to shut down if
					// authentication and sqlAuthorization is on.
					checkIsDBOwner(OP_SHUTDOWN);
				}
				throw tr.shutdownDatabaseException();
			}

            // Drop the database at this point, if that is requested.
            if (dropDatabase) {
                if (!usingNoneAuth &&
                        getLanguageConnection().usesSqlAuthorization()) {
                    // Only the database owner is allowed to drop the database.
                    // NOTE: Reusing the message for shutdown, as drop database
                    //       includes a shutdown. May want to change this later
                    //       if/when we add system privileges.
                    checkIsDBOwner(OP_SHUTDOWN);
                }

                // TODO: If system privileges is turned on, we need to check
                // that the user has the shutdown/drop privilege. Waiting for
                // Derby-2109

                String dbName = tr.getDBName(); // Save before shutdown
                // TODO: Should block database access and sleep for a while here
                // Shut down the database.
                handleException(tr.shutdownDatabaseException());
                // Give running threads a chance to detect the shutdown.
                // Removing the service, or rather its conglomerates, too early
                // may cause a number of errors to be thrown. Try to make the
                // shutdown/drop as clean as possible.
                sleep(500L);
                Monitor.removePersistentService(dbName);
                // Generate the drop database exception here, as this is the
                // only place it will be thrown.
                StandardException se = StandardException.newException(
                    SQLState.DROP_DATABASE, dbName);
                se.setReport(StandardException.REPORT_NEVER);
                throw se;
            }

			// Raise a warning in sqlAuthorization mode if authentication is not ON
			if (usingNoneAuth && getLanguageConnection().usesSqlAuthorization())
				addWarning(SQLWarningFactory.newSQLWarning(SQLState.SQL_AUTHORIZATION_WITH_NO_AUTHENTICATION));
            InterruptStatus.restoreIntrFlagIfSeen(getLanguageConnection());
		}
        catch (OutOfMemoryError noMemory)
		{
			//System.out.println("freeA");
            InterruptStatus.restoreIntrFlagIfSeen();
			restoreContextStack();
			tr.lcc = null;
			tr.cm = null;
			
			//System.out.println("free");
			//System.out.println(Runtime.getRuntime().freeMemory());
            memoryState.setLowMemory();
			
			//noMemory.printStackTrace();
			// throw Util.generateCsSQLException(SQLState.LOGIN_FAILED, noMemory.getMessage(), noMemory);
			throw NO_MEM;
		}
		catch (Throwable t) {
            InterruptStatus.restoreIntrFlagIfSeen();

            if (t instanceof StandardException)
            {
                StandardException se = (StandardException) t;
                if (se.getSeverity() < ExceptionSeverity.SESSION_SEVERITY)
                    se.setSeverity(ExceptionSeverity.SESSION_SEVERITY);
            }
            //DERBY-4856, assume database is not up
            tr.cleanupOnError(t, false);
			throw handleException(t);
		} finally {
			restoreContextStack();
		}
	}

    /**
     * Check that a database has already been booted. Throws an exception 
     * otherwise
     *
     * @param database The database that should have been booted
     * @param operation The operation that requires that the database has 
     * already been booted, used in the exception message if not booted
     * @param dbname The name of the database that should have been booted, 
     * used in the exception message if not booted
     * @throws java.sql.SQLException thrown if database is not booted
     */
    private void checkDatabaseBooted(Database database,
                                     String operation, 
                                     String dbname) throws SQLException {
        if (database == null) {
            // Do not clear the TransactionResource context. It will
            // be restored as part of the finally clause of the constructor.
            this.setInactive();
            throw newSQLException(SQLState.REPLICATION_DB_NOT_BOOTED, 
                                  operation, dbname);
        }
    }

	/**
	  Examine the attributes set provided for illegal boot
	  combinations and determine if this is a create boot.

	  @return true iff the attribute <em>create=true</em> is provided. This
	  means create a standard database.  In other cases, returns
	  false.

	  @param p the attribute set.

	  @exception SQLException Throw if more than one of
	  <em>create</em>, <em>createFrom</em>, <em>restoreFrom</em> and
	  <em>rollForwardRecoveryFrom</em> is used simultaneously. <br>

	  Also, throw if (re)encryption is attempted with one of
	  <em>createFrom</em>, <em>restoreFrom</em> and
	  <em>rollForwardRecoveryFrom</em>.

	*/
	private boolean createBoot(Properties p) throws SQLException
	{
		int createCount = 0;

		if (Boolean.valueOf(p.getProperty(Attribute.CREATE_ATTR)).booleanValue())
			createCount++;

		int restoreCount=0;
		//check if the user has specified any /create/restore/recover from backup attributes.
		if (p.getProperty(Attribute.CREATE_FROM) != null)
			restoreCount++;
		if (p.getProperty(Attribute.RESTORE_FROM) != null)
			restoreCount++;
		if (p.getProperty(Attribute.ROLL_FORWARD_RECOVERY_FROM)!=null)
			restoreCount++;
		if(restoreCount > 1)
			throw newSQLException(SQLState.CONFLICTING_RESTORE_ATTRIBUTES);
	
        // check if user has specified re-encryption attributes in
        // combination with createFrom/restoreFrom/rollForwardRecoveryFrom
        // attributes.  Re-encryption is not
        // allowed when restoring from backup.
        if (restoreCount != 0 && isEncryptionBoot(p)) {
			throw newSQLException(SQLState.CONFLICTING_RESTORE_ATTRIBUTES);
        }


		//add the restore count to create count to make sure 
		//user has not specified and restore together by mistake.
		createCount = createCount + restoreCount ;

		//
		if (createCount > 1) throw newSQLException(SQLState.CONFLICTING_CREATE_ATTRIBUTES);
		
        // Don't allow combinations of create/restore and drop.
        if (createCount == 1 && isDropDatabase(p)) {
            // See whether we have conflicting create or restore attributes.
            String sqlState = SQLState.CONFLICTING_CREATE_ATTRIBUTES;
            if (restoreCount > 0) {
                sqlState = SQLState.CONFLICTING_RESTORE_ATTRIBUTES;
            }
            throw newSQLException(sqlState);
        }

		//retuns true only for the  create flag not for restore flags
		return (createCount - restoreCount) == 1;
	}

    private void handleDBNotFound() throws SQLException {
        String dbname = tr.getDBName();
        // do not clear the TransactionResource context. It will be restored
        // as part of the finally clause of the object creator. 
        this.setInactive();
        throw newSQLException(SQLState.DATABASE_NOT_FOUND, dbname);
    }

    /**
     * Examines the boot properties and determines if the given attributes
     * would entail dropping the database.
     *
     * @param p the attribute set
     * @return {@code true} if the drop database operation is requested,
     *      {@code false} if not.
     */
    private boolean isDropDatabase(Properties p) {
        return (Boolean.valueOf(
                    p.getProperty(Attribute.DROP_ATTR)).booleanValue());
    }

	/**
	 * Examine boot properties and determine if a boot with the given
	 * attributes would entail an encryption operation.
	 *
	 * @param p the attribute set
	 * @return true if a boot will encrypt or re-encrypt the database
	 */
	private boolean isEncryptionBoot(Properties p)
	{
		return ((Boolean.valueOf(
					 p.getProperty(Attribute.DATA_ENCRYPTION)).booleanValue()) ||
				(p.getProperty(Attribute.NEW_BOOT_PASSWORD) != null)           ||
				(p.getProperty(Attribute.NEW_CRYPTO_EXTERNAL_KEY) != null));
	}

	/**
	 * Examine boot properties and determine if a boot with the given
	 * attributes would entail a hard upgrade.
	 *
	 * @param p the attribute set
	 * @return true if a boot will hard upgrade the database
	 */
	private boolean isHardUpgradeBoot(Properties p)
	{
		return Boolean.valueOf(
			p.getProperty(Attribute.UPGRADE_ATTR)).booleanValue();
	}

    private boolean isStartReplicationSlaveBoot(Properties p) {
        return ((Boolean.valueOf(
                 p.getProperty(Attribute.REPLICATION_START_SLAVE)).
                 booleanValue()));
    }

    private boolean isStartReplicationMasterBoot(Properties p) {
        return ((Boolean.valueOf(
                 p.getProperty(Attribute.REPLICATION_START_MASTER)).
                 booleanValue()));
    }
    
    /**
     * used to verify if the failover attribute has been set.
     * 
     * @param p The attribute set.
     * @return true if the failover attribute has been set.
     *         false otherwise.
     */
    private boolean isReplicationFailover(Properties p) {
        return ((Boolean.valueOf(
                 p.getProperty(Attribute.REPLICATION_FAILOVER)).
                 booleanValue()));
    }

    private boolean isStopReplicationMasterBoot(Properties p) {
        return ((Boolean.valueOf(
                 p.getProperty(Attribute.REPLICATION_STOP_MASTER)).
                 booleanValue()));
    }
    
    /**
     * Examine the boot properties and determine if a boot with the
     * given attributes should stop slave replication mode.
     * 
     * @param p The attribute set.
     * @return true if the stopSlave attribute has been set, false
     * otherwise.
     */
    private boolean isStopReplicationSlaveBoot(Properties p) {
        return Boolean.valueOf(
               p.getProperty(Attribute.REPLICATION_STOP_SLAVE)).
               booleanValue();
    }

 	/**
	 * Remove any encryption or upgarde properties from the given properties
	 *
	 * @param p the attribute set
	 * @return clone sans encryption properties
	 */
	private Properties removePhaseTwoProps(Properties p)
	{
		p.remove(Attribute.DATA_ENCRYPTION);
		p.remove(Attribute.NEW_BOOT_PASSWORD);
		p.remove(Attribute.NEW_CRYPTO_EXTERNAL_KEY);
		p.remove(Attribute.UPGRADE_ATTR);
		return p;
	}


	/**
	 * Create a new connection based off of the 
	 * connection passed in.  Initializes state
	 * based on input connection, and copies 
	 * appropriate object pointers. This is only used
	   for nested connections.
	 *
	 * @param inputConnection the input connection
	 */
	public  EmbedConnection(EmbedConnection inputConnection)
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(inputConnection.active, 
			"trying to create a proxy for an inactive conneciton");
		}

		// Proxy connections are always autocommit false
		// thus needCommit is irrelavent.
		autoCommit = false;


		/*
		** Nesting specific state we are copying from 
		** the inputConnection
		*/

		/*
		** Objects we are sharing across nestings
		*/
		// set it to null to allow it to be final.
		tr = null;			// a proxy connection has no direct
								// pointer to the tr.  Every call has to go
								// thru the rootConnection's tr.
		active = true;
		this.rootConnection = inputConnection.rootConnection;
		this.applicationConnection = this;
		this.factory = inputConnection.factory;

		//if no holdability specified for the resultset, use the holability
		//defined for the connection
		this.connectionHoldAbility = inputConnection.connectionHoldAbility;

		//RESOLVE: although it looks like the right
		// thing to share the metadata object, if
		// we do we'll get the wrong behavior on
		// getCurrentConnection().getMetaData().isReadOnly()
		// so don't try to be smart and uncomment the
		// following.  Ultimately, the metadata should
		// be shared by all connections anyway.
		//dbMetadata = inputConnection.dbMetadata;
	}

	//
	// Check passed-in user's credentials.
	//
	private void checkUserCredentials( boolean creatingDatabase, String dbname,
									  Properties userInfo)
	  throws SQLException
	{
		if (SanityManager.DEBUG)
			SanityManager.ASSERT(!isClosed(), "connection is closed");

		// If a database name was passed-in then check user's credential
		// in that database using the database's authentication service,
		// otherwise check if it is a valid user in the JBMS system.
		//
		// NOTE: We always expect an authentication service per database
		// and one at the system level.
		//
		AuthenticationService authenticationService = null;

        try {
            // Retrieve appropriate authentication service handle
            if (dbname == null)
                authenticationService =
                    getLocalDriver().getAuthenticationService();
            else
                authenticationService =
                    getTR().getDatabase().getAuthenticationService();

        } catch (StandardException se) {
            throw Util.generateCsSQLException(se);
        }

		// check that we do have a authentication service
		// it is _always_ expected.
		if (authenticationService == null)
		{
			String failedString = MessageService.getTextMessage(
				(dbname == null) ? MessageId.AUTH_NO_SERVICE_FOR_SYSTEM : MessageId.AUTH_NO_SERVICE_FOR_DB);

			throw newSQLException(SQLState.LOGIN_FAILED, failedString);
		}

        //
        // We must handle the special case when the system uses NATIVE
        // authentication for system-wide operations but we are being
        // asked to create the system-wide credentials database. In this situation,
        // the database holding the credentials does not exist yet. In this situation,
        // we are supposed to create the credentials database and store the
        // creation credentials in that database as the credentials of the system administrator.
        //
        if (
            creatingDatabase &&
            compareDatabaseNames( getDBName(), authenticationService.getSystemCredentialsDatabaseName() )
            )
        {
            //
            // NATIVE authentication using a system-wide credentials database
            // which is being created now. Allow this to succeed. However, here we make sure that
            // the credentials are legal. This prevents the credentials db from being
            // created with a bad DBO or password.
            //
            String  user = userInfo.getProperty(Attribute.USERNAME_ATTR);
            String  password = userInfo.getProperty(Attribute.PASSWORD_ATTR);

            if ( emptyCredential( user ) || emptyCredential( password ) )
            {
                throw newSQLException( SQLState.AUTH_EMPTY_CREDENTIALS );
            }
            
            return;
        }

        //
        // If we are creating a database, we set the dbname
        //

        if (dbname != null) {
        	if (!internal)
        		checkUserIsNotARole();
		}

		// Let's authenticate now

        boolean authenticationSucceeded = true;
        try {
        		if (!internal) {
					authUser = authenticationService.authenticate(dbname, userInfo);
					authenticationSucceeded = (authUser != null) ? true : false;
				}
//        	authenticationSucceeded = true; // internal connections are allowed without authentication
        }
        catch (SQLWarning warnings)
        {
            //
            // Let the user handle the warning that her password is about to expire.
            //
            addWarning( warnings );
        }
		if ( !authenticationSucceeded )
        {
            throw newSQLException(SQLState.NET_CONNECT_AUTH_FAILED,
                     MessageService.getTextMessage(MessageId.AUTH_INVALID));
        }

		// If authentication is not on, we have to raise a warning if sqlAuthorization is ON
		// Since NoneAuthenticationService is the default for Derby, it should be ok to refer
		// to its implementation here, since it will always be present.
		if (authenticationService instanceof NoneAuthenticationServiceImpl)
			usingNoneAuth = true;

	}

    /**
     * <p>
     * Forbid empty or null usernames and passwords.
     * </p>
     */
    private boolean emptyCredential( String credential )
    {
        return ( (credential == null) || (credential.length() == 0) );
    }

    /**
     * Compare two user-specified database names to see if they identify
     * the same database.
     */
    private boolean compareDatabaseNames( String leftDBName, String rightDBName )
        throws SQLException
    {
        try {
            String  leftCanonical = Monitor.getMonitor().getCanonicalServiceName( leftDBName );
            String  rightCanonical = Monitor.getMonitor().getCanonicalServiceName( rightDBName );

            if ( leftCanonical == null ) { return false; }
            else { return leftCanonical.equals( rightCanonical ); }
            
        } catch (StandardException se) { throw Util.generateCsSQLException(se); }
    }


	/**
	 * If applicable, check that we don't connect with a user name
	 * that equals a role.
	 *
	 * @exception SQLException Will throw if the current authorization
	 *            id in {@code lcc} (which is already normalized to
	 *            case normal form - CNF) equals an existing role name
	 *            (which is also stored in CNF).
	 */
	private void checkUserIsNotARole() throws SQLException {
		TransactionResourceImpl tr = getTR();

		try {
			tr.startTransaction();
            LanguageConnectionContext lcc = tr.getLcc();
            String username = lcc.getSessionUserId();

			DataDictionary dd = lcc.getDataDictionary();

			// Check is only performed if we have db.database.sqlAuthorization == true
			if (lcc.usesSqlAuthorization()) {
				String failedString = MessageService.getTextMessage(MessageId.AUTH_INVALID);
				if (dd.getRoleDefinitionDescriptor(username) != null) {
					throw newSQLException(SQLState.NET_CONNECT_AUTH_FAILED, failedString);
				}
			}

			tr.rollback();
            InterruptStatus.restoreIntrFlagIfSeen(lcc);
		} catch (StandardException e) {
			try {
				tr.rollback();
			} catch (StandardException ee) {
			}

			throw handleException(e);
		}
	
	}

	/* Enumerate operations controlled by database owner powers */
	private static final int OP_ENCRYPT = 0;
	private static final int OP_SHUTDOWN = 1;
	private static final int OP_HARD_UPGRADE = 2;
	private static final int OP_REPLICATION = 3;
	/**
	 * Check if actual authenticationId is equal to the database owner's.
	 *
	 * @param operation attempted operation which needs database owner powers
	 * @throws SQLException if actual authenticationId is different
	 * from authenticationId of database owner.
	 */
	private void checkIsDBOwner(int operation) throws SQLException
	{
		final LanguageConnectionContext lcc = getLanguageConnection();
        final String actualId = lcc.getSessionUserId();
		final String dbOwnerId = lcc.getDataDictionary().
			getAuthorizationDatabaseOwner();
		if (!actualId.equals(dbOwnerId)) {
			switch (operation) {
			case OP_ENCRYPT:
				throw newSQLException(SQLState.AUTH_ENCRYPT_NOT_DB_OWNER,
									  actualId, tr.getDBName());
			case OP_SHUTDOWN:
				throw newSQLException(SQLState.AUTH_SHUTDOWN_NOT_DB_OWNER,
									  actualId, tr.getDBName());
			case OP_HARD_UPGRADE:
				throw newSQLException(SQLState.AUTH_HARD_UPGRADE_NOT_DB_OWNER,
									  actualId, tr.getDBName());
			case OP_REPLICATION:
				throw newSQLException(SQLState.AUTH_REPLICATION_NOT_DB_OWNER,
									  actualId, tr.getDBName());
			default:
				if (SanityManager.DEBUG) {
					SanityManager.THROWASSERT(
						"illegal checkIsDBOwner operation");
				}
				throw newSQLException(
					SQLState.AUTH_DATABASE_CONNECTION_REFUSED);
			}
		}
	}

    /**
     * Gets the EngineType of the connected database.
     *
     * @return 0 if there is no database, the engine type otherwise. @see com.splicemachine.db.iapi.reference.EngineType
     */
    public int getEngineType()
    {
        Database db = getDatabase();

        if( null == db)
            return 0;
        return db.getEngineType();
    }
    
	/*
	** Methods from java.sql.Connection
	*/

    /**
	 * SQL statements without parameters are normally
     * executed using Statement objects. If the same SQL statement 
     * is executed many times, it is more efficient to use a 
     * PreparedStatement
     *
     * JDBC 2.0
     *
     * Result sets created using the returned Statement will have
     * forward-only type, and read-only concurrency, by default.
     *
     * @return a new Statement object 
     * @exception SQLException if a database-access error occurs.
     */
	public final Statement createStatement() throws SQLException 
	{
		return createStatement(ResultSet.TYPE_FORWARD_ONLY,
							   ResultSet.CONCUR_READ_ONLY,
							   connectionHoldAbility);
	}

    /**
     * JDBC 2.0
     *
     * Same as createStatement() above, but allows the default result set
     * type and result set concurrency type to be overridden.
     *
     * @param resultSetType a result set type, see ResultSet.TYPE_XXX
     * @param resultSetConcurrency a concurrency type, see ResultSet.CONCUR_XXX
     * @return a new Statement object 
      * @exception SQLException if a database-access error occurs.
    */
    public final Statement createStatement(int resultSetType,
    								 int resultSetConcurrency) 
						throws SQLException
	{
		return createStatement(resultSetType, resultSetConcurrency,
			    connectionHoldAbility);
	}

    /**
     * JDBC 3.0
     *
     * Same as createStatement() above, but allows the default result set
     * type, result set concurrency type and result set holdability type to
     * be overridden.
     *
     * @param resultSetType a result set type, see ResultSet.TYPE_XXX
     * @param resultSetConcurrency a concurrency type, see ResultSet.CONCUR_XXX
     * @param resultSetHoldability a holdability type,
     *  ResultSet.HOLD_CURSORS_OVER_COMMIT or ResultSet.CLOSE_CURSORS_AT_COMMIT
     * @return a new Statement object
     * @exception SQLException if a database-access error occurs.
     */
    public final Statement createStatement(int resultSetType,
    								 int resultSetConcurrency,
    								 int resultSetHoldability)
						throws SQLException
	{
		checkIfClosed();

		return factory.newEmbedStatement(this, false,
			setResultSetType(resultSetType), resultSetConcurrency,
			resultSetHoldability);
	}

    /**
     * A SQL statement with or without IN parameters can be
     * pre-compiled and stored in a PreparedStatement object. This
     * object can then be used to efficiently execute this statement
     * multiple times.
     *
     * <P><B>Note:</B> This method is optimized for handling
     * parametric SQL statements that benefit from precompilation. If
     * the driver supports precompilation, prepareStatement will send
     * the statement to the database for precompilation. Some drivers
     * may not support precompilation. In this case, the statement may
     * not be sent to the database until the PreparedStatement is
     * executed.  This has no direct affect on users; however, it does
     * affect which method throws certain SQLExceptions.
     *
     * JDBC 2.0
     *
     * Result sets created using the returned PreparedStatement will have
     * forward-only type, and read-only concurrency, by default.
     *
     * @param sql a SQL statement that may contain one or more '?' IN
     * parameter placeholders
     * @return a new PreparedStatement object containing the
     * pre-compiled statement 
     * @exception SQLException if a database-access error occurs.
     */
    public final PreparedStatement prepareStatement(String sql)
	    throws SQLException
	{
		return prepareStatement(sql,ResultSet.TYPE_FORWARD_ONLY,
			ResultSet.CONCUR_READ_ONLY,
			connectionHoldAbility,
			Statement.NO_GENERATED_KEYS,
			null,
			null);
	}


    /**
     * JDBC 2.0
     *
     * Same as prepareStatement() above, but allows the default result set
     * type and result set concurrency type to be overridden.
     *
     * @param resultSetType a result set type, see ResultSet.TYPE_XXX
     * @param resultSetConcurrency a concurrency type, see ResultSet.CONCUR_XXX
     * @return a new PreparedStatement object containing the
     * pre-compiled SQL statement
     * @exception SQLException if a database-access error occurs.
     */
    public final PreparedStatement prepareStatement(String sql, int resultSetType,
					int resultSetConcurrency)
	    throws SQLException
	{
		return prepareStatement(sql,
			resultSetType,
			resultSetConcurrency,
			connectionHoldAbility,
			Statement.NO_GENERATED_KEYS,
			null,
			null);
	}

    /**
     * JDBC 3.0
     *
     * Same as prepareStatement() above, but allows the default result set
     * type, result set concurrency type and result set holdability
     * to be overridden.
     *
     * @param resultSetType a result set type, see ResultSet.TYPE_XXX
     * @param resultSetConcurrency a concurrency type, see ResultSet.CONCUR_XXX
     * @param resultSetHoldability - one of the following ResultSet constants:
     *  ResultSet.HOLD_CURSORS_OVER_COMMIT or ResultSet.CLOSE_CURSORS_AT_COMMIT
     * @return a new PreparedStatement object containing the
     *  pre-compiled SQL statement
     * @exception SQLException if a database-access error occurs.
     */
    public final PreparedStatement prepareStatement(String sql, int resultSetType,
					int resultSetConcurrency, int resultSetHoldability)
	    throws SQLException
	{
		return prepareStatement(sql,
			resultSetType,
			resultSetConcurrency,
			resultSetHoldability,
			Statement.NO_GENERATED_KEYS,
			null,
			null);
	}


	/**
	 * Creates a default PreparedStatement object capable of returning
	 * the auto-generated keys designated by the given array. This array contains
	 * the indexes of the columns in the target table that contain the auto-generated
	 * keys that should be made available. This array is ignored if the SQL statement
	 * is not an INSERT statement

		JDBC 3.0
	 *
	 *
	 * @param sql  An SQL statement that may contain one or more ? IN parameter placeholders
	 * @param columnIndexes  An array of column indexes indicating the columns
	 *  that should be returned from the inserted row or rows
	 *
	 * @return  A new PreparedStatement object, containing the pre-compiled
	 *  SQL statement, that will have the capability of returning auto-generated keys
	 *  designated by the given array of column indexes
	 *
	 * @exception SQLException  Thrown on error.
	 */
	public final PreparedStatement prepareStatement(
			String sql,
			int[] columnIndexes)
    throws SQLException
	{
  		return prepareStatement(sql,
			ResultSet.TYPE_FORWARD_ONLY,
			ResultSet.CONCUR_READ_ONLY,
			connectionHoldAbility,
			(columnIndexes == null || columnIndexes.length == 0)
				? Statement.NO_GENERATED_KEYS
				: Statement.RETURN_GENERATED_KEYS,
			columnIndexes,
			null);
	}

	/**
	 * Creates a default PreparedStatement object capable of returning
	 * the auto-generated keys designated by the given array. This array contains
	 * the names of the columns in the target table that contain the auto-generated
	 * keys that should be returned. This array is ignored if the SQL statement
	 * is not an INSERT statement
	 *
		JDBC 3.0
	 *
	 * @param sql  An SQL statement that may contain one or more ? IN parameter placeholders
	 * @param columnNames  An array of column names indicating the columns
	 *  that should be returned from the inserted row or rows
	 *
	 * @return  A new PreparedStatement object, containing the pre-compiled
	 *  SQL statement, that will have the capability of returning auto-generated keys
	 *  designated by the given array of column names
	 *
	 * @exception SQLException Thrown on error.
	 */
	public final PreparedStatement prepareStatement(
			String sql,
			String[] columnNames)
    throws SQLException
	{
  		return prepareStatement(sql,
			ResultSet.TYPE_FORWARD_ONLY,
			ResultSet.CONCUR_READ_ONLY,
			connectionHoldAbility,
			(columnNames == null || columnNames.length == 0)
				? Statement.NO_GENERATED_KEYS
				: Statement.RETURN_GENERATED_KEYS,
			null,
			columnNames);
	}

	/**
	 * Creates a default PreparedStatement object that has the capability to
	 * retieve auto-generated keys. The given constant tells the driver
	 * whether it should make auto-generated keys available for retrieval.
	 * This parameter is ignored if the SQL statement is not an INSERT statement.
	 * JDBC 3.0
	 *
	 * @param sql  A SQL statement that may contain one or more ? IN parameter placeholders
	 * @param autoGeneratedKeys  A flag indicating whether auto-generated keys
	 *  should be returned
	 *
	 * @return  A new PreparedStatement object, containing the pre-compiled
	 *  SQL statement, that will have the capability of returning auto-generated keys
	 *
	 * @exception SQLException  Feature not implemented for now.
	 */
	public final PreparedStatement prepareStatement(
			String sql,
			int autoGeneratedKeys)
    throws SQLException
	{
		return prepareStatement(sql,
			ResultSet.TYPE_FORWARD_ONLY,
			ResultSet.CONCUR_READ_ONLY,
			connectionHoldAbility,
			autoGeneratedKeys,
			null,
			null);
	}
    
	private PreparedStatement prepareStatement(String sql, int resultSetType,
					int resultSetConcurrency, int resultSetHoldability,
					int autoGeneratedKeys, int[] columnIndexes, String[] columnNames)
       throws SQLException
	 {
		synchronized (getConnectionSynchronization()) {
                        setupContextStack();
			try {
			    return factory.newEmbedPreparedStatement(this, sql, false,
											   setResultSetType(resultSetType),
											   resultSetConcurrency,
											   resultSetHoldability,
											   autoGeneratedKeys,
											   columnIndexes,
											   columnNames);
			} finally {
			    restoreContextStack();
			}
		}
     }

    /**
     * A SQL stored procedure call statement is handled by creating a
     * CallableStatement for it. The CallableStatement provides
     * methods for setting up its IN and OUT parameters, and
     * methods for executing it.
     *
     * <P><B>Note:</B> This method is optimized for handling stored
     * procedure call statements. Some drivers may send the call
     * statement to the database when the prepareCall is done; others
     * may wait until the CallableStatement is executed. This has no
     * direct affect on users; however, it does affect which method
     * throws certain SQLExceptions.
     *
     * JDBC 2.0
     *
     * Result sets created using the returned CallableStatement will have
     * forward-only type, and read-only concurrency, by default.
     *
     * @param sql a SQL statement that may contain one or more '?'
     * parameter placeholders. Typically this  statement is a JDBC
     * function call escape string.
     * @return a new CallableStatement object containing the
     * pre-compiled SQL statement 
     * @exception SQLException if a database-access error occurs.
     */
	public final CallableStatement prepareCall(String sql) 
		throws SQLException 
	{
		return prepareCall(sql, ResultSet.TYPE_FORWARD_ONLY,
						   ResultSet.CONCUR_READ_ONLY,
						   connectionHoldAbility);
	}

    /**
     * JDBC 2.0
     *
     * Same as prepareCall() above, but allows the default result set
     * type and result set concurrency type to be overridden.
     *
     * @param resultSetType a result set type, see ResultSet.TYPE_XXX
     * @param resultSetConcurrency a concurrency type, see ResultSet.CONCUR_XXX
     * @return a new CallableStatement object containing the
     * pre-compiled SQL statement 
     * @exception SQLException if a database-access error occurs.
     */
    public final CallableStatement prepareCall(String sql, int resultSetType,
				 int resultSetConcurrency)
		throws SQLException 
	{
		return prepareCall(sql, resultSetType, resultSetConcurrency,
						   connectionHoldAbility);
	}

    /**
     * JDBC 3.0
     *
     * Same as prepareCall() above, but allows the default result set
     * type, result set concurrency type and result set holdability
     * to be overridden.
     *
     * @param resultSetType a result set type, see ResultSet.TYPE_XXX
     * @param resultSetConcurrency a concurrency type, see ResultSet.CONCUR_XXX
     * @param resultSetHoldability - one of the following ResultSet constants:
     *  ResultSet.HOLD_CURSORS_OVER_COMMIT or ResultSet.CLOSE_CURSORS_AT_COMMIT
     * @return a new CallableStatement object containing the
     * pre-compiled SQL statement 
     * @exception SQLException if a database-access error occurs.
     */
    public final CallableStatement prepareCall(String sql, int resultSetType, 
				 int resultSetConcurrency, int resultSetHoldability)
		throws SQLException 
	{
		checkIfClosed();

		synchronized (getConnectionSynchronization())
		{
            setupContextStack();
			try 
			{
			    return factory.newEmbedCallableStatement(this, sql,
											   setResultSetType(resultSetType),
											   resultSetConcurrency,
											   resultSetHoldability);
			} 
			finally 
			{
			    restoreContextStack();
			}
		}
	}

    /**
     * A driver may convert the JDBC sql grammar into its system's
     * native SQL grammar prior to sending it; nativeSQL returns the
     * native form of the statement that the driver would have sent.
     *
     * @param sql a SQL statement that may contain one or more '?'
     * parameter placeholders
     * @return the native form of this statement
     */
    public String nativeSQL(String sql) throws SQLException {
        checkIfClosed();
		// we don't massage the strings at all, so this is easy:
		return sql;
	}

    /**
     * If a connection is in auto-commit mode, then all its SQL
     * statements will be executed and committed as individual
     * transactions.  Otherwise, its SQL statements are grouped into
     * transactions that are terminated by either commit() or
     * rollback().  By default, new connections are in auto-commit
     * mode.
     *
     * The commit occurs when the statement completes or the next
     * execute occurs, whichever comes first. In the case of
     * statements returning a ResultSet, the statement completes when
     * the last row of the ResultSet has been retrieved or the
     * ResultSet has been closed. In advanced cases, a single
     * statement may return multiple results as well as output
     * parameter values. Here the commit occurs when all results and
     * output param values have been retrieved.
     *
     * @param autoCommit true enables auto-commit; false disables
     * auto-commit.  
     * @exception SQLException if a database-access error occurs.
     */
	public void setAutoCommit(boolean autoCommit) throws SQLException {
		checkIfClosed();

		// Is this a nested connection
		if (rootConnection != this) {
			if (autoCommit)
				throw newSQLException(SQLState.NO_AUTO_COMMIT_ON);
		}

		if (this.autoCommit != autoCommit)
			commit();

		this.autoCommit = autoCommit;
	}

    /**
     * Get the current auto-commit state.
     *
     * @return Current state of auto-commit mode.
     * @see #setAutoCommit 
     */
    public boolean getAutoCommit() throws SQLException {
        checkIfClosed();
		return autoCommit;
	}

    /**
     * Commit makes all changes made since the previous
     * commit/rollback permanent and releases any database locks
     * currently held by the Connection. This method should only be
     * used when auto commit has been disabled.
     *
     * @exception SQLException if a database-access error occurs.
     * @see #setAutoCommit 
     */
    public void commit() throws SQLException {
		synchronized (getConnectionSynchronization())
		{
			/*
			** Note that the context stack is
			** needed even for rollback & commit
			*/
            setupContextStack();

			try
			{
		    	getTR().commit();
		    	clearLOBMapping();
                InterruptStatus.restoreIntrFlagIfSeen(getLanguageConnection());
			}
            catch (Throwable t)
			{
				throw handleException(t);
			}
			finally 
			{
				restoreContextStack();
			}

			needCommit = false;
		}
	}

    /**
     * Rollback drops all changes made since the previous
     * commit/rollback and releases any database locks currently held
     * by the Connection. This method should only be used when auto
     * commit has been disabled.
     *
     * @exception SQLException if a database-access error occurs.
     * @see #setAutoCommit 
     */
    public void rollback() throws SQLException {

		synchronized (getConnectionSynchronization())
		{
			/*
			** Note that the context stack is
			** needed even for rollback & commit
			*/
            setupContextStack();
			try
			{
		    	getTR().rollback();
		    	clearLOBMapping();
                InterruptStatus.restoreIntrFlagIfSeen(getLanguageConnection());
			} catch (Throwable t) {
				throw handleException(t);
			}
			finally 
			{
				restoreContextStack();
			}
			needCommit = false;
		} 
	}

    /**
     * In some cases, it is desirable to immediately release a
     * Connection's database and JDBC resources instead of waiting for
     * them to be automatically released; the close method provides this
     * immediate release. 
     *
     * <P><B>Note:</B> A Connection is automatically closed when it is
     * garbage collected. Certain fatal errors also result in a closed
     * Connection.
     *
     * @exception SQLException if a database-access error occurs.
     */
    public void close() throws SQLException {
        checkForTransactionInProgress();
		close(exceptionClose);
	}

    /**
     * Check if the transaction is active so that we cannot close down the
     * connection. If auto-commit is on, the transaction is committed when the
     * connection is closed, so it is always OK to close the connection in that
     * case. Otherwise, throw an exception if a transaction is in progress.
     *
     * @throws SQLException if this transaction is active and the connection
     * cannot be closed
     */
    public void checkForTransactionInProgress() throws SQLException {
        if (!isClosed() && (rootConnection == this) &&
                !autoCommit && !transactionIsIdle()) {
            // DERBY-1191 partial fix. Make sure this  exception is logged with
            // db.stream.error.logSeverityLevel=0 so users can see changes needed
            // after the DERBY-3319 fix.
            Util.logAndThrowSQLException(newSQLException(SQLState.CANNOT_CLOSE_ACTIVE_CONNECTION));
        }
    }

	// This inner close takes the exception and calls 
	// the context manager to make the connection close.
	// The exception must be a session severity exception.
	//
	// NOTE: This method is not part of JDBC specs.
	//
    protected void close(StandardException e) throws SQLException {
		
		synchronized(getConnectionSynchronization())
		{
			if (rootConnection == this)
			{
				/*
				 * If it isn't active, it's already been closed.
				 */
				if (active || isAborting()) {
					if (tr.isActive()) {
						setupContextStack();
						try {
							tr.rollback();
                            InterruptStatus.
                                    restoreIntrFlagIfSeen(tr.getLcc());
							
							// Let go of lcc reference so it can be GC'ed after
							// cleanupOnError, the tr will stay around until the
							// rootConnection itself is GC'ed, which is dependent
							// on how long the client program wants to hold on to
							// the Connection object.
							tr.clearLcc(); 
                            // DERBY-4856, assume database is not up
                            tr.cleanupOnError(e, false);
							
						} catch (Throwable t) {
							throw handleException(t);
						} finally {
							restoreContextStack();
						}
					} else {
						// DERBY-1947: If another connection has closed down
						// the database, the transaction is not active, but
						// the cleanup has not been done yet.
                        InterruptStatus.restoreIntrFlagIfSeen();
						tr.clearLcc(); 
                        // DERBY-4856, assume database is not up
                        tr.cleanupOnError(e, false);
					}
				}
			}

            aborting = false;
            
			if (!isClosed())
				setInactive();
		}
	}

    /**
     * Tests to see if a Connection is closed.
     *
     * @return true if the connection is closed; false if it's still open
     */
    public final boolean isClosed() {
		if (active) {

			// I am attached, check the database state
			if (getTR().isActive()) {
				return false;
			}
		}
		return true;
	}

    /**
     * A Connection's database is able to provide information
     * describing its tables, its supported SQL grammar, its stored
     * procedures, the capabilities of this connection, etc. This
     * information is made available through a DatabaseMetaData
     * object.
     *
     * @return a DatabaseMetaData object for this Connection 
     * @exception SQLException if a database-access error occurs.
     */
    public DatabaseMetaData getMetaData() throws SQLException {
        checkIfClosed();

		if (dbMetadata == null) {

 			// There is a case where dbname can be null.
			// Replication client of this method does not have a
			// JDBC connection; therefore dbname is null and this
			// is expected.
			//
			dbMetadata = factory.newEmbedDatabaseMetaData(this, getTR().getUrl());
		}
		return dbMetadata;
	}

	/**
		JDBC 3.0
	 * Retrieves the current holdability of ResultSet objects created using this
	 * Connection object.
	 *
	 *
	 * @return  The holdability, one of ResultSet.HOLD_CURSORS_OVER_COMMIT
	 * or ResultSet.CLOSE_CURSORS_AT_COMMIT
	 *
	 */
	public final int getHoldability() throws SQLException {
		checkIfClosed();
		return connectionHoldAbility;
	}

	/**
		JDBC 3.0
	 * Changes the holdability of ResultSet objects created using this
	 * Connection object to the given holdability.
	 *
	 *
	 * @param holdability  A ResultSet holdability constant, one of ResultSet.HOLD_CURSORS_OVER_COMMIT
	 * or ResultSet.CLOSE_CURSORS_AT_COMMIT
	 *
	 */
	public final void setHoldability(int holdability) throws SQLException {
		checkIfClosed();
		connectionHoldAbility = holdability;
	}

    /**
     * You can put a connection in read-only mode as a hint to enable 
     * database optimizations.
     *
     * <P><B>Note:</B> setReadOnly cannot be called while in the
     * middle of a transaction.
     *
     * @param readOnly true enables read-only mode; false disables
     * read-only mode.  
     * @exception SQLException if a database-access error occurs.
     */
    public final void setReadOnly(boolean readOnly) throws SQLException
	{
		synchronized(getConnectionSynchronization())
		{
                        setupContextStack();
			try {
                LanguageConnectionContext lcc = getLanguageConnection();
                lcc.setReadOnly(readOnly);
                InterruptStatus.restoreIntrFlagIfSeen(lcc);
			} catch (StandardException e) {
				throw handleException(e);
			} finally {
				restoreContextStack();
			}
		}
	}

    /**
     * Tests to see if the connection is in read-only mode.
     *
     * @return true if connection is read-only
     * @exception SQLException if a database-access error occurs.
     */
    public final boolean isReadOnly() throws SQLException
	{
		checkIfClosed();
		try {
			return getLanguageConnection().isReadOnly();
		} catch (StandardException se) {
			throw new SQLException(se);
		}
	}

    /**
     * A sub-space of this Connection's database may be selected by setting a
     * catalog name. If the driver does not support catalogs it will
     * silently ignore this request.
     *
     * @exception SQLException if a database-access error occurs.
     */
    public void setCatalog(String catalog) throws SQLException {
        checkIfClosed();
		// silently ignoring this request like the javadoc said.
		return;
	}

    /**
     * Return the Connection's current catalog name.
     *
     * @return the current catalog name or null
     * @exception SQLException if a database-access error occurs.
     */
	public String getCatalog() throws SQLException {
		checkIfClosed();
		// we do not have support for Catalog, just return null as
		// the JDBC specs mentions then.
		return null;
	}

    /**
     * You can call this method to try to change the transaction
     * isolation level using one of the TRANSACTION_* values.
     *
     * <P><B>Note:</B> setTransactionIsolation causes the current
     * transaction to commit if the isolation level is changed. Otherwise, if
     * the requested isolation level is the same as the current isolation
     * level, this method is a no-op.
     *
     * @param level one of the TRANSACTION_* isolation values with the
     * exception of TRANSACTION_NONE; some databases may not support
     * other values
     * @exception SQLException if a database-access error occurs.
     * @see DatabaseMetaData#supportsTransactionIsolationLevel 
     */
    public void setTransactionIsolation(int level) throws SQLException {

		if (level == getTransactionIsolation())
			return;

		// Convert the isolation level to the internal one
		int iLevel;
		switch (level)
		{
		case java.sql.Connection.TRANSACTION_READ_UNCOMMITTED:
			iLevel = ExecutionContext.READ_UNCOMMITTED_ISOLATION_LEVEL;
			break;

		case java.sql.Connection.TRANSACTION_READ_COMMITTED:
			iLevel = ExecutionContext.READ_COMMITTED_ISOLATION_LEVEL;
			break;

		case java.sql.Connection.TRANSACTION_REPEATABLE_READ:
            iLevel = ExecutionContext.REPEATABLE_READ_ISOLATION_LEVEL;
            break;

		case java.sql.Connection.TRANSACTION_SERIALIZABLE:
			iLevel = ExecutionContext.SERIALIZABLE_ISOLATION_LEVEL;
			break;
		default:
			throw newSQLException(SQLState.UNIMPLEMENTED_ISOLATION_LEVEL, new Integer(level));
		}

		synchronized(getConnectionSynchronization())
		{
            setupContextStack();
			try {
                LanguageConnectionContext lcc = getLanguageConnection();
                lcc.setIsolationLevel(iLevel);
                InterruptStatus.restoreIntrFlagIfSeen(lcc);
			} catch (StandardException e) {
				throw handleException(e);
			} finally {
				restoreContextStack();
			}
		}
	}


    /**
     * Get this Connection's current transaction isolation mode.
     *
     * @return the current TRANSACTION_* mode value
     * @exception SQLException if a database-access error occurs.
     */
    public final int getTransactionIsolation() throws SQLException {
        checkIfClosed();
		return ExecutionContext.CS_TO_JDBC_ISOLATION_LEVEL_MAP[getLanguageConnection().getCurrentIsolationLevel()];
	}

    /**
     * The first warning reported by calls on this Connection is
     * returned.  
     *
     * <P><B>Note:</B> Subsequent warnings will be chained to this
     * SQLWarning.
     *
     * @return the first SQLWarning or null 
     *
	 * Synchronization note: Warnings are synchronized 
	 * on nesting level
     */
	public final synchronized SQLWarning getWarnings() throws SQLException {
		checkIfClosed();
   		return topWarning;
	}

    /**
     * After this call, getWarnings returns null until a new warning is
     * reported for this Connection.  
     *
	 * Synchronization node: Warnings are synchonized 
	 * on nesting level
     */
    public final synchronized void clearWarnings() throws SQLException {
        checkIfClosed();
		topWarning = null;
	}

 	/////////////////////////////////////////////////////////////////////////
	//
	//	JDBC 2.0	-	New public methods
	//
	/////////////////////////////////////////////////////////////////////////

    /**
     *
	 * Get the type-map object associated with this connection.
	 * By default, the map returned is empty.
	 * JDBC 2.0 - java.util.Map requires JDK 1
     *
     */
    public java.util.Map getTypeMap() throws SQLException {
        checkIfClosed();
		// just return an immuntable empty map
		return java.util.Collections.EMPTY_MAP;
    }

    /** 
	 * Install a type-map object as the default type-map for
	 * this connection.
	 * JDBC 2.0 - java.util.Map requires JDK 1
     *
     * @exception SQLException Feature not implemented for now.
	 */
    public final void setTypeMap(java.util.Map map) throws SQLException {
        checkIfClosed();
        if( map == null)
            throw Util.generateCsSQLException(SQLState.INVALID_API_PARAMETER,map,"map",
                                              "java.sql.Connection.setTypeMap");
        if(!(map.isEmpty()))
            throw Util.notImplemented();
    }

	/////////////////////////////////////////////////////////////////////////
	//
	//	Implementation specific methods	
	//
	/////////////////////////////////////////////////////////////////////////

	/**
		Add a warning to the current list of warnings, to follow
		this note from Connection.getWarnings.
		Note: Subsequent warnings will be chained to this SQLWarning. 

		@see java.sql.Connection#getWarnings
	*/
	 public final synchronized void addWarning(SQLWarning newWarning) {
		if (topWarning == null) {
			topWarning = newWarning;
			return;
		}

		topWarning.setNextWarning(newWarning);
	}

	/**
	 * Return the dbname for this connection.
	 *
	 * @return String	The dbname for this connection.
	 */
	public String getDBName()
	{
		if (SanityManager.DEBUG)
			SanityManager.ASSERT(!isClosed(), "connection is closed");

		return getTR().getDBName();
	}

	public final LanguageConnectionContext getLanguageConnection() {

		if (SanityManager.DEBUG)
			SanityManager.ASSERT(!isClosed() || isAborting(), "connection is closed");

		return getTR().getLcc();
	}

    /**
     * Raises an exception if the connection is closed.
     *
     * @exception SQLException if the connection is closed
     */
    protected final void checkIfClosed() throws SQLException {
        if (isClosed()) {
            throw Util.noCurrentConnection();
        }
    }

	//EmbedConnection30 overrides this method so it can release the savepoints array if
	//the exception severity is transaction level
	SQLException handleException(Throwable thrownException)
			throws SQLException
	{
		//assume in case of SQLException cleanup is 
		//done already. This assumption is inline with
		//TR's assumption. In case no rollback was 
		//called lob objects will remain valid.
		if (thrownException instanceof StandardException) {
			if (((StandardException) thrownException)
				.getSeverity() 
				>= ExceptionSeverity.TRANSACTION_SEVERITY) {
				clearLOBMapping();
			}
		}

		/*
		** By default, rollback the connection on if autocommit
	 	** is on.
		*/
		return getTR().handleException(thrownException, 
								  autoCommit,
								  true // Rollback xact on auto commit
								  );
	}

	/**
		Handle any type of Exception.
		<UL>
		<LI> Inform the contexts of the error
		<LI> Throw an Util based upon the thrown exception.
		</UL>

		REMIND: now that we know all the exceptions from our driver
		are Utils, would it make sense to shut down the system
		for unknown SQLExceptions? At present, we do not.

		Because this is the last stop for exceptions,
		it will catch anything that occurs in it and try
		to cleanup before re-throwing them.
	
		@param thrownException the exception
		@param rollbackOnAutoCommit rollback the xact on if autocommit is
				on, otherwise rollback stmt but leave xact open (and
				continue to hold on to locks).  Most of the time, this
				will be true, excepting operations on result sets, like
				getInt().
	*/
	final SQLException handleException(Throwable thrownException, 
									   boolean rollbackOnAutoCommit) 
			throws SQLException 
	{
		//assume in case of SQLException cleanup is 
		//done already. This assumption is inline with
		//TR's assumption. In case no rollback was 
		//called lob objects will remain valid.
		if (thrownException instanceof StandardException) {
			if (((StandardException) thrownException)
				.getSeverity() 
				>= ExceptionSeverity.TRANSACTION_SEVERITY) {
				clearLOBMapping();
			}
		}
		return getTR().handleException(thrownException, autoCommit,
								  rollbackOnAutoCommit); 

	}

	/*
	   This is called from the EmbedConnectionContext to
	   close on errors.  We assume all handling of the connectin
	   is dealt with via the context stack, and our only role
	   is to mark ourself as closed.
	 */

	/**
		Close the connection when processing errors, or when
	 	closing a nested connection.
		<p>
		This only marks it as closed and frees up its resources;
		any closing of the underlying connection or commit work
		is assumed to be done elsewhere.

		Called from EmbedConnectionContext's cleanup routine,	
		and by proxy.close().
	 */

	public final void setInactive() {

		if (active == false)
			return;
		// active = false
		// tr = null !-> active = false

		synchronized (getConnectionSynchronization()) {
			active = false;
			// tr = null; cleanupOnerror sets inactive but still needs tr to
			// restore context later
			dbMetadata = null;
		}
	}

	/**
		@exception Throwable	standard error policy
	 */
	protected void finalize() throws Throwable 
	{
		try {
			// Only close root connections, since for nested
			// connections, it is not strictly necessary and close()
			// synchronizes on the root connection which can cause
			// deadlock with the call to runFinalization from
			// GenericPreparedStatement#prepareToInvalidate (see
			// DERBY-1947) on SUN VMs.
			if (rootConnection == this) {
				close(exceptionClose);
			}
		}
		finally {
			super.finalize();
		}
	}

	/**
	 * if auto commit is on, remember that we need to commit
	 * the current statement.
	 */
    protected void needCommit() {
		if (!needCommit) needCommit = true;
	}

	/**
	 * if a commit is needed, perform it.
     *
     * Must have connection synchonization and context set up already.
     *
	 * @exception SQLException if commit returns error
	 */
	protected void commitIfNeeded() throws SQLException 
    {
		if (autoCommit && needCommit) 
        {
            try
            {
                getTR().commit();
                clearLOBMapping();
                InterruptStatus.restoreIntrFlagIfSeen(getLanguageConnection());
            } 
            catch (Throwable t)
            {
                throw handleException(t);
            }
            needCommit = false;
		}
	}

	/**
	 * If in autocommit, then commit.
     * 
     * Used to force a commit after a result set closes in autocommit mode.
     * The needCommit mechanism does not work correctly as there are times
     * with cursors (like a commit, followed by a next, followed by a close)
     * where the system does not think it needs a commit but we need to 
     * force the commit on close.  It seemed safer to just force a commit
     * on close rather than count on keeping the needCommit flag correct for
     * all cursor cases.
     *
     * Must have connection synchonization and context set up already.
     *
	 * @exception SQLException if commit returns error
	 */
	protected void commitIfAutoCommit() throws SQLException 
    {
		if (autoCommit) 
        {
            try
            {
                getTR().commit();
                clearLOBMapping();
                InterruptStatus.restoreIntrFlagIfSeen(getLanguageConnection());
            } 
            catch (Throwable t)
            {
                throw handleException(t);
            }
            needCommit = false;
		}
	}


	final protected Object getConnectionSynchronization()
  {
		return rootConnection;
  }

	/**
		Install the context manager for this thread.  Check connection status here.
	 	@exception SQLException if fails
	 */
	protected final void  setupContextStack() throws SQLException {

		/*
			Track this entry, then throw an exception
			rather than doing the quiet return.  Need the
			track before the throw because the backtrack
			is in a finally block.
		 */

		if ( !isAborting()) { checkIfClosed(); }

		getTR().setupContextStack();

	}

	protected final void restoreContextStack() throws SQLException {

		if (SanityManager.DEBUG)
		Util.ASSERT(this, (active) || getTR().getCsf() !=null, "No context service to do restore");

		TransactionResourceImpl tr = getTR();

		//REMIND: someone is leaving an incorrect manager on when they
		// are exiting the system in the nested case.
//		if (SanityManager.DEBUG) {
//			if (tr.getCsf() != null) {
//				ContextManager cm1 = tr.getCsf().getCurrentContextManager();
//				ContextManager cm2 = tr.getContextManager();
//				// If the system has been shut down, cm1 can be null.
//				// Otherwise, cm1 and cm2 should be identical.
//				if (cm1 != cm2 && cm1 != null) {
//					Util.ASSERT(this, (cm1 == cm2 || cm1 == null),
//						"Current Context Manager not the one was expected: " +
//						 cm1 + " " + cm2);
//				}
//			}
//		}

		tr.restoreContextStack();
	}

	/*
	** Create database methods.
	*/

	/**
		Create a new database.
		@param dbname the database name
		@param info the properties

		@return	Database The newly created database or null.

	 	@exception SQLException if fails to create database
	*/

	private Database createDatabase(String dbname, Properties info)
		throws SQLException {

		info = filterProperties(info);

		// check for create database privileges
		// DERBY-3495: uncomment to enable system privileges checks
		//final String user = IdUtil.getUserNameFromURLProps(info);
		//checkDatabaseCreatePrivileges(user, dbname);

		try {
			if (Monitor.createPersistentService(Property.DATABASE_MODULE, dbname, info) == null) 
			{
				// service already exists, create a warning
				addWarning(SQLWarningFactory.newSQLWarning(SQLState.DATABASE_EXISTS, dbname));
			}

		} catch (StandardException mse) {
            throw Util.seeNextException(SQLState.CREATE_DATABASE_FAILED,
                                        new Object[] { dbname },
                                        handleException(mse));
		}

		// clear these values as some modules hang onto
		// the properties set corresponding to service.properties
		// and they shouldn't be interested in these JDBC attributes.
		info.clear();

		return (Database) Monitor.findService(Property.DATABASE_MODULE, dbname);
	}

    /**
     * Checks that a user has the system privileges to create a database.
     * To perform this check the following policy grants are required
     * <ul>
     * <li> to run the encapsulated test:
     *        permission javax.security.auth.AuthPermission "doAsPrivileged";
     * <li> to resolve relative path names:
     *        permission java.util.PropertyPermission "user.dir", "read";
     * <li> to canonicalize path names:
     *        permission java.io.FilePermission "...", "read";
     * </ul>
     * or a SQLException will be raised detailing the cause.
     * <p>
     * In addition, for the test to succeed
     * <ul>
     * <li> the given user needs to be covered by a grant:
     *        principal com.splicemachine.db.authentication.SystemPrincipal "..." {}
     * <li> that lists a permission covering the database location:
     *        permission com.splicemachine.db.security.DatabasePermission "directory:...", "create";
     * </ul>
     * or it will fail with a SQLException detailing the cause.
     *
     * @param user The user to be checked for database create privileges
     * @param dbname the name of the database to create
     * @throws SQLException if the privileges check fails
     */
    private void checkDatabaseCreatePrivileges(String user,
                                               String dbname)
        throws SQLException {
        // approve action if not running under a security manager
        if (System.getSecurityManager() == null) {
            return;
        }
        if (dbname == null) {
            throw new NullPointerException("dbname can't be null");
        }
        
        // the check
        try {
            // raises IOException if dbname is non-canonicalizable
            final String url
                = (DatabasePermission.URL_PROTOCOL_DIRECTORY
                   + stripSubSubProtocolPrefix(dbname));
            final Permission dp
                = new DatabasePermission(url, DatabasePermission.CREATE);
            
            factory.checkSystemPrivileges(user, dp);
        } catch (AccessControlException ace) {
            throw Util.generateCsSQLException(
                    SQLState.AUTH_DATABASE_CREATE_MISSING_PERMISSION,
                    user, dbname, ace);
        } catch (IOException ioe) {
            throw Util.generateCsSQLException(
                    SQLState.AUTH_DATABASE_CREATE_EXCEPTION,
                    dbname, (Object)ioe); // overloaded method
        } catch (Exception e) {
            throw Util.generateCsSQLException(
                    SQLState.AUTH_DATABASE_CREATE_EXCEPTION,
                    dbname, (Object)e); // overloaded method
        }
    }

    /**
     * Puts the current thread to sleep.
     * <p>
     * <em>NOTE</em>: This method guarantees that the thread sleeps at
     * least {@code millis} milliseconds.
     *
     * @param millis milliseconds to sleep
     */
    private static void sleep(long millis) {
        long startMillis = System.currentTimeMillis();
        long waited = 0L;
        while (waited < millis) {
            try {
                Thread.sleep(millis - waited);
            } catch (InterruptedException ie) {
                InterruptStatus.setInterrupted();
                waited = System.currentTimeMillis() - startMillis;
                continue;
            }
            break;
        }

    }

    /**
     * Strips any sub-sub-protocol prefix from a database name.
     *
     * @param dbname a database name
     * @return the database name without any sub-sub-protocol prefixes
     * @throws NullPointerException if dbname is null
     */
    static public String stripSubSubProtocolPrefix(String dbname) {
        // check if database name starts with a sub-sub-protocol tag
        final int i = dbname.indexOf(':');
        if (i > 0) {
            // construct the sub-sub-protocol's system property name
            final String prop
                = Property.SUB_SUB_PROTOCOL_PREFIX + dbname.substring(0, i);
            
            // test for existence of a system property (JVM + db.properties)
            if (PropertyUtil.getSystemProperty(prop, null) != null) {
                return dbname.substring(i + 1); // the stripped database name
            }
        }
        return dbname; // the unmodified database name
    }

	/**
	 * Boot database.
	 *
	 * @param info boot properties
	 *
	 * @param softAuthenticationBoot If true, don't fail soft upgrade due
	 * to missing features (phase one of two phased hard upgrade boot).
	 *
	 * @return false iff the monitor cannot handle a service
	 * of the type indicated by the protocol within the name.
	 * If that's the case then we are the wrong driver.
	 *
	 * @throws Throwable if anything else is wrong.
	 */

	private boolean bootDatabase(Properties info,
								 boolean softAuthenticationBoot
								 ) throws Throwable
	{
		String dbname = tr.getDBName();

		// boot database now
		try {

			info = filterProperties(info);

			if (softAuthenticationBoot) {
				info.setProperty(Attribute.SOFT_UPGRADE_NO_FEATURE_CHECK,
								 "true");
			} else {
				info.remove(Attribute.SOFT_UPGRADE_NO_FEATURE_CHECK);
			}
			
			// try to start the service if it doesn't already exist
			if (!Monitor.startPersistentService(dbname, info)) {
				// a false indicates the monitor cannot handle a service
				// of the type indicated by the protocol within the name.
				// If that's the case then we are the wrong driver
				// so just return null.
				return false;
			}

			// clear these values as some modules hang onto
			// the properties set corresponding to service.properties
			// and they shouldn't be interested in these JDBC attributes.
			info.clear();

			Database database = (Database) Monitor.findService(Property.DATABASE_MODULE, dbname);
			tr.setDatabase(database);

		} catch (StandardException mse) {

			Throwable ne = mse.getCause();
			SQLException nse;

			/*
			  If there is a next exception, assume
			  that the first one is just a redundant "see the
			  next exception" message.
			  if it is a BEI, treat it as a database exception.
			  If there isn't a BEI, treat it as a java exception.

			  In general we probably want to walk the chain
			  and return all of them, but empirically, this
			  is all we need to do for now.
			  */
			if (ne instanceof StandardException)
				nse = Util.generateCsSQLException((StandardException)ne);
			else if (ne != null)
				nse = Util.javaException(ne);
			else
				nse = Util.generateCsSQLException(mse);

            throw Util.seeNextException(SQLState.BOOT_DATABASE_FAILED,
                                        new Object[] { dbname, 
                                        (Object) this.getClass().getClassLoader() }, nse);
		}

		// If database exists, getDatabase() will return the database object.
		// If any error occured while booting an existing database, an
		// exception would have been thrown already.
		return true;

	}

	/*
	 * Class interface methods used by database metadata to ensure
	 * good relations with autocommit.
	 */

    PreparedStatement prepareMetaDataStatement(String sql)
	    throws SQLException {
		synchronized (getConnectionSynchronization()) {
                        setupContextStack();
			PreparedStatement s = null;
			try {
			    s = factory.newEmbedPreparedStatement(this, sql, true,
											  ResultSet.TYPE_FORWARD_ONLY,
											  ResultSet.CONCUR_READ_ONLY,
											  connectionHoldAbility,
											  Statement.NO_GENERATED_KEYS,
											  null,
											  null);
			} finally {
                // Restore here, cf. comment in
                // EmbedDatabaseMetaData#getPreparedQuery:
                InterruptStatus.
                    restoreIntrFlagIfSeen(getLanguageConnection());
			    restoreContextStack();
			}
			return s;
		}
	}

	public final InternalDriver getLocalDriver()
	{
		if (SanityManager.DEBUG)
			SanityManager.ASSERT(!isClosed(), "connection is closed");

		return getTR().getDriver();
	}

	/**
		Return the context manager for this connection.
	*/
	public final ContextManager getContextManager() {

		if (SanityManager.DEBUG)
			SanityManager.ASSERT(!isClosed(), "connection is closed");

		return getTR().getContextManager();
	}

	/**
	 * Filter out properties from the passed in set of JDBC attributes
	 * to remove any db.* properties. This is to ensure that setting
	 * db.* properties does not work this way, it's not a defined way
	 * to set such properties and could be a secuirty hole in allowing
	 * remote connections to override system, application or database settings.
	 * 
	 * @return a new Properties set copied from the parameter but with no
	 * db.* properties.
	 */
	private Properties filterProperties(Properties inputSet) {
		Properties limited = new Properties();

		// filter out any db.* properties, only
		// JDBC attributes can be set this way
		for (java.util.Enumeration e = inputSet.propertyNames(); e.hasMoreElements(); ) {

			String key = (String) e.nextElement();

			// we don't allow properties to be set this way
			if (key.startsWith("derby."))
				continue;
			limited.put(key, inputSet.getProperty(key));
		}
		return limited;
	}

	/*
	** methods to be overridden by subimplementations wishing to insert
	** their classes into the mix.
	*/

	protected Database getDatabase() 
	{
		if (SanityManager.DEBUG)
			SanityManager.ASSERT(!isClosed(), "connection is closed");

		return getTR().getDatabase();
	}

	final protected TransactionResourceImpl getTR()
	{
		return rootConnection.tr;
	}

	private EmbedConnectionContext pushConnectionContext(ContextManager cm) {
		return new EmbedConnectionContext(cm, this);
	}

	public final void setApplicationConnection(java.sql.Connection applicationConnection) {
		this.applicationConnection = applicationConnection;
	}

	public final java.sql.Connection getApplicationConnection() {
		return applicationConnection;
	}

	public void setDrdaID(String drdaID) {
		getLanguageConnection().setDrdaID(drdaID);
	}

    /** @see EngineConnection#isInGlobalTransaction() */
    public boolean isInGlobalTransaction() {
    	return false;
    }

	/**
		Reset the connection before it is returned from a PooledConnection
		to a new application request (wrapped by a BrokeredConnection).
		Examples of reset covered here is dropping session temporary tables
		and reseting IDENTITY_VAL_LOCAL.
		Most JDBC level reset is handled by calling standard java.sql.Connection
		methods from EmbedPooledConnection.
	 */
	public void resetFromPool() throws SQLException {
		synchronized (getConnectionSynchronization())
		{
			setupContextStack();
			try {
                LanguageConnectionContext lcc = getLanguageConnection();
                lcc.resetFromPool();
                InterruptStatus.restoreIntrFlagIfSeen(lcc);
			} catch (StandardException t) {
				throw handleException(t);
			}
			finally
			{
				restoreContextStack();
			}
		}
	}

	/*
	** methods to be overridden by subimplementations wishing to insert
	** their classes into the mix.
	** The reason we need to override them is because we want to create a
	** Local20/LocalStatment object (etc) rather than a Local/LocalStatment
	** object (etc).
	*/


	/*
	** XA support
	*/

    /**
     * Do not use this method directly use XATransactionState.xa_prepare
     * instead because it also maintains/cancels the timout task which is
     * scheduled to cancel/rollback the global transaction.
     */
	public final int xa_prepare() throws SQLException {

		synchronized (getConnectionSynchronization())
		{
            setupContextStack();
			try
			{
                LanguageConnectionContext lcc = getLanguageConnection();
				XATransactionController tc = 
                    (XATransactionController)lcc.getTransactionExecute();

				int ret = tc.xa_prepare();

				if (ret == XATransactionController.XA_RDONLY)
				{
					// On a prepare call, xa allows an optimization that if the
					// transaction is read only, the RM can just go ahead and
					// commit it.  So if store returns this read only status -
					// meaning store has taken the liberty to commit already - we
					// needs to turn around and call internalCommit (without
					// committing the store again) to make sure the state is
					// consistent.  Since the transaction is read only, there is
					// probably not much that needs to be done.

                    lcc.internalCommit(false /* don't commitStore again */);
				}
                InterruptStatus.restoreIntrFlagIfSeen(lcc);
				return ret;
			} catch (StandardException t)
			{
				throw handleException(t);
			}
			finally
			{
				restoreContextStack();
			}
		}
	}


    /**
     * Do not use this method directly use XATransactionState.xa_commit
     * instead because it also maintains/cancels the timout task which is
     * scheduled to cancel/rollback the global transaction.
     */
	public final void xa_commit(boolean onePhase) throws SQLException {

		synchronized (getConnectionSynchronization())
		{
            setupContextStack();
			try
			{
                LanguageConnectionContext lcc = getLanguageConnection();
                lcc.xaCommit(onePhase);
                InterruptStatus.restoreIntrFlagIfSeen(lcc);
			} catch (StandardException t)
			{
				throw handleException(t);
			}
			finally 
			{
				restoreContextStack();
			}
		}
	}


    /**
     * Do not use this method directly use XATransactionState.xa_rollback
     * instead because it also maintains/cancels the timout task which is
     * scheduled to cancel/rollback the global transaction.
     */
	public final void xa_rollback() throws SQLException {
		synchronized (getConnectionSynchronization())
		{
            setupContextStack();
			try
			{
                LanguageConnectionContext lcc = getLanguageConnection();
                lcc.xaRollback();
                InterruptStatus.restoreIntrFlagIfSeen(lcc);
			} catch (StandardException t)
			{
				throw handleException(t);
			}
			finally 
			{
				restoreContextStack();
			}
		}
	}


	/**
	 * returns false if there is an underlying transaction and that transaction
	 * has done work.  True if there is no underlying transaction or that
	 * underlying transaction is idle
	 */
	public final boolean transactionIsIdle()
	{
		return getTR().isIdle();
	}
	private int setResultSetType(int resultSetType) {

		/* Add warning if scroll sensitive cursor
		 * and downgrade to scroll insensitive cursor.
		 */
		if (resultSetType == ResultSet.TYPE_SCROLL_SENSITIVE)
		{
			addWarning(SQLWarningFactory.newSQLWarning(SQLState.NO_SCROLL_SENSITIVE_CURSORS));
			resultSetType = ResultSet.TYPE_SCROLL_INSENSITIVE;
		}
		return resultSetType;
	}
	

	/** 
	 * Set the transaction isolation level that will be used for the 
	 * next prepare.  Used by network server to implement DB2 style 
	 * isolation levels.
	 * @param level Isolation level to change to.  level is the DB2 level
	 *               specified in the package names which happen to correspond
	 *               to our internal levels. If 
	 *               level == ExecutionContext.UNSPECIFIED_ISOLATION,
	 *               the statement won't be prepared with an isolation level.
	 * 
	 * 
	 */
	public void setPrepareIsolation(int level) throws SQLException
	{
		if (level == getPrepareIsolation())
			return;

		switch (level)
		{
			case ExecutionContext.READ_UNCOMMITTED_ISOLATION_LEVEL:
			case ExecutionContext.REPEATABLE_READ_ISOLATION_LEVEL:
			case ExecutionContext.READ_COMMITTED_ISOLATION_LEVEL:
			case ExecutionContext.SERIALIZABLE_ISOLATION_LEVEL:
			case ExecutionContext.UNSPECIFIED_ISOLATION_LEVEL:
				break;
			default:
				throw Util.generateCsSQLException(
															   SQLState.UNIMPLEMENTED_ISOLATION_LEVEL, new Integer(level));
		}
		
		synchronized(getConnectionSynchronization())
		{
			getLanguageConnection().setPrepareIsolationLevel(level);
		}
	}

	/**
	 * Return prepare isolation 
	 */
	public int getPrepareIsolation()
	{
		return getLanguageConnection().getPrepareIsolationLevel();
	}

	/**
		Return a unique order number for a result set.
		A unique value is only needed if the result set is
		being created within procedure and thus must be using
		a nested connection.
	*/
	final int getResultSetOrderId() {

		if (this == rootConnection) {
			return 0;
		} else {
			return rootConnection.resultSetId++;
		}
	}

    /** Get the exception factory for this connection. */
    public ExceptionFactory getExceptionFactory() {
        return Util.getExceptionFactory();
    }

	protected SQLException newSQLException(String messageId) {
		return Util.generateCsSQLException(messageId);
	}
	protected SQLException newSQLException(String messageId, Object arg1) {
		return Util.generateCsSQLException(messageId, arg1);
	}
	protected SQLException newSQLException(String messageId, Object arg1, Object arg2) {
		return Util.generateCsSQLException(messageId, arg1, arg2);
	}

	/////////////////////////////////////////////////////////////////////////
	//
	//	OBJECT OVERLOADS
	//
	/////////////////////////////////////////////////////////////////////////

    /**
     * Get a String representation that uniquely identifies
     * this connection.  Include the same information that is
     * printed in the log for various trace and error messages.
     *
     * In Derby the "physical" connection is a LanguageConnectionContext, 
     * or LCC.
     * The JDBC Connection is an JDBC-specific layer on top of this.  Rather
     * than create a new id here, we simply use the id of the underlying LCC.
     * Note that this is a big aid in debugging, because much of the
     * engine trace and log code prints the LCC id. 
     *
     * @return a string representation for this connection
     */
    public String toString()
    {
        if ( connString == null )
        {
            
            LanguageConnectionContext lcc = getLanguageConnection();

            connString = 
              this.getClass().getName() + "@" + this.hashCode() + " " +
                lcc.xidStr +                  
                    lcc.getTransactionExecute().getTransactionIdString() + 
                    "), " +
                lcc.lccStr + 
                    Integer.toString(lcc.getInstanceNumber()) + "), " +
                lcc.dbnameStr + lcc.getDbname() + "), " +
                lcc.drdaStr + lcc.getDrdaID() + ") ";
        }       
        
        return connString;
    }


	/**
	*
	* Constructs an object that implements the <code>Clob</code> interface. The object
	* returned initially contains no data.  The <code>setAsciiStream</code>,
	* <code>setCharacterStream</code> and <code>setString</code> methods of
	* the <code>Clob</code> interface may be used to add data to the <code>Clob</code>.
	*
	* @return An object that implements the <code>Clob</code> interface
	* @throws SQLException if an object that implements the
	* <code>Clob</code> interface can not be constructed, this method is
	* called on a closed connection or a database access error occurs.
	*
	*/
	public Clob createClob() throws SQLException {
		checkIfClosed();
		return new EmbedClob(this);
	}

	/**
	*
	* Constructs an object that implements the <code>Blob</code> interface. The object
	* returned initially contains no data.  The <code>setBinaryStream</code> and
	* <code>setBytes</code> methods of the <code>Blob</code> interface may be used to add data to
	* the <code>Blob</code>.
	*
	* @return  An object that implements the <code>Blob</code> interface
	* @throws SQLException if an object that implements the
	* <code>Blob</code> interface can not be constructed, this method is
	* called on a closed connection or a database access error occurs.
	*
	*/
	public Blob createBlob() throws SQLException {
		checkIfClosed();
		return new EmbedBlob(new byte[0], this);
	}

	/**
	* Add the locator and the corresponding LOB object into the
	* HashMap
	*
	* @param LOBReference The object which contains the LOB object that
	*                     that is added to the HashMap.
	* @return an integer that represents the locator that has been
	*         allocated to this LOB.
	*/
	public int addLOBMapping(Object LOBReference) {
		int loc = getIncLOBKey();
		getlobHMObj().put(new Integer(loc), LOBReference);
		return loc;
	}

	/**
	* Remove the key(LOCATOR) from the hash table.
	* @param key an integer that represents the locator that needs to be
	*            removed from the table.
	*/
	public void removeLOBMapping(int key) {
		getlobHMObj().remove(new Integer(key));
	}

	/**
	* Get the LOB reference corresponding to the locator.
	* @param key the integer that represents the LOB locator value.
	* @return the LOB Object corresponding to this locator.
	*/
	public Object getLOBMapping(int key) {
		return getlobHMObj().get(new Integer(key));
	}

	/**
	* Clear the HashMap of all entries.
	* Called when a commit or rollback of the transaction
	* happens.
	*/
	public void clearLOBMapping() throws SQLException {

		//free all the lob resources in the HashMap
		Map map = rootConnection.lobReferences;
		if (map != null) {
            Iterator it = map.keySet ().iterator ();
            while (it.hasNext()) {
                ((EngineLOB)it.next()).free();
			}
			map.clear();
		}
        if (rootConnection.lobHashMap != null) {
            rootConnection.lobHashMap.clear ();
        }
		
		synchronized (this) {   
            // Try a bit harder to close all open files, as open file handles
            // can cause problems further down the road.
			if (lobFiles != null) {       
                SQLException firstException = null;
				Iterator it = lobFiles.iterator();
                while (it.hasNext()) {
                    try {
                        ((LOBFile) it.next()).close();
                    } catch (IOException ioe) {
                        // Discard all exceptions besides the first one.
                        if (firstException == null) {
                            firstException = Util.javaException(ioe);
                        }
                    }
                }
                lobFiles.clear();
                if (firstException != null) {
                    throw firstException;
                }
			}
		}
	}

	/**
	* Return the current locator value/
        * 0x800x values are not  valid values as they are used to indicate the BLOB 
        * is being sent by value, so we skip those values (DERBY-3243)
        * 
	* @return an integer that represents the most recent locator value.
	*/
	private int getIncLOBKey() {
                int newKey = ++rootConnection.lobHMKey;
                // Skip 0x8000, 0x8002, 0x8004, 0x8006, for DERBY-3243
                // Earlier versions of the Derby Network Server (<10.3) didn't
                // support locators and would send an extended length field
                // with one of the above mentioned values instead of a
                // locator, even when locators were requested. To enable the
                // client driver to detect that locators aren't supported,
                // we don't use any of them as locator values.
                if (newKey == 0x8000 || newKey == 0x8002 ||  newKey == 0x8004 ||
                     newKey == 0x8006 || newKey == 0x8008)
                    newKey = ++rootConnection.lobHMKey;
                // Also roll over when the high bit of four byte locator is set.
                // This will prevent us from sending a negative locator to the
                // client. Don't allow zero since it is not a valid locator for the 
                // client.
                if (newKey == 0x80000000 || newKey == 0)
                    newKey = rootConnection.lobHMKey = 1;
                return newKey;
	}

    /**
     * Adds an entry of the lob in WeakHashMap. These entries are used
     * for cleanup during commit/rollback or close.
     * @param lobReference LOB Object
     */
    void addLOBReference (Object lobReference) {
        if (rootConnection.lobReferences == null) {
            rootConnection.lobReferences = new WeakHashMap ();
        }
        rootConnection.lobReferences.put (lobReference, null);
    }

	/**
	* Return the Hash Map in the root connection
	* @return the HashMap that contains the locator to LOB object mapping
	*/
	private HashMap getlobHMObj() {
		if (rootConnection.lobHashMap == null) {
			rootConnection.lobHashMap = new HashMap();
		}
		return rootConnection.lobHashMap;
	}

    /** Cancels the current running statement. */
    public void cancelRunningStatement() {
        getLanguageConnection().getStatementContext().cancel();
    }

    /**
     * Obtain the name of the current schema. Not part of the
     * java.sql.Connection interface, but is accessible through the
     * EngineConnection interface, so that the NetworkServer can get at the
     * current schema for piggy-backing
     * @return the current schema name
     */
    public String getCurrentSchemaName() {
        return getLanguageConnection().getCurrentSchemaName();
    }
    
    
	/**
	 * Add a temporary lob file to the lobFiles set.
	 * This will get closed at transaction end or removed as the lob is freed.
	 * @param lobFile  LOBFile to add
	 */
	void addLobFile(LOBFile lobFile) {
		synchronized (this) {
			if (lobFiles == null)
				lobFiles = new HashSet();
			lobFiles.add(lobFile);
		}
	}
    
	/**
	 * Remove LOBFile from the lobFiles set. This will occur when the lob 
	 * is freed or at transaction end if the lobFile was removed from the 
	 * WeakHashMap but not finalized.
	 * @param lobFile  LOBFile to remove.
	 */
	void removeLobFile(LOBFile lobFile) {
		synchronized (this) {
			lobFiles.remove(lobFile);
		}
	}

    /** Return true if the connection is aborting */
    public  boolean isAborting() { return aborting; }

    /** Begin aborting the connection */
    protected  void    beginAborting()
    {
        aborting = true;
        setInactive();
    }
        
    ////////////////////////////////////////////////////////////////////
    //
    // INTRODUCED BY JDBC 4.1 IN JAVA 7
    //
    ////////////////////////////////////////////////////////////////////

    /**
     * Get the name of the current schema.
     */
    public String   getSchema() throws SQLException
	{
		checkIfClosed();

		synchronized(getConnectionSynchronization())
		{
            setupContextStack();
			try {
                LanguageConnectionContext lcc = getLanguageConnection();
                return lcc.getCurrentSchemaName();
			} finally {
				restoreContextStack();
			}
		}
	}

    /**
     * Set the default schema for the Connection.
     */
    public void   setSchema(  String schemaName ) throws SQLException
	{
		checkIfClosed();

        PreparedStatement   ps = null;

        try {
            ps = prepareStatement( "set schema ?" );
            ps.setString( 1, schemaName );
            ps.execute();
        }
        finally
        {
            if ( ps != null ) { ps.close(); }
        }
	}

}
