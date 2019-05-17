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

package com.splicemachine.db.impl.drda;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.Enumeration;
import java.util.Hashtable;
import com.splicemachine.db.iapi.tools.i18n.LocalizedResource;
import java.sql.SQLException;
import java.util.UUID;

/**
	Session stores information about the current session
	It is used so that a DRDAConnThread can work on any session.
*/
class Session
{

	// session states	   
	protected static final int INIT = 1;	// before exchange of server attributes
	protected static final int ATTEXC = 2;	// after first exchange of server attributes
	protected static final int SECACC = 3;	// after ACCSEC (Security Manager Accessed)
	protected static final int CHKSEC = 4;	// after SECCHK  (Checked Security)
	protected static final int CLOSED = 5;	// session has ended

	// session types
	protected static final int DRDA_SESSION = 1;
	protected static final int CMD_SESSION = 2;
	
	// trace name prefix and suffix
	private static final String TRACENAME_PREFIX = "Server";
	private static final String TRACENAME_SUFFIX = ".trace";

	// session information
	protected Socket clientSocket;		// session socket
	protected int connNum;				// connection number
	protected InputStream sessionInput;	// session input stream
	protected OutputStream sessionOutput;	// session output stream
	protected String traceFileName;		// trace file name for session
	protected boolean traceOn;			// whether trace is currently on for the session
	protected int state;				// the current state of the session
	protected int sessionType;			// type of session - DRDA or NetworkServerControl command
	protected String drdaID;			// DRDA ID of the session
	protected UUID uuid;			// UUID of the session, used for RDBINTTKN
	protected DssTrace dssTrace;		// trace object associated with the session
	protected AppRequester appRequester;	// Application requester for this session
	protected Database database;		// current database
	protected int qryinsid;				// unique identifier for each query
	protected LocalizedResource langUtil;		// localization information for command session
										// client

	private	Hashtable	dbtable;		// Table of databases accessed in this session
	private NetworkServerControlImpl nsctrl;        // NetworkServerControlImpl needed for logging
                                                        // message if tracing fails.
        private boolean enableOutboundCompression;

	private RemoteUser remoteUser;

	protected boolean canCompress()
 	{
 		return enableOutboundCompression;
 	}
 	
 	protected void enableCompress(boolean enable)
 	{
 		enableOutboundCompression = enable;
 	}
                                                        

	// constructor
	/**
	 * Session constructor
	 * 
	 * @param connNum		connection number
	 * @param clientSocket	communications socket for this session
	 * @param traceDirectory	location for trace files
	 * @param traceOn		whether to start tracing this connection
	 *
	 * @exception throws IOException
	 */
	Session (NetworkServerControlImpl nsctrl, int connNum, Socket clientSocket, String traceDirectory,
			boolean traceOn) throws Exception
	{
        this.nsctrl = nsctrl;
		this.connNum = connNum;
		this.clientSocket = clientSocket;
		this.traceOn = traceOn;
		if (traceOn)
			dssTrace = new DssTrace(); 
		dbtable = new Hashtable();
		this.uuid = UUID.randomUUID();
		initialize(traceDirectory);
	}

	/**
	 * Close session - close connection sockets and set state to closed
	 * 
	 */
	protected void close() throws SQLException
	{
		
		try {
			sessionInput.close();
			sessionOutput.close();
			clientSocket.close();
			setTraceOff();
			if (dbtable != null)
				for (Enumeration e = dbtable.elements() ; e.hasMoreElements() ;) 
				{
					((Database) e.nextElement()).close();
				}
			
		}catch (IOException e) {} // ignore IOException when we are shutting down
		finally {
			state = CLOSED;
			dbtable = null;
			database = null;
		}
	}

	/**
	 * initialize a server trace for the DRDA protocol
	 * 
	 * @param traceDirectory - directory for trace file
     * @param throwException - true if we should throw an exception if
     *                         turning on tracing fails.  We do this
     *                         for NetworkServerControl API commands.
	 * @throws IOException 
	 */
	protected void initTrace(String traceDirectory, boolean throwException)  throws Exception
	{
		if (traceDirectory != null)
			traceFileName = traceDirectory + "/" + TRACENAME_PREFIX+
				connNum+ TRACENAME_SUFFIX;
		else
			traceFileName = TRACENAME_PREFIX +connNum+ TRACENAME_SUFFIX;
		
		if (dssTrace == null)
			dssTrace = new DssTrace();
        try {
            dssTrace.startComBufferTrace(traceFileName);
            traceOn = true;
        } catch (Exception e) {   
            if (throwException) {
                throw e;
            }
            // If there is an error starting tracing for the session,
            // log to the console and db.log and do not turn tracing on.
            // let connection continue.
            nsctrl.consoleExceptionPrintTrace(e);
        }              
	}

	/**
	 * Set tracing on
	 * 
	 * @param traceDirectory 	directory for trace files
	 * @throws Exception 
	 */
	protected void setTraceOn(String traceDirectory, boolean throwException) throws Exception
	{
		if (traceOn)
			return;
		initTrace(traceDirectory, throwException);    
	}

	/**
	 * Get whether tracing is on 
	 *
	 * @return true if tracing is on false otherwise
	 */
	protected boolean isTraceOn()
	{
		return traceOn;
	}

	/**
	 * Get connection number
	 *
	 * @return connection number
	 */
	protected int getConnNum()
	{
		return connNum;
	}

	/**
	 * Set tracing off
	 * 
	 */
	protected void setTraceOff()
	{
		if (! traceOn)
			return;
		traceOn = false;
		if (traceFileName != null)
			dssTrace.stopComBufferTrace();
	}
	/**
	 * Add database to session table
	 */
	protected void addDatabase(Database d)
	{
		dbtable.put(d.getDatabaseName(), d);
	}

	/**
	 * Get database
	 */
	protected Database getDatabase(String dbName)
	{
		return (Database)dbtable.get(dbName);
	}

	/**
	 * Get requried security checkpoint.
	 * Used to verify EXCSAT/ACCSEC/SECCHK order.
	 *
	 *  @return next required Security checkpoint or -1 if 
	 *          neither ACCSEC or SECCHK are required at this time.
	 *
	 */
	protected int getRequiredSecurityCodepoint()
	{
		switch (state)
		{
			case ATTEXC:
				// On initial exchange of attributes we require ACCSEC 
				// to access security manager
				return CodePoint.ACCSEC;
			case SECACC:
				// After security manager has been accessed successfully we
				// require SECCHK to check security
				return CodePoint.SECCHK;
			default:
				return -1;
		}	 
	}

	/**
	 * Check if a security codepoint is required
	 *
	 * @return true if ACCSEC or SECCHK are required at this time.
	 */
	protected boolean requiresSecurityCodepoint()
	{
		return (getRequiredSecurityCodepoint() != -1);
	}

	/**
	 * Set Session state
	 * 
	 */
	protected void setState(int s)
	{
		state = s;
	}
	
	/**
	 * Get session into initial state
	 *
	 * @param traceDirectory	- directory for trace files
	 */
	private void initialize(String traceDirectory)
		throws Exception
	{
		sessionInput = clientSocket.getInputStream();
		sessionOutput = clientSocket.getOutputStream();
		if (traceOn)
			initTrace(traceDirectory,false);
		state = INIT;

                enableOutboundCompression = false;
	}

	protected  String buildRuntimeInfo(String indent, LocalizedResource localLangUtil)
	{
		String s = "";
		s += indent +  localLangUtil.getTextMessage("DRDA_RuntimeInfoSessionNumber.I")
			+ connNum + "\n";
		if (database == null)
			return s;
		s += database.buildRuntimeInfo(indent,localLangUtil);
		s += "\n";
		return s;
	}
	
	public RemoteUser getRemoteUser() {
		return remoteUser;
	}

	public void setRemoteUser(RemoteUser remoteUser) {
		this.remoteUser = remoteUser;
	}
}
