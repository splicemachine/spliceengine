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

import com.splicemachine.db.iapi.error.ErrorStringBuilder;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.i18n.MessageService;

import com.splicemachine.db.iapi.services.property.PropertyUtil;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.services.stream.HeaderPrintWriter;
import com.splicemachine.db.iapi.services.io.StoredFormatIds;
import com.splicemachine.db.iapi.services.monitor.Monitor;
import com.splicemachine.db.iapi.types.TypeId;

import com.splicemachine.db.iapi.error.ExceptionSeverity;

import com.splicemachine.db.iapi.reference.Property;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.reference.MessageId;
import com.splicemachine.db.iapi.reference.JDBC40Translation;

import java.sql.SQLException;
import java.sql.Types;
import java.io.IOException;

/**
	This class understands the message protocol and looks up
	SQLExceptions based on keys, so that the Local JDBC driver's
	messages can be localized.

	REMIND: May want to investigate putting some of this in the protocol
	side, for the errors that any Derby JDBC driver might return.

	The ASSERT mechanism is a wrapper of the basic services,
	to ensure that failed asserts at this level will behave
	well in a JDBC environment.

*/
//In the past, this class was sent on the wire to the client and because it
//has the message protcol stuff and also the detailed stack trace as one
//of it's member variable, the client.jar files was really big. To get
//around this problem, now we have added EmbedSQLException which is
//just a java sql exception with the stack trace information variable
//transient so it doesn't get transported to the client side and thus
//reducing the size of client.jar The bug for this fix was 1850. The
//p4 number for it will have the details of all the files impacted and
//the actual changes made.
public abstract class Util  {


    private static SQLExceptionFactory exceptionFactory = 
                                    new SQLExceptionFactory ();


	private static int logSeverityLevel = PropertyUtil.getSystemInt(Property.LOG_SEVERITY_LEVEL,
		SanityManager.DEBUG ? 0 : ExceptionSeverity.SESSION_SEVERITY);
	/*
	** Methods of Throwable
	*/

	// class implementation

    /**
     * Log SQLException to the error log if the severity exceeds the 
     * logSeverityLevel  and then throw it.  This method can be used for 
     * logging JDBC exceptions to db.log DERBY-1191.
     * 
     * @param se SQLException to log and throw
     * @throws SQLException
     */
    public static void logAndThrowSQLException(SQLException se) throws SQLException {
    	if (se.getErrorCode() >= logSeverityLevel){
    	logSQLException(se);
    	}
    	throw se;
    }
    
	/**
	 * Log an SQLException to the error log or to the console if there is no
	 * error log available.
	 * This method could perhaps be optimized to have a static shared
	 * ErrorStringBuilder and synchronize the method, but this works for now.
	 * 
	 * @param se SQLException to log
	 */
	public static void logSQLException(SQLException se) {
		if (se == null)
			return;
		logException(se, se.getMessage());
    }

	/**
	 * Log an StandardException to the error log or to the console if there is no
	 * error log available.
	 * This method could perhaps be optimized to have a static shared
	 * ErrorStringBuilder and synchronize the method, but this works for now.
	 *
	 * @param se SQLException to log
	 */
	public static void logSQLException(StandardException se) {
    	if (se == null)
    		return;
    	logException(se, se.getMessage());
    }

	/**
	 * Log an SQLException to the error log or to the console if there is no
	 * error log available.
	 * This method could perhaps be optimized to have a static shared
	 * ErrorStringBuilder and synchronize the method, but this works for now.
	 *
	 * @param se SQLException to log
	 */
	private static void logException(Exception se, String sqlstate) {
    	if (se == null)
    		return;
    	String message = se.getMessage();
    	if ((sqlstate != null) && (sqlstate.equals(SQLState.LOGIN_FAILED)) &&
    			(message != null) && (message.equals("Connection refused : java.lang.OutOfMemoryError")))
    		return;

    	HeaderPrintWriter errorStream = Monitor.getStream();
    	if (errorStream == null) {
    		se.printStackTrace();
    		return;
    	}
    	ErrorStringBuilder	errorStringBuilder = new ErrorStringBuilder(errorStream.getHeader());
    	errorStringBuilder.append("\nERROR " +  sqlstate + ": "  + se.getMessage());
    	errorStringBuilder.stackTrace(se);
    	errorStream.println(errorStringBuilder.get().toString());
    	errorStringBuilder.reset();
    }

	
	/**
     * This looks up the message and sqlstate values and calls
     * the SQLExceptionFactory method to generate
     * the appropriate exception off of them.
     */

	private static SQLException newEmbedSQLException(String messageId,
			Object[] args, SQLException next, int severity, Throwable t) {
        String message = MessageService.getCompleteMessage
                                        (messageId, args);
        return exceptionFactory.getSQLException (
			    message, messageId, next, severity, t, args);
	}

	public static SQLException newEmbedSQLException(String messageId,
			Object[] args, int severity) {
		return newEmbedSQLException(messageId, args, (SQLException) null, severity, (Throwable) null);
	}

	private static SQLException newEmbedSQLException(String messageId,
			Object[] args, int severity, Throwable t) {
		return newEmbedSQLException(messageId,args, (SQLException)  null, severity, t);
	}

	private static SQLException newEmbedSQLException(
			String messageId, int severity) {
		return newEmbedSQLException(messageId, (Object[]) null, (SQLException) null, severity, (Throwable) null);
	}

	// class interface


	/**
		Mimic SanityManager.ASSERT in a JDBC-friendly way,
		and providing system cleanup for JDBC failures.
		We need the connection to do cleanup...

		@exception SQLException the exception
	 */
	public static void ASSERT(EmbedConnection conn, boolean mustBeTrue, String msg) throws SQLException {
		if (SanityManager.DEBUG) {
			try {
				SanityManager.ASSERT(mustBeTrue, msg);
			} catch (Throwable t) {
				SQLException se = conn.handleException(t);
				// get around typing constraints.
				// it must be a Util, we wrapped it.
				SanityManager.ASSERT(se instanceof EmbedSQLException);
				throw (EmbedSQLException)se;
			}
		}
	}

	/**
		Mimic SanityManager.THROWASSERT in a JDBC-friendly way,
		and providing system cleanup for JDBC failures.
		We need the connection to do cleanup...
	 */
	static void THROWASSERT(EmbedConnection conn, String msg) throws SQLException {
		if (SanityManager.DEBUG) {
			try {
				SanityManager.THROWASSERT(msg);
			} catch (Throwable t) {
				SQLException se = conn.handleException(t);
				// get around typing constraints.
				// it must be a Util, we wrapped it.
				SanityManager.ASSERT(se instanceof EmbedSQLException);
				throw (EmbedSQLException)se;
			}
		}
	}

	/*
	** There is at least one static method for each message id.
	** Its parameters are specific to its message.
	** These will throw SQLException when the message repository
	** cannot be located.
	** Note that these methods call the static method newEmbedSQLException,
	** they don't directly do a new Util.
	*/

	/* 3 arguments */
	static SQLException newException(String messageID, Object a1,
			Object a2, Object a3) {
		return newEmbedSQLException(messageID, new Object[] {a1, a2, a3},
        		StandardException.getSeverityFromIdentifier(messageID));
	}


	public static SQLException generateCsSQLException(String error) {
		return newEmbedSQLException(error,
        		StandardException.getSeverityFromIdentifier(error));
	}

	public static SQLException generateCsSQLException(String error, Object arg1)     {
		return newEmbedSQLException(error,
			new Object[] {arg1},
                StandardException.getSeverityFromIdentifier(error));
	}

	public static SQLException generateCsSQLException(
                             String error, Object arg1, Object arg2){
		return newEmbedSQLException(error,
			new Object[] {arg1, arg2},
                StandardException.getSeverityFromIdentifier(error));
	}

	public static SQLException generateCsSQLException(
		String error, Object arg1, Object arg2, Object arg3) {

		return newEmbedSQLException(error,
			new Object[] {arg1, arg2, arg3},
                StandardException.getSeverityFromIdentifier(error));
	}


	static SQLException generateCsSQLException(
                    String error, Object arg1, Throwable t) {
		return newEmbedSQLException(error,
			new Object[] {arg1},
                StandardException.getSeverityFromIdentifier(error), t);
	}

	public static SQLException generateCsSQLException(StandardException se) {
        return exceptionFactory.getSQLException(
                se.getMessage(), se.getMessageId(), (SQLException) null,
                se.getSeverity(), se, se.getArguments());
    }

	public static SQLException noCurrentConnection() {
		return newEmbedSQLException(SQLState.NO_CURRENT_CONNECTION,
        		StandardException.getSeverityFromIdentifier(SQLState.NO_CURRENT_CONNECTION));
	}

    /**
     * Generate an <code>SQLException</code> which points to another
     * <code>SQLException</code> nested within it with
     * <code>setNextException()</code>.
     *
     * @param messageId message id
     * @param args the arguments to the message creation
     * @param next the next SQLException
     * @return an SQLException wrapping another SQLException
     */
    static SQLException seeNextException(String messageId, Object[] args,
                                         SQLException next) {
        return newEmbedSQLException(messageId, args, next,
            StandardException.getSeverityFromIdentifier(messageId), null);
    }

	public static SQLException javaException(Throwable t) {
		String name, msg;

		msg = t.getMessage();
		if (msg == null) msg = "";
		name = t.getClass().getName();
        SQLException next = null;
        Throwable cause = t.getCause();
        if (cause != null) {
            if (cause instanceof SQLException) {
                next = (SQLException) cause;
            } else if (cause instanceof StandardException) {
                next = generateCsSQLException((StandardException) cause);
            } else {
                next = javaException(cause);
            }
        }
		return newEmbedSQLException(SQLState.JAVA_EXCEPTION,
                new Object[] {name, msg}, next,
                ExceptionSeverity.NO_APPLICABLE_SEVERITY, t);
	}


	public static SQLException policyNotReloaded( Throwable t ) {
		return newEmbedSQLException(SQLState.POLICY_NOT_RELOADED, new Object[] { t.getMessage() },
        		StandardException.getSeverityFromIdentifier(SQLState.POLICY_NOT_RELOADED), t);
	}

	public static SQLException notImplemented() {

		return notImplemented( MessageService.getTextMessage(MessageId.CONN_NO_DETAILS) );
	}

	public static SQLException notImplemented(String feature) {

		return newEmbedSQLException(SQLState.NOT_IMPLEMENTED,
			new Object[] {feature},
                StandardException.getSeverityFromIdentifier(SQLState.NOT_IMPLEMENTED));
	}

	static SQLException setStreamFailure(IOException e) {
		String msg;

		msg = e.getMessage();
		if (msg == null) 
			msg = e.getClass().getName();
        return generateCsSQLException(SQLState.SET_STREAM_FAILURE, msg, e);
	}

	static SQLException typeMisMatch(int targetSQLType) {
		return newEmbedSQLException(SQLState.TYPE_MISMATCH,
			new Object[] {typeName(targetSQLType)},
                StandardException.getSeverityFromIdentifier(SQLState.TYPE_MISMATCH));
	}

    /**
     * Create an {@code IOException} that wraps another {@code Throwable}.
     *
     * @param cause the underlying cause of the error
     * @return an {@code IOException} linked to {@code cause}
     */
    static IOException newIOException(Throwable cause) {
        return new IOException(cause.getMessage(), cause);
    }

    /**
     * this method is called to replace the exception factory to be 
     * used to generate the SQLException or the subclass
     */

    public static void setExceptionFactory (SQLExceptionFactory factory) {
        exceptionFactory = factory;
    }

    /**
     * Get the exception factory specific to the version of JDBC which
	 * we are running.
     */
	public	static	SQLExceptionFactory	getExceptionFactory() { return exceptionFactory; }

	public static String typeName(int jdbcType) {
		switch (jdbcType) {
			case Types.ARRAY: return TypeId.ARRAY_NAME;
			case Types.BIT 		:  return TypeId.BIT_NAME;
			case Types.BOOLEAN  : return TypeId.BOOLEAN_NAME;
			case Types.DATALINK: return TypeId.DATALINK_NAME;
			case Types.TINYINT 	:  return TypeId.TINYINT_NAME;
			case Types.SMALLINT	:  return TypeId.SMALLINT_NAME;
			case Types.INTEGER 	:  return TypeId.INTEGER_NAME;
			case Types.BIGINT 	:  return TypeId.LONGINT_NAME;

			case Types.FLOAT 	:  return TypeId.FLOAT_NAME;
			case Types.REAL 	:  return TypeId.REAL_NAME;
			case Types.DOUBLE 	:  return TypeId.DOUBLE_NAME;

			case Types.NUMERIC 	:  return TypeId.NUMERIC_NAME;
			case Types.DECIMAL	:  return TypeId.DECIMAL_NAME;

			case Types.CHAR		:  return TypeId.CHAR_NAME;
			case Types.VARCHAR 	:  return TypeId.VARCHAR_NAME;
			case Types.LONGVARCHAR 	:  return "LONGVARCHAR";
            case Types.CLOB     :  return TypeId.CLOB_NAME;

			case Types.DATE 		:  return TypeId.DATE_NAME;
			case Types.TIME 		:  return TypeId.TIME_NAME;
			case Types.TIMESTAMP 	:  return TypeId.TIMESTAMP_NAME;

			case Types.BINARY			:  return TypeId.BINARY_NAME;
			case Types.VARBINARY	 	:  return TypeId.VARBINARY_NAME;
			case Types.LONGVARBINARY 	:  return TypeId.LONGVARBINARY_NAME;
            case Types.BLOB             :  return TypeId.BLOB_NAME;

			case Types.OTHER		:  return "OTHER";
			case Types.JAVA_OBJECT	:  return "Types.JAVA_OBJECT";
			case Types.REF : return TypeId.REF_NAME;
			case JDBC40Translation.ROWID: return TypeId.ROWID_NAME;
			case Types.STRUCT: return TypeId.STRUCT_NAME;
			case StoredFormatIds.XML_TYPE_ID :  return TypeId.XML_NAME;
			case JDBC40Translation.SQLXML: return TypeId.SQLXML_NAME;
			default : return String.valueOf(jdbcType);
		}
	}
}
