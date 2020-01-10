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
import com.splicemachine.db.iapi.sql.conn.StatementContext;

import com.splicemachine.db.jdbc.InternalDriver;

import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.reference.Limits;

import java.sql.*;

import java.util.Properties;
import java.util.concurrent.Executor;


/**
 * This class extends the EmbedConnection20 class in order to support new
 * methods and classes that come with JDBC 3.0.

   <P><B>Supports</B>
   <UL>
   <LI> JSR169 - Subsetting only removes getTypeMap and setTypeMap, which references
        java.util.Map which exists in Foundation and ee.miniumum. Thus the methods can
		safely be left in the implementation for JSR169.

  <LI> JDBC 3.0 - Separate from JDBC 2.0 implementation as JDBC 3.0 introduces
        a new class java.sql.Savepoint, which is referenced by java.sql.Connection.
   </UL>
 *
 * @see EmbedConnection
 *
 */
public class EmbedConnection30 extends EmbedConnection
{

	//////////////////////////////////////////////////////////
	// CONSTRUCTORS
	//////////////////////////////////////////////////////////

	public EmbedConnection30(
			InternalDriver driver,
			String url,
			Properties info)
		throws SQLException
	{
		super(driver, url, info);
	}

	public EmbedConnection30(
			EmbedConnection inputConnection)
	{
		super(inputConnection);
	}

 	/////////////////////////////////////////////////////////////////////////
	//
	//	JDBC 3.0	-	New public methods
	//
	/////////////////////////////////////////////////////////////////////////

	/**
	 * Creates an unnamed savepoint in the current transaction and
	 * returns the new Savepoint object that represents it.
	 *
	 *
	 * @return  The new Savepoint object
	 *
	 * @exception SQLException if a database access error occurs or
	 * this Connection object is currently in auto-commit mode
	 */
	public Savepoint setSavepoint()
		throws SQLException
	{
		return commonSetSavepointCode(null, false);
	}

	/**
	 * Creates a savepoint with the given name in the current transaction and
	 * returns the new Savepoint object that represents it.
	 *
	 *
	 * @param name  A String containing the name of the savepoint
	 *
	 * @return  The new Savepoint object
	 *
	 * @exception SQLException if a database access error occurs or
	 * this Connection object is currently in auto-commit mode
	 */
	public Savepoint setSavepoint(
			String name)
		throws SQLException
	{
		return commonSetSavepointCode(name, true);
	}

	/**
	 * Creates a savepoint with the given name(if it is a named savepoint else we will generate a name
	 * because Derby only supports named savepoints internally) in the current transaction and
	 * returns the new Savepoint object that represents it.
	 *
	 * @param name  A String containing the name of the savepoint. Will be null if this is an unnamed savepoint
	 * @param userSuppliedSavepointName  If true means it's a named user defined savepoint.
	 *
	 * @return  The new Savepoint object
	 */
	private Savepoint commonSetSavepointCode(String name, boolean userSuppliedSavepointName) throws SQLException
	{
		synchronized (getConnectionSynchronization()) {
			setupContextStack();
			try {
				verifySavepointCommandIsAllowed();
				if (userSuppliedSavepointName && (name == null))//make sure that if it is a named savepoint then supplied name is not null
					throw newSQLException(SQLState.NULL_NAME_FOR_SAVEPOINT);
				//make sure that if it is a named savepoint then supplied name length is not > 128
				if (userSuppliedSavepointName && (name.length() > Limits.MAX_IDENTIFIER_LENGTH))
					throw newSQLException(SQLState.LANG_IDENTIFIER_TOO_LONG, name, String.valueOf(Limits.MAX_IDENTIFIER_LENGTH));
				if (userSuppliedSavepointName && name.startsWith("SYS")) //to enforce DB2 restriction which is savepoint name can't start with SYS
					throw newSQLException(SQLState.INVALID_SCHEMA_SYS, "SYS");
				return new EmbedSavepoint30(this, name);
			} catch (StandardException e) {
				throw handleException(e);
			} finally {
			    restoreContextStack();
			}
		}
	}

	/**
	 * Undoes all changes made after the given Savepoint object was set.
	 * This method should be used only when auto-commit has been disabled.
	 *
	 *
	 * @param savepoint  The Savepoint object to rollback to
	 *
	 * @exception SQLException  if a database access error occurs,
	 * the Savepoint object is no longer valid, or this Connection
	 * object is currently in auto-commit mode
	 */
	public void rollback(
			Savepoint savepoint)
		throws SQLException
	{
		synchronized (getConnectionSynchronization()) {
			setupContextStack();
			try {
				verifySavepointCommandIsAllowed();
				verifySavepointArg(savepoint);
				//Need to cast and get the name because JDBC3 spec doesn't support names for
				//unnamed savepoints but Derby keeps names for named & unnamed savepoints.
				getLanguageConnection().internalRollbackToSavepoint(((EmbedSavepoint30)savepoint).getInternalName(),true, savepoint);
			} catch (StandardException e) {
				throw handleException(e);
			} finally {
			    restoreContextStack();
			}
		}
	}

	/**
	 * Removes the given Savepoint object from the current transaction.
	 * Any reference to the savepoint after it has been removed will cause
	 * an SQLException to be thrown
	 *
	 *
	 * @param savepoint  The Savepoint object to be removed
	 *
	 * @exception SQLException  if a database access error occurs or the
	 * given Savepoint object is not a valid savepoint in the current transaction
	 */
	public void releaseSavepoint(
			Savepoint savepoint)
		throws SQLException
	{
		synchronized (getConnectionSynchronization()) {
			setupContextStack();
			try {
				verifySavepointCommandIsAllowed();
				verifySavepointArg(savepoint);
				//Need to cast and get the name because JDBC3 spec doesn't support names for
				//unnamed savepoints but Derby keeps name for named & unnamed savepoints.
				getLanguageConnection().releaseSavePoint(((EmbedSavepoint30)savepoint).getInternalName(), savepoint);
			} catch (StandardException e) {
				throw handleException(e);
			} finally {
			    restoreContextStack();
			}
		}
	}

	// used by setSavepoint to check autocommit is false and not inside the trigger code
	private void verifySavepointCommandIsAllowed() throws SQLException
	{
		if (autoCommit)
			throw newSQLException(SQLState.NO_SAVEPOINT_WHEN_AUTO);

		//Bug 4507 - savepoint not allowed inside trigger
		StatementContext stmtCtxt = getLanguageConnection().getStatementContext();
		if (stmtCtxt!= null && stmtCtxt.inTrigger())
			throw newSQLException(SQLState.NO_SAVEPOINT_IN_TRIGGER);
	}

	// used by release/rollback to check savepoint argument
	private void verifySavepointArg(Savepoint savepoint) throws SQLException
	{
		//bug 4451 - Check for null savepoint
		EmbedSavepoint30 lsv = (EmbedSavepoint30) savepoint;
	    // bug 4451 need to throw error for null Savepoint
	    if (lsv == null)
		throw
		    Util.generateCsSQLException(SQLState.XACT_SAVEPOINT_NOT_FOUND, "null");

		//bug 4468 - verify that savepoint rollback is for a savepoint from the current
		// connection
		if (!lsv.sameConnection(this))
			throw newSQLException(SQLState.XACT_SAVEPOINT_RELEASE_ROLLBACK_FAIL);

    }

	//@Override
	public NClob createNClob() throws SQLException{
		throw Util.notImplemented("CreateNClob");
	}

	//@Override
	public SQLXML createSQLXML() throws SQLException{
		throw Util.notImplemented("createSQLXML");
	}

	//@Override
	public boolean isValid(int timeout) throws SQLException{
		throw Util.notImplemented("isValid");
	}

	//@Override
	public void setClientInfo(String name,String value) throws SQLClientInfoException{
		throw new UnsupportedOperationException();
	}

	//@Override
	public void setClientInfo(Properties properties) throws SQLClientInfoException{
		throw new UnsupportedOperationException();
	}

	//@Override
	public String getClientInfo(String name) throws SQLException{
		throw Util.notImplemented("getClientInfo");
	}

	//@Override
	public Properties getClientInfo() throws SQLException{
		throw Util.notImplemented("getClientInfo");
	}

	//@Override
	public Array createArrayOf(String typeName,Object[] elements) throws SQLException{
		throw Util.notImplemented("createArrayOf");
	}

	//@Override
	public Struct createStruct(String typeName,Object[] attributes) throws SQLException{
		throw Util.notImplemented("createStruct");
	}

	//@Override
	public void abort(Executor executor) throws SQLException{
		throw Util.notImplemented("abort");
	}

	//@Override
	public void setNetworkTimeout(Executor executor,int milliseconds) throws SQLException{
		throw Util.notImplemented("setNetworkTimeout");
	}

	//@Override
	public int getNetworkTimeout() throws SQLException{
		throw Util.notImplemented("getNetworkTimeout");
	}

	//@Override
	public <T> T unwrap(Class<T> iface) throws SQLException{
		throw Util.notImplemented("unwrap");
	}

	//@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException{
		throw Util.notImplemented("isWrapperFor");
	}
}
