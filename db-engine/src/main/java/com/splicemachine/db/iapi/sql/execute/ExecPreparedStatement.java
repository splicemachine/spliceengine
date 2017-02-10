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

package com.splicemachine.db.iapi.sql.execute;

import com.splicemachine.db.iapi.services.loader.GeneratedClass;

import com.splicemachine.db.iapi.error.StandardException;

import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;

import com.splicemachine.db.iapi.sql.PreparedStatement;
import com.splicemachine.db.iapi.sql.ResultColumnDescriptor;

import java.util.List;

/**
 * Execution extends prepared statement to add methods it needs
 * for execution purposes (that should not be on the Database API).
 *
 */
public interface ExecPreparedStatement 
	extends PreparedStatement {

	/**
	 * set the statement text
	 *
	 * @param txt the source text
	 */
	void setSource(String txt);

    String getSource();

	/**
	 *	Get the Execution constants. This routine is called at Execution time.
	 *
	 *	@return	ConstantAction	The big structure enclosing the Execution constants.
	 */
	ConstantAction	getConstantAction( );

	/**
	 *	Get a saved object by number.  This is called during execution to
	 *  access objects created at compile time.  These are meant to be
	 *  read-only at run time.
	 *
	 *	@return	Object	A saved object.  The caller has to know what
	 *	it is requesting and cast it back to the expected type.
	 */
	Object	getSavedObject(int objectNum);

	/**
	 *	Get all the saved objects.  Used for stored prepared
	 * 	statements.
	 *
	 *	@return	Object[]	the saved objects
	 */
	Object[]	getSavedObjects();

	/**
	 *	Get the saved cursor info.  Used for stored prepared
	 * 	statements.
	 *
	 *	@return	Object	the cursor info
	 */
	Object	getCursorInfo();

	/**
	 *  Get the class generated for this prepared statement.
	 *  Used to confirm compatability with auxilary structures.
	 *
	 * @exception StandardException on error obtaining class
	 *	(probably when a stored prepared statement is loading)
	 */
	GeneratedClass getActivationClass() throws StandardException;

    /**
     * <p>
     * Checks whether this PreparedStatement is up to date and its activation
     * class is identical to the supplied generated class. A call to {@code
     * upToDate(gc)} is supposed to perform the same work as the following code
     * in one atomic operation:
     * </p>
     *
     * <pre>
     * getActivationClass() == gc && upToDate()
     * </pre>
     *
     * @param gc a generated class that must be identical to {@code
     * getActivationClass()} for this method to return {@code true}
     * @return {@code true} if this statement is up to date and its activation
     * class is identical to {@code gc}, {@code false} otherwise
     * @see PreparedStatement#upToDate()
     * @see #getActivationClass()
     */
    boolean upToDate(GeneratedClass gc) throws StandardException;

	/**
	 *  Mark the statement as unusable, i.e. the system is
	 * finished with it and no one should be able to use it.
	 */
	void finish(LanguageConnectionContext lcc);

	/**
	 * Does this statement need a savpoint
	 *
	 * @return true if needs a savepoint
	 */
	boolean needsSavepoint();

	/**
	 * Get a new prepared statement that is a shallow copy
	 * of the current one.
	 *
	 * @return a new prepared statement
	 *
	 * @exception StandardException on error 
	 */
	public ExecPreparedStatement getClone() throws StandardException;

	/* Methods from old CursorPreparedStatement */

	/**
	 * the update mode of the cursor
	 *
	 * @return	The update mode of the cursor
	 */
	int	getUpdateMode();

	/**
	 * the target table of the cursor
	 *
	 * @return	target table of the cursor
	 */
	ExecCursorTableReference getTargetTable();

	/**
	 * the target columns of the cursor; this is a superset of
	 * the updatable columns, describing the row available
	 *
	 * @return	target columns of the cursor as an array of column descriptors
	 */
	ResultColumnDescriptor[]	getTargetColumns();

	/**
	 * the update columns of the cursor
	 *
	 * @return	update columns of the cursor as a string of column names
	 */
	String[]	getUpdateColumns();

	/**
	 * set this parepared statement to be valid
	 */
	void setValid();

	/**
	 * Indicate that the statement represents an SPS action
	 */
	void setSPSAction();

	/**
	 * @return the list of permissions required to execute this statement. May be null if
	 *         the database does not use SQL standard authorization
	 */
	List getRequiredPermissionsList();
}

