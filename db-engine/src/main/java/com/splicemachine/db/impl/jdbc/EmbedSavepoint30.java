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

import com.splicemachine.db.iapi.reference.SQLState;

import com.splicemachine.db.iapi.error.StandardException;

import java.sql.Savepoint;
import java.sql.SQLException;

/**
 * This class implements the Savepoint interface from JDBC3.0
 * This allows to set, release, or rollback a transaction to
 * designated Savepoints. Savepoints provide finer-grained
 * control of transactions by marking intermediate points within
 * a transaction. Once a savepoint has been set, the transaction
 * can be rolled back to that savepoint without affecting preceding work.
   <P><B>Supports</B>
   <UL>
   <LI> JSR169 - no subsetting for java.sql.Savepoint
   <LI> JDBC 3.0 - class introduced in JDBC 3.0
   </UL>
 *
 * @see java.sql.Savepoint
 *
 */
final class EmbedSavepoint30 extends ConnectionChild
    implements Savepoint {

    //In order to avoid name conflict, the external names are prepanded
    //with "e." and internal names always start with "i." This is for bug 4467
    private final String savepointName;
    private final int savepointID;

	//////////////////////////////////////////////////////////////
	//
	// CONSTRUCTORS
	//
	//////////////////////////////////////////////////////////////
	/*
		Constructor assumes caller will setup context stack
		and restore it.
	    @exception SQLException on error
	 */
    EmbedSavepoint30(EmbedConnection conn, String name)
    throws StandardException {
   		super(conn);
   		if (name == null) //this is an unnamed savepoint
   		{
				//Generating a unique internal name for unnamed savepoints
				savepointName = "i." + conn.getLanguageConnection().getUniqueSavepointName();
				savepointID = conn.getLanguageConnection().getUniqueSavepointID();
   		} else
   		{
				savepointName = "e." + name;
				savepointID = -1;
   		}
   		conn.getLanguageConnection().languageSetSavePoint(savepointName, this);
    }

	/**
    *
    * Retrieves the generated ID for the savepoint that this Savepoint object
    * represents.
    *
    * @return the numeric ID of this savepoint
    * @exception SQLException if this is a named savepoint
    */
    public int getSavepointId() throws SQLException {
   		if (savepointID == -1)
			throw newSQLException(SQLState.NO_ID_FOR_NAMED_SAVEPOINT);
   		return savepointID;
    }

	/**
    *
    * Retrieves the name of the savepoint that this Savepoint object
    * represents.
    *
    * @return the name of this savepoint
    * @exception SQLException if this is an un-named savepoint
    */
    public String getSavepointName() throws SQLException {
   		if (savepointID != -1)
			throw newSQLException(SQLState.NO_NAME_FOR_UNNAMED_SAVEPOINT);
   		return savepointName.substring(2);
    }

    // Derby internally keeps name for both named and unnamed savepoints
    String getInternalName() {
   		return savepointName;
    }


    //bug 4468 - verify that savepoint rollback/release is for a savepoint from
    //the current connection
    boolean sameConnection(EmbedConnection con) {
   		return (getEmbedConnection().getLanguageConnection() == con.getLanguageConnection());
    }
}

