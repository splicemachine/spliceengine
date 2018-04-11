
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

package com.splicemachine.db.iapi.sql.dictionary;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.catalog.UUID;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.Activation;

/**
 * This class describes a routine execute permission
 * required by a statement.
 */

public final class StatementRoutinePermission extends StatementPermission
{
	private UUID routineUUID;

	public StatementRoutinePermission( UUID routineUUID)
	{
		this.routineUUID = routineUUID;
	}
									 
	/**
	 * Return routine UUID for this access descriptor
	 *
	 * @return	Routine UUID
	 */
	public UUID getRoutineUUID()
	{
		return routineUUID;
	}

	/**
	 * @see StatementPermission#check
	 */
	public void check( LanguageConnectionContext lcc,
					   boolean forGrant,
					   Activation activation) throws StandardException
	{
        genericCheck( lcc, forGrant, activation, "EXECUTE" );
	}

	/**
	 * @see StatementPermission#isCorrectPermission
	 */
    public boolean isCorrectPermission( PermissionsDescriptor raw )
    {
        if ( (raw == null) || !( raw instanceof RoutinePermsDescriptor) ) { return false; }

        RoutinePermsDescriptor pd = (RoutinePermsDescriptor) raw;
        
        return pd.getHasExecutePermission();
    }

	/**
	 * @see StatementPermission#getPrivilegedObject
	 */
    public PrivilegedSQLObject getPrivilegedObject( DataDictionary dd ) throws StandardException
    { return dd.getAliasDescriptor( routineUUID); }

	/**
	 * @see StatementPermission#getPermissionDescriptor
	 */
	public PermissionsDescriptor getPermissionDescriptor(String authid, DataDictionary dd)
	throws StandardException
	{
		return dd.getRoutinePermissions(routineUUID,authid);
	}

	/**
	 * @see StatementPermission#getObjectType
	 */
    public String getObjectType() { return "ROUTINE"; }

	public String toString()
	{
		return "StatementRoutinePermission: " + routineUUID;
	}

	@Override
	public Type getType() {
		return Type.ROUTINE;
	}

}
