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

package com.splicemachine.db.iapi.sql.dictionary;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.catalog.UUID;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.sql.Activation;

/**
 * This class describes a generic permission (such as USAGE)
 * required by a statement.
 */

public final class StatementGenericPermission extends StatementPermission
{
	private UUID _objectID;
    private String _objectType; // e.g., PermDescriptor.SEQUENCE_TYPE
    private String _privilege; // e.g., PermDescriptor.USAGE_PRIV

	public StatementGenericPermission( UUID objectID, String objectType, String privilege )
	{
		_objectID = objectID;
        _objectType = objectType;
        _privilege = privilege;
	}

    // accessors
	public UUID getObjectID() { return _objectID; }
    public String getPrivilege() { return _privilege; }

	/**
	 * @see StatementPermission#getObjectType
	 */
    public String getObjectType() { return _objectType; }

	/**
	 * @see StatementPermission#check
	 */
	public void check( LanguageConnectionContext lcc,
					   boolean forGrant,
					   Activation activation) throws StandardException
	{
        genericCheck( lcc, forGrant, activation, _privilege );
	}


	/**
	 * @see StatementPermission#isCorrectPermission
	 */
    public boolean isCorrectPermission( PermissionsDescriptor raw )
    {
        if ( (raw == null) || !( raw instanceof PermDescriptor) ) { return false; }

        PermDescriptor pd = (PermDescriptor) raw;
        
        return
            pd.getPermObjectId().equals( _objectID ) &&
            pd.getObjectType().equals( _objectType ) &&
            pd.getPermission().equals( _privilege )
            ;
    }

	/**
	 * @see StatementPermission#getPrivilegedObject
	 */
    public PrivilegedSQLObject getPrivilegedObject( DataDictionary dd ) throws StandardException
    {
        if ( PermDescriptor.UDT_TYPE.equals( _objectType ) ) { return dd.getAliasDescriptor( _objectID ); }
        else if ( PermDescriptor.AGGREGATE_TYPE.equals( _objectType ) ) { return dd.getAliasDescriptor( _objectID ); }
        else if ( PermDescriptor.SEQUENCE_TYPE.equals( _objectType ) ) { return dd.getSequenceDescriptor( _objectID ); }
        else
        {
            throw StandardException.newException( SQLState.BTREE_UNIMPLEMENTED_FEATURE );
        }
    }

	/**
	 * @see StatementPermission#getPermissionDescriptor
	 */
	public PermissionsDescriptor getPermissionDescriptor(String authid, DataDictionary dd)
	throws StandardException
	{
		return dd.getGenericPermissions( _objectID, _objectType, _privilege, authid );
	}


	public String toString()
	{
		return "StatementGenericPermission( " + _objectID + ", " + _objectType + ", " + _privilege + " )";
	}

	@Override
	public Type getType() {
		return Type.GENERIC;
	}


}
