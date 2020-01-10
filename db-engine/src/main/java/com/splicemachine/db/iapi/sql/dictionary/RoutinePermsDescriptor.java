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

import com.splicemachine.db.catalog.Dependable;
import com.splicemachine.db.catalog.DependableFinder;
import com.splicemachine.db.catalog.UUID;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.StoredFormatIds;

/**
 * This class describes rows in the SYS.SYSROUTINEPERMS system table, which keeps track of the routine
 * (procedure and function) permissions that have been granted but not revoked.
 */
public class RoutinePermsDescriptor extends PermissionsDescriptor
{
    private UUID routineUUID;
    private String routineName;
    private boolean hasExecutePermission;
	
	public RoutinePermsDescriptor( DataDictionary dd,
                                   String grantee,
                                   String grantor,
                                   UUID routineUUID,
                                   boolean hasExecutePermission) throws StandardException
	{
        super (dd, grantee, grantor);
        this.routineUUID = routineUUID;
        this.hasExecutePermission = hasExecutePermission;
        //routineUUID can be null only if the constructor with routineePermsUUID
        //has been invoked.
        if (routineUUID != null) {
        	AliasDescriptor ad = dd.getAliasDescriptor(routineUUID);
        	if (ad != null)
        		routineName = ad.getObjectName();
		}
	}
	
	public RoutinePermsDescriptor( DataDictionary dd,
                                   String grantee,
                                   String grantor,
                                   UUID routineUUID) throws StandardException
	{
        this( dd, grantee, grantor, routineUUID, true);
	}

    /**
     * This constructor just sets up the key fields of a RoutinePermsDescriptor.
     */
	public RoutinePermsDescriptor( DataDictionary dd,
                                   String grantee,
                                   String grantor) throws StandardException
    {
        this( dd, grantee, grantor, (UUID) null);
    }
	   
    public RoutinePermsDescriptor( DataDictionary dd, UUID routineePermsUUID) 
    throws StandardException
	{
        this( dd, null, null, null, true);
        this.oid = routineePermsUUID;
	}
    
    public int getCatalogNumber()
    {
        return DataDictionary.SYSROUTINEPERMS_CATALOG_NUM;
    }
	
	/*----- getter functions for rowfactory ------*/
    public UUID getRoutineUUID() { return routineUUID;}
    public boolean getHasExecutePermission() { return hasExecutePermission;}

	public String toString()
	{
		return "routinePerms: grantee=" + getGrantee() + 
        ",routinePermsUUID=" + getUUID() +
          ",grantor=" + getGrantor() +
          ",routineUUID=" + getRoutineUUID();
	}		

    /**
     * @return true iff the key part of this permissions descriptor equals the key part of another permissions
     *         descriptor.
     */
    public boolean equals( Object other)
    {
        if( !( other instanceof RoutinePermsDescriptor))
            return false;
        RoutinePermsDescriptor otherRoutinePerms = (RoutinePermsDescriptor) other;
        return super.keyEquals( otherRoutinePerms) &&
          routineUUID.equals( otherRoutinePerms.routineUUID);
    }
    
    /**
     * @return the hashCode for the key part of this permissions descriptor
     */
    public int hashCode()
    {
        return super.keyHashCode() + routineUUID.hashCode();
    }
	
	/**
	 * @see PermissionsDescriptor#checkOwner
	 */
	public boolean checkOwner(String authorizationId) throws StandardException
	{
		UUID sd = getDataDictionary().getAliasDescriptor(routineUUID).getSchemaUUID();
		return getDataDictionary().getSchemaDescriptor(sd, null).getAuthorizationId().equals(authorizationId);
	}

	//////////////////////////////////////////////
	//
	// PROVIDER INTERFACE
	//
	//////////////////////////////////////////////

	/**
	 * Return the name of this Provider.  (Useful for errors.)
	 *
	 * @return String	The name of this provider.
	 */
	public String getObjectName()
	{
		return "Routine Privilege on " + routineName; 
	}

	/**
	 * Get the provider's type.
	 *
	 * @return char		The provider's type.
	 */
	public String getClassType()
	{
		return Dependable.ROUTINE_PERMISSION;
	}

	/**		
		@return the stored form of this provider

			@see Dependable#getDependableFinder
	 */
	public DependableFinder getDependableFinder() 
	{
        return getDependableFinder(
                StoredFormatIds.ROUTINE_PERMISSION_FINDER_V01_ID);
	}

	public String getRoutineName() {
		return routineName;
	}
}
