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
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.services.io.StoredFormatIds;

/**
 * This class describes a row in the SYS.SYSCOLPERMS system table, which keeps
 * the column permissions that have been granted but not revoked.
 */
public class ColPermsDescriptor extends PermissionsDescriptor
{
    private UUID tableUUID;
    private String type;
    private FormatableBitSet columns;
    private String tableName;
	
	public ColPermsDescriptor( DataDictionary dd,
			                   String grantee,
                               String grantor,
                               UUID tableUUID,
                               String type,
                               FormatableBitSet columns) throws StandardException
	{
		super (dd, grantee, grantor);
        this.tableUUID = tableUUID;
        this.type = type;
        this.columns = columns;
        //tableUUID can be null only if the constructor with colPermsUUID
        //has been invoked.
        if (tableUUID != null)
        	tableName = dd.getTableDescriptor(tableUUID).getName();
	}

    /**
     * This constructor just initializes the key fields of a ColPermsDescriptor
     */
	public ColPermsDescriptor( DataDictionary dd,
                               String grantee,
                               String grantor,
                               UUID tableUUID,
                               String type) throws StandardException
    {
        this( dd, grantee, grantor, tableUUID, type, (FormatableBitSet) null);
    }           
    
    public ColPermsDescriptor( DataDictionary dd,
            UUID colPermsUUID) throws StandardException
    {
        super(dd,null,null);
        this.oid = colPermsUUID;
	}
    
    public int getCatalogNumber()
    {
        return DataDictionary.SYSCOLPERMS_CATALOG_NUM;
    }
	
	/*----- getter functions for rowfactory ------*/
    public UUID getTableUUID() { return tableUUID;}
    public String getType() { return type;}
    public FormatableBitSet getColumns() { return columns;}

	public String toString()
	{
		return "colPerms: grantee=" + getGrantee() + 
        ",colPermsUUID=" + getUUID() +
			",grantor=" + getGrantor() +
          ",tableUUID=" + getTableUUID() +
          ",type=" + getType() +
          ",columns=" + getColumns();
	}		

	/**
     * @return true iff the key part of this permissions descriptor equals the key part of another permissions
     *         descriptor.
     */
    public boolean equals( Object other)
    {
        if( !( other instanceof ColPermsDescriptor))
            return false;
        ColPermsDescriptor otherColPerms = (ColPermsDescriptor) other;
        return super.keyEquals( otherColPerms) &&
          tableUUID.equals( otherColPerms.tableUUID) &&
          ((type == null) ? (otherColPerms.type == null) : type.equals( otherColPerms.type));
    }
    
    /**
     * @return the hashCode for the key part of this permissions descriptor
     */
    public int hashCode()
    {
    	return super.keyHashCode() + tableUUID.hashCode() +
		((type == null) ? 0 : type.hashCode());
    }
	
	/**
	 * @see PermissionsDescriptor#checkOwner
	 */
	public boolean checkOwner(String authorizationId) throws StandardException
	{
		TableDescriptor td = getDataDictionary().getTableDescriptor(tableUUID);
		return td.getSchemaDescriptor().getAuthorizationId().equals(authorizationId);
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
		return "Column Privilege on " + tableName; 
	}

	/**
	 * Get the provider's type.
	 *
	 * @return char		The provider's type.
	 */
	public String getClassType()
	{
		return Dependable.COLUMNS_PERMISSION;
	}

	/**		
		@return the stored form of this provider

			@see Dependable#getDependableFinder
	 */
	public DependableFinder getDependableFinder() 
	{
        return getDependableFinder(
                StoredFormatIds.COLUMNS_PERMISSION_FINDER_V01_ID);
	}

}
