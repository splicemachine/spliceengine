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

package com.splicemachine.db.iapi.sql.dictionary;

import com.splicemachine.db.catalog.Dependable;
import com.splicemachine.db.catalog.DependableFinder;
import com.splicemachine.db.catalog.UUID;

import com.splicemachine.db.iapi.services.io.StoredFormatIds;
import com.splicemachine.db.iapi.error.StandardException;

/**
 * This class describes a row in the SYS.SYSTABLEPERMS system table, which
 * stores the table permissions that have been granted but not revoked.
 */
public class TablePermsDescriptor extends PermissionsDescriptor
{
    private UUID tableUUID;
    private String tableName;
    private String selectPriv;
    private String deletePriv;
    private String insertPriv;
    private String updatePriv;
    private String referencesPriv;
    private String triggerPriv;
	
	public TablePermsDescriptor( DataDictionary dd,
                                 String grantee,
                                 String grantor,
                                 UUID tableUUID,
                                 String selectPriv,
                                 String deletePriv,
                                 String insertPriv,
                                 String updatePriv,
                                 String referencesPriv,
                                 String triggerPriv) throws StandardException
	{
		super (dd, grantee, grantor);
        this.tableUUID = tableUUID;
        this.selectPriv = selectPriv;
        this.deletePriv = deletePriv;
        this.insertPriv = insertPriv;
        this.updatePriv = updatePriv;
        this.referencesPriv = referencesPriv;
        this.triggerPriv = triggerPriv;
        //tableUUID can be null only if the constructor with tablePermsUUID
        //has been invoked.
        if (tableUUID != null)
        	tableName = dd.getTableDescriptor(tableUUID).getName();
	}

    /**
     * This constructor just sets up the key fields of a TablePermsDescriptor
     */
    public TablePermsDescriptor( DataDictionary dd,
                                 String grantee,
                                 String grantor,
                                 UUID tableUUID) throws StandardException
    {
        this( dd, grantee, grantor, tableUUID,
              (String) null, (String) null, (String) null, (String) null, (String) null, (String) null);
    }
    
    public TablePermsDescriptor( DataDictionary dd,
            UUID tablePermsUUID) throws StandardException
            {
        this( dd, null, null, null,
                (String) null, (String) null, (String) null, (String) null, (String) null, (String) null);
        this.oid = tablePermsUUID;
			}

    public int getCatalogNumber()
    {
        return DataDictionary.SYSTABLEPERMS_CATALOG_NUM;
    }
	
	/*----- getter functions for rowfactory ------*/
    public UUID getTableUUID() { return tableUUID;}
    public String getSelectPriv() { return selectPriv;}
    public String getDeletePriv() { return deletePriv;}
    public String getInsertPriv() { return insertPriv;}
    public String getUpdatePriv() { return updatePriv;}
    public String getReferencesPriv() { return referencesPriv;}
    public String getTriggerPriv() { return triggerPriv;}

	public String toString()
	{
		return "tablePerms: grantee=" + getGrantee() +
		",tablePermsUUID=" + getUUID() +
			",grantor=" + getGrantor() +
          ",tableUUID=" + getTableUUID() +
          ",selectPriv=" + getSelectPriv() +
          ",deletePriv=" + getDeletePriv() +
          ",insertPriv=" + getInsertPriv() +
          ",updatePriv=" + getUpdatePriv() +
          ",referencesPriv=" + getReferencesPriv() +
          ",triggerPriv=" + getTriggerPriv();
	}

    /**
     * @return true iff the key part of this permissions descriptor equals the key part of another permissions
     *         descriptor.
     */
    public boolean equals( Object other)
    {
        if( !( other instanceof TablePermsDescriptor))
            return false;
        TablePermsDescriptor otherTablePerms = (TablePermsDescriptor) other;
        return super.keyEquals( otherTablePerms) && tableUUID.equals( otherTablePerms.tableUUID);
    }
    
    /**
     * @return the hashCode for the key part of this permissions descriptor
     */
    public int hashCode()
    {
    	return super.keyHashCode() + tableUUID.hashCode();
    }
	
	/**
	 * @see PermissionsDescriptor#checkOwner
	 */
	public boolean checkOwner(String authorizationId) throws StandardException
	{
		TableDescriptor td = getDataDictionary().getTableDescriptor(tableUUID);
		if (td.getSchemaDescriptor().getAuthorizationId().equals(authorizationId))
			return true;
		else
			return false;
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
		return "Table Privilege on " + tableName; 
	}

	/**
	 * Get the provider's type.
	 *
	 * @return char		The provider's type.
	 */
	public String getClassType()
	{
		return Dependable.TABLE_PERMISSION;
	}

	/**		
		@return the stored form of this provider

			@see Dependable#getDependableFinder
	 */
	public DependableFinder getDependableFinder() 
	{
        return getDependableFinder(
                StoredFormatIds.TABLE_PERMISSION_FINDER_V01_ID);
	}
}
