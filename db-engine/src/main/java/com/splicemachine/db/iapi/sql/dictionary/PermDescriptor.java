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

import com.splicemachine.db.catalog.UUID;
import com.splicemachine.db.catalog.DependableFinder;
import com.splicemachine.db.catalog.Dependable;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.services.io.StoredFormatIds;
import com.splicemachine.db.iapi.sql.depend.Provider;

/**
 * This class describes rows in the SYS.SYSPERMS system table, which keeps track of the
 * permissions that have been granted but not revoked.
 */
public class PermDescriptor extends PermissionsDescriptor
        implements Provider
{
    // object types
    public static final String SEQUENCE_TYPE = "SEQUENCE";
    public static final String UDT_TYPE = "TYPE";
    public static final String AGGREGATE_TYPE = "DERBY AGGREGATE";
    
    // permissions
    public static final String USAGE_PRIV = "USAGE";

    // state
    
    private String objectType;
    private UUID permObjectId;
    private String permission;
    private boolean grantable;

    /**
     * Constructor
     *
     * @param dataDictionary data dictionary
     * @param permUUID       unique identification in time and space of this perm descriptor
     * @param objectType     E.g., SEQUENCE_TYPE
     * @param permObjectId   Unique id of the object being protected
     * @param permission     E.g., USAGE_PRIV
     * @param grantor        Authorization id which confers the privilege
     * @param grantee        Authorization id which receives the privilege
     * @param isGrantable    True if the privilege can be granted onwards
     */

    public PermDescriptor(DataDictionary dataDictionary, UUID permUUID, String objectType,
                          UUID permObjectId, String permission, String grantor,
                          String grantee, boolean isGrantable) {
        super(dataDictionary, grantee, grantor);
        setUUID(permUUID);
        this.objectType = objectType;
        this.permObjectId = permObjectId;
        this.permission = permission;
        this.grantable = isGrantable;
    }

    public PermDescriptor(DataDictionary dd, UUID permUUID)
            throws StandardException {
        /*
    TODO When merging all permisions catalogs to this master catalog at a future date,
    this pattern which uses a partially initialised key descriptor should be cleaned up.
     */
        this(dd, permUUID, null, null, null, null, null, false);
    }

    public String getObjectType() {
        return objectType;
    }

    public UUID getPermObjectId() {
        return permObjectId;
    }

    public String getPermission() {
        return permission;
    }

    public boolean isGrantable() {
        return grantable;
    }

    public int getCatalogNumber() {
        return DataDictionary.SYSPERMS_CATALOG_NUM;
    }

    public String toString() {
        if (SanityManager.DEBUG) {
            return "permUUID: " + oid + "\n" +
                    "objectType: " + objectType + "\n" +
                    "permObjectId: " + permObjectId + "\n" +
                    "permission: " + permission + "\n" +
                    "grantable: " + grantable + "\n";
        } else {
            return "";
        }
    }

    /**
     * @return true iff the key part of this perm descriptor equals the key part of another perm
     *         descriptor.
     */
    public boolean equals(Object other) {
        if (!(other instanceof PermDescriptor))
            return false;
        PermDescriptor otherPerm = (PermDescriptor) other;
        return super.keyEquals(otherPerm) &&
                permObjectId.equals(otherPerm.permObjectId);
    }

    /**
     * @return the hashCode for the key part of this permissions descriptor
     */
    public int hashCode() {
        return super.keyHashCode() + permObjectId.hashCode();
    }

    /**
     * @see PermissionsDescriptor#checkOwner
     */
    public boolean checkOwner( String authorizationId ) throws StandardException
    {
        DataDictionary dd = getDataDictionary();
        PrivilegedSQLObject pso = getProtectedObject( dd, permObjectId, objectType );
        
        return pso.getSchemaDescriptor().getAuthorizationId().equals(authorizationId);
    }

    /**
     * Get the protected object.
     *
     * @param dd Metadata
     * @param objectID Unique handle on the protected object
     * @param objectType Type of the object
     */
    public static PrivilegedSQLObject getProtectedObject
        ( DataDictionary dd, UUID objectID, String objectType ) throws StandardException
    {
        if ( PermDescriptor.SEQUENCE_TYPE.equals( objectType ) )
        {
            return dd.getSequenceDescriptor( objectID );
        }
        else if ( PermDescriptor.AGGREGATE_TYPE.equals( objectType ) )
        {
            return dd.getAliasDescriptor( objectID );
        }
        else if ( PermDescriptor.UDT_TYPE.equals( objectType ) )
        {
            return dd.getAliasDescriptor( objectID );
        }
        else
        {
            // oops, still need to implement support for this kind
            // of privileged object
            throw StandardException.newException( SQLState.BTREE_UNIMPLEMENTED_FEATURE );
        }
    }

    //////////////////////////////////////////////
    //
    // PROVIDER INTERFACE
    //
    //////////////////////////////////////////////

    /**
     * Return the name of this Provider.  (Useful for errors.)
     *
     * @return String   The name of this provider.
     */
    public String getObjectName()
    {
        try {
            DataDictionary dd = getDataDictionary();
            PrivilegedSQLObject pso = getProtectedObject( dd, permObjectId, objectType );
        
            return pso.getName();
        } catch (StandardException se) { return objectType; }
    }

    /**
     * Get the provider's type.
     *
     * @return char         The provider's type.
     */
    public String getClassType() {
        return Dependable.PERM;
    }

    /**
     * @return the stored form of this provider
     * @see Dependable#getDependableFinder
     */
    public DependableFinder getDependableFinder() {
        return getDependableFinder(
                StoredFormatIds.PERM_DESCRIPTOR_FINDER_V01_ID);
    }

}
