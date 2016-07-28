package com.splicemachine.colperms;

import com.splicemachine.db.catalog.UUID;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.dictionary.ColPermsDescriptor;
import com.splicemachine.db.impl.sql.catalog.DataDictionaryImpl;

/**
 *
 *
 * Interface corresponding to the Data Dictionary's getColumnPermissions request.  This
 * should retun ColPermsDescriptor when Splice Machine's enterprise features are enabled and
 * null when they are not enabled.
 *
 */
public interface ColPermsManager {
    /**
     *
     * Get the Column Permissions based on the UUID and the DataDictionary
     *
     * @param dd
     * @param colPermsUUID
     * @return
     * @throws StandardException
     */
    ColPermsDescriptor getColumnPermissions(DataDictionaryImpl dd, UUID colPermsUUID) throws StandardException;

    /**
     *
     * Get the Column Permissions based on the table UUID, priviledge Type, forGrant, and authorizationID.
     *
     * @param dd
     * @param tableUUID
     * @param privType
     * @param forGrant
     * @param authorizationId
     * @return
     * @throws StandardException
     */
    ColPermsDescriptor getColumnPermissions(DataDictionaryImpl dd, UUID tableUUID,
                                                   int privType,
                                                   boolean forGrant,
                                                   String authorizationId) throws StandardException;
}
