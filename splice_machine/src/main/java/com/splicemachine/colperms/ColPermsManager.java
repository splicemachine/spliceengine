package com.splicemachine.colperms;

import com.splicemachine.db.catalog.UUID;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.dictionary.ColPermsDescriptor;
import com.splicemachine.db.impl.sql.catalog.DataDictionaryImpl;

/**
 * TODO: JC - 7/14/16
 */
public interface ColPermsManager {

    ColPermsDescriptor getColumnPermissions(DataDictionaryImpl dd, UUID colPermsUUID) throws StandardException;

    ColPermsDescriptor getColumnPermissions(DataDictionaryImpl dd, UUID tableUUID,
                                                   int privType,
                                                   boolean forGrant,
                                                   String authorizationId) throws StandardException;
}
