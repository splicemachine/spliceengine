/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.derby.impl.sql;

import java.sql.SQLException;
import java.util.Properties;
import com.splicemachine.backup.BackupManager;
import com.splicemachine.colperms.ColPermsManager;
import com.splicemachine.db.authentication.UserAuthenticator;
import com.splicemachine.db.catalog.UUID;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.dictionary.ColPermsDescriptor;
import com.splicemachine.db.impl.jdbc.authentication.AuthenticationServiceBase;
import com.splicemachine.db.impl.sql.catalog.DataDictionaryImpl;
import com.splicemachine.encryption.EncryptionManager;
import com.splicemachine.management.Manager;

/**
 * Fake manager impl for mem db
 */
public class NoOpManager implements Manager {
    private static NoOpManager ourInstance=new NoOpManager();

    public static NoOpManager getInstance(){
        return ourInstance;
    }

    private NoOpManager(){ }

    @Override
    public void enableEnterprise(char[] value) throws StandardException {
        // no-op
    }

    @Override
    public BackupManager getBackupManager() throws StandardException {
        return NoOpBackupManager.getInstance();
    }

    @Override
    public UserAuthenticator getAuthenticationManager(AuthenticationServiceBase svc, Properties properties) throws StandardException {
        return null;
    }

    @Override
    public ColPermsManager getColPermsManager() throws StandardException {
        return new ColPermsManager() {
            @Override
            public ColPermsDescriptor getColumnPermissions(DataDictionaryImpl dd, UUID colPermsUUID) throws StandardException {
                return null;
            }

            @Override
            public ColPermsDescriptor getColumnPermissions(DataDictionaryImpl dd, UUID tableUUID, int privType, boolean forGrant, String authorizationId) throws StandardException {
                return null;
            }
        };
    }

    @Override
    public EncryptionManager getEncryptionManager() throws StandardException {
        return new EncryptionManager() {};
    }

    @Override
    public boolean isEnabled() {
        return false;
    }
}
