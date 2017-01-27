/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
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
