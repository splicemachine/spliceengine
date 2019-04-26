/*
 * Copyright (c) 2012 - 2019 Splice Machine, Inc.
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

package com.splicemachine.management;

import java.util.Properties;

import com.splicemachine.backup.BackupManager;
import com.splicemachine.colperms.ColPermsManager;
import com.splicemachine.db.authentication.UserAuthenticator;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.impl.jdbc.authentication.AuthenticationServiceBase;
import com.splicemachine.encryption.EncryptionManager;
import com.splicemachine.replication.ReplicationManager;

/**
 * Manager
 */
public interface Manager {

    void enableEnterprise(final char[] value) throws StandardException;

    BackupManager getBackupManager() throws StandardException;

    UserAuthenticator getAuthenticationManager(AuthenticationServiceBase svc, Properties properties) throws StandardException;

    ColPermsManager getColPermsManager() throws StandardException;

    EncryptionManager getEncryptionManager() throws StandardException;

    ReplicationManager getReplicationManager() throws StandardException;

    boolean isEnabled();
}
