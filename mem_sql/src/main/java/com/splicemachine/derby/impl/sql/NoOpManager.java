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
import com.splicemachine.db.impl.jdbc.authentication.AuthenticationServiceBase;
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
    public void enableEnterprise(char[] value) throws SQLException {
        // no-op
    }

    @Override
    public BackupManager getBackupManager() throws SQLException {
        return NoOpBackupManager.getInstance();
    }

    @Override
    public UserAuthenticator getAuthenticationManager(AuthenticationServiceBase svc, Properties properties) throws SQLException {
        return null;
    }

    @Override
    public ColPermsManager getColPermsManager() throws SQLException {
        return null;
    }
}
