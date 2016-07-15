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

package com.splicemachine.derby.lifecycle;

import java.util.Iterator;
import java.util.Properties;
import java.util.ServiceLoader;

import org.apache.log4j.Logger;

import com.splicemachine.access.configuration.HBaseConfiguration;
import com.splicemachine.backup.BackupManager;
import com.splicemachine.colperms.ColPermsManager;
import com.splicemachine.db.authentication.UserAuthenticator;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.impl.jdbc.authentication.AuthenticationServiceBase;
import com.splicemachine.db.shared.common.reference.SQLState;
import com.splicemachine.encryption.EncryptionManager;
import com.splicemachine.management.Manager;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.utils.SpliceLogUtils;

/**
 * Uses ServiceLoader to load the appropriate {@link Manager} implementation.
 */
public class ManagerLoader {
    private static final Logger LOG=Logger.getLogger(ManagerLoader.class);
    private static volatile Manager manager;

    public static Manager load() {
        Manager manager = ManagerLoader.manager;
        if (manager == null) {
            manager = loadManager(null);
        }
        return manager;
    }

    public static void clear() {
        manager = null;
    }

    private static synchronized Manager loadManager(ClassLoader loader) {
        Manager mgr = manager;
        if (mgr == null) {

            ServiceLoader<Manager> load;
            if (loader == null) {
                load = ServiceLoader.load(Manager.class);
            } else {
                load = ServiceLoader.load(Manager.class, loader);
            }
            Iterator<Manager> iter = load.iterator();
            if(! iter.hasNext()) {
                // enterprise either disabled or error loading mgr
                // log and return disabled mgr
                LOG.warn("No Manager implementation found.");
                mgr = new DisabledManager();
            } else {
                mgr = manager = iter.next();
            }
            if(iter.hasNext())
                throw new IllegalStateException("Only one Manager is allowed!");
        }
        return mgr;
    }

    public static class DisabledManager implements Manager {

        @Override
        public void enableEnterprise(char[] value) throws StandardException {
            // this will only be seen in open source version
            throw StandardException.newException(SQLState.MANAGER_DISABLED);
        }

        @Override
        public BackupManager getBackupManager() throws StandardException {
            throw StandardException.newException(SQLState.MANAGER_DISABLED);
        }

        @Override
        public UserAuthenticator getAuthenticationManager(AuthenticationServiceBase svc, Properties properties) throws
            StandardException {
            throw StandardException.newException(SQLState.MANAGER_DISABLED);
        }

        @Override
        public ColPermsManager getColPermsManager() throws StandardException {
            throw StandardException.newException(SQLState.MANAGER_DISABLED);
        }

        @Override
        public EncryptionManager getEncryptionManager() throws StandardException {
            checkEncryptionLevels();
            return null;
        }

        @Override
        public boolean isEnabled() {
            return false;
        }

    }

    public static void checkEncryptionLevels() {
        if (encryptedMessaging() || encryptedFileSystem()) {
            SpliceLogUtils.error(LOG,"Splice Machine Enterprise Required for encrypted Hadoop Clusters (Kerberos, etc.)");
            System.exit(1);
        }
    }

    public static boolean encryptedMessaging() {
        return SIDriver.driver().getConfiguration().getHbaseSecurityAuthentication();
    }

    public static boolean encryptedFileSystem() {
        return SIDriver.driver().getConfiguration().getHbaseSecurityAuthorization() != null &&
                ! SIDriver.driver().getConfiguration().getHbaseSecurityAuthorization().equals(HBaseConfiguration.DEFAULT_HBASE_SECURITY_AUTHORIZATION);
    }


}
