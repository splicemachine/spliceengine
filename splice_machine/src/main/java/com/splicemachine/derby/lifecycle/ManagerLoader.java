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
