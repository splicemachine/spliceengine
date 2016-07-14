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

import java.sql.SQLException;
import java.util.Iterator;
import java.util.Properties;
import java.util.ServiceLoader;

import com.splicemachine.encryption.EncryptionManager;
import org.apache.log4j.Logger;
import com.splicemachine.backup.BackupManager;
import com.splicemachine.colperms.ColPermsManager;
import com.splicemachine.db.authentication.UserAuthenticator;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.impl.jdbc.authentication.AuthenticationServiceBase;
import com.splicemachine.db.shared.common.reference.SQLState;
import com.splicemachine.management.Manager;

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
            // TODO: remove
            printClasspath();

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
    private static void printClasspath() {
        String cp = System.getProperty("java.class.path");
        if (cp == null || cp.isEmpty()) {
            LOG.debug("*** Empty classpath");
        } else {
            System.out.println("*** classpath:");
            for (String item : cp.split(":")) {
                LOG.debug("  "+item);
            }
        }
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
            throw StandardException.newException(SQLState.MANAGER_DISABLED);
        }

        @Override
        public boolean isEnabled() {
            return false;
        }
    }
}
