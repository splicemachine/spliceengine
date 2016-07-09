package com.splicemachine.derby.lifecycle;

import java.util.Iterator;
import java.util.ServiceLoader;

import org.apache.log4j.Logger;

import com.splicemachine.backup.BackupManager;
import com.splicemachine.db.iapi.error.StandardException;
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
            manager = loadBackup(null);
        }
        return manager;
    }

    private static synchronized Manager loadBackup(ClassLoader loader) {
        Manager mgr = manager;
        if (mgr == null) {
            if (LOG.isDebugEnabled()) {
                printClasspath();
            }
            ServiceLoader<Manager> load;
            if (loader == null) {
                load = ServiceLoader.load(Manager.class);
            } else {
                load = ServiceLoader.load(Manager.class, loader);
            }
            Iterator<Manager> iter = load.iterator();
            if(! iter.hasNext()) {
                // backup either disabled or we had a problem loading real backup mgr
                // log and return disabled backup mgr
                LOG.warn("No backup manager implementation found.");
                throw new IllegalStateException("No backup manager implementation found.");
            } else {
                mgr = manager = iter.next();
            }
            if(iter.hasNext())
                throw new IllegalStateException("Only one Manager is allowed!");
        }
        return mgr;
    }

    // TODO: JC -remove
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
}
