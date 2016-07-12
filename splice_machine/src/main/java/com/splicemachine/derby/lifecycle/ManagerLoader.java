package com.splicemachine.derby.lifecycle;

import java.sql.SQLException;
import java.util.Iterator;
import java.util.ServiceLoader;

import org.apache.log4j.Logger;

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
            manager = loadManager(null);
        }
        return manager;
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
        public void enableEnterprise(char[] value) throws SQLException {
            // this will only be seen in open source version
            throw new SQLException(StandardException.newException(SQLState.MANAGER_DISABLED));
        }
    }
}
