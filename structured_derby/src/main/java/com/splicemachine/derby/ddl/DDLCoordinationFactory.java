package com.splicemachine.derby.ddl;

public class DDLCoordinationFactory {
    private static DDLController controller = new ZookeeperDDLController();
    private static DDLWatcher watcher = new ZookeeperDDLWatcher();

    public static DDLController getController() {
        return controller;
    }

    public static DDLWatcher getWatcher() {
        return watcher;
    }
}
