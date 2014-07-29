package com.splicemachine.derby.ddl;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.derby.catalog.UUID;

public class DDLCoordinationFactory {

    public static final Gson GSON = new GsonBuilder().
            registerTypeAdapter(TentativeDDLDesc.class, new InterfaceSerializer<TentativeDDLDesc>()).
            registerTypeAdapter(UUID.class, new InterfaceSerializer<UUID>()).
            create();

    private static final DDLController DDL_CONTROLLER = new ZookeeperDDLController();
    private static final DDLWatcher DDL_WATCHER = new ZookeeperDDLWatcher();

    public static DDLController getController() {
        return DDL_CONTROLLER;
    }

    public static DDLWatcher getWatcher() {
        return DDL_WATCHER;
    }
}
