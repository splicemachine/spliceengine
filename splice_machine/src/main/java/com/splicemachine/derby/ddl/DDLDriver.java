package com.splicemachine.derby.ddl;

import com.splicemachine.access.api.SConfiguration;

/**
 * @author Scott Fines
 *         Date: 12/31/15
 */
public class DDLDriver{
    private static DDLDriver INSTANCE;

    private final DDLController ddlController;
    private final DDLWatcher ddlWatcher;
    private final SConfiguration ddlConfiguration;

    public static DDLDriver driver(){
        return INSTANCE;
    }

    public DDLDriver(DDLEnvironment ddlEnv){
        this.ddlController  =ddlEnv.getController();
        this.ddlWatcher = ddlEnv.getWatcher();
        this.ddlConfiguration = ddlEnv.getConfiguration();
        ddlConfiguration.addDefaults(DDLConfiguration.defaults);
    }

    public DDLController ddlController(){ return ddlController;}

    public DDLWatcher ddlWatcher() { return ddlWatcher;}

    public SConfiguration ddlConfiguration(){ return ddlConfiguration;}

    public static void loadDriver(DDLEnvironment env){
        INSTANCE = new DDLDriver(env);
    }
}
