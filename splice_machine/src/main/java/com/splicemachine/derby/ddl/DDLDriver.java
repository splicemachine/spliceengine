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

package com.splicemachine.derby.ddl;

import com.splicemachine.access.api.SConfiguration;

/**
 * @author Scott Fines
 *         Date: 12/31/15
 */
public class DDLDriver{
    private static volatile DDLDriver INSTANCE;

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
    }

    public DDLController ddlController(){ return ddlController;}

    public DDLWatcher ddlWatcher() { return ddlWatcher;}

    public SConfiguration ddlConfiguration(){ return ddlConfiguration;}

    public static void loadDriver(DDLEnvironment env){
        INSTANCE = new DDLDriver(env);
    }
}
