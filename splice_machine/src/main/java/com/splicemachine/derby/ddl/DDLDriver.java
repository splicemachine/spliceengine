/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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
