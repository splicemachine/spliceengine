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

package com.splicemachine.derby.impl.sql.catalog.upgrade;

import com.splicemachine.EngineDriver;
import com.splicemachine.backup.BackupSystemProcedures;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.procedures.ProcedureUtils;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.log4j.Logger;

import java.sql.ResultSet;
import java.sql.SQLException;

public class OnlineUpgradeSystemProcedures {

    private static Logger LOG = Logger.getLogger(OnlineUpgradeSystemProcedures.class);

    public static void ROLLING_RESTART(String hostnameAndPort,
                                       String cluster,
                                       String user,
                                       String password,
                                       ResultSet[] resultSets) throws StandardException, SQLException {
        try {
            OnlineUpgradeManager manager = EngineDriver.driver().manager().getOnlineUpgradeManager();
            manager.rollingRestart(hostnameAndPort, cluster, user,password);
        }
        catch (Throwable t) {
            resultSets[0] = ProcedureUtils.generateResult("Error", t.getLocalizedMessage());
            SpliceLogUtils.error(LOG, "Rolling restart error", t);
        }
    }
}
