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
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.procedures.ProcedureUtils;

import java.sql.*;
import java.util.List;

public class UpgradeSystemProcedures {

    public static void BEGIN_ROLLING_UPGRADE(final ResultSet[] resultSets) throws SQLException, StandardException {
        try {
            UpgradeManager upgradeManager = EngineDriver.driver().manager().getUpgradeManager();
            upgradeManager.startRollingUpgrade();
            resultSets[0] = ProcedureUtils.generateResult("Success", "The system is rolling upgrade.");
        }
        catch (Exception e) {
            resultSets[0] = ProcedureUtils.generateResult("Error", e.getLocalizedMessage());
        }
    }

    public static void END_ROLLING_UPGRADE(final ResultSet[] resultSets) throws SQLException, StandardException {
        try {
            UpgradeManager upgradeManager = EngineDriver.driver().manager().getUpgradeManager();
            upgradeManager.endRollingUpgrade();
            resultSets[0] = ProcedureUtils.generateResult("Success", "The system completed rolling upgrade.");
        }
        catch (Exception e) {
            resultSets[0] = ProcedureUtils.generateResult("Error", e.getLocalizedMessage());
        }
    }

    public static void UNLOAD_REGIONS(String hostAndPort, final ResultSet[] resultSets) throws SQLException, StandardException {
        try {
            UpgradeManager upgradeManager = EngineDriver.driver().manager().getUpgradeManager();
            List<String> regions = upgradeManager.unloadRegions(hostAndPort);

            resultSets[0] = ProcedureUtils.generateResult("Success",
                    String.format("Unloaded %d regions from server %s", regions.size(), hostAndPort));
        }
        catch (Exception e) {
            resultSets[0] = ProcedureUtils.generateResult("Error", e.getLocalizedMessage());
        }
    }

    public static void LOAD_REGIONS(String serverName, final ResultSet[] resultSets) throws SQLException, StandardException {
        try {
            UpgradeManager upgradeManager = EngineDriver.driver().manager().getUpgradeManager();
            List<String> regions = upgradeManager.loadRegions(serverName);
            resultSets[0] = ProcedureUtils.generateResult("Success",
                    String.format("Loaded %d regions to server %s", regions.size(), serverName));
        }
        catch (Exception e) {
            resultSets[0] = ProcedureUtils.generateResult("Error", e.getLocalizedMessage());
        }
    }
}
