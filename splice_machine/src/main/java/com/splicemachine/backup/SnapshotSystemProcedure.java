/*
 * Copyright (c) 2012 - 2021 Splice Machine, Inc.
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
package com.splicemachine.backup;

import com.splicemachine.EngineDriver;
import com.splicemachine.db.catalog.types.RoutineAliasInfo;
import com.splicemachine.db.impl.sql.catalog.Procedure;
import com.splicemachine.derby.utils.EngineUtils;
import com.splicemachine.procedures.ProcedureUtils;
import com.splicemachine.snapshot.SnapshotManager;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.log4j.Logger;
import java.sql.ResultSet;
import java.util.List;

public class SnapshotSystemProcedure {

    private static Logger LOG = Logger.getLogger(SnapshotSystemProcedure.class);

    public static void addProcedures(List<Procedure> procedures) {
        Procedure snapshotSchema = Procedure.newBuilder().name("CREATE_SCHEMA_SNAPSHOT")
                .varchar("schemaName", 128)
                .sqlControl(RoutineAliasInfo.MODIFIES_SQL_DATA)
                .numOutputParams(0)
                .numResultSets(1)
                .ownerClass(SnapshotSystemProcedure.class.getCanonicalName())
                .build();
        procedures.add(snapshotSchema);

        Procedure cloneSnapshot = Procedure.newBuilder().name("CLONE_SCHEMA_SNAPSHOT")
                .bigint("snapshotId")
                .catalog("schemaName")
                .sqlControl(RoutineAliasInfo.MODIFIES_SQL_DATA)
                .numOutputParams(0)
                .numResultSets(1)
                .ownerClass(SnapshotSystemProcedure.class.getCanonicalName())
                .build();
        procedures.add(cloneSnapshot);

        Procedure deleteSnapshot = Procedure.newBuilder().name("DELETE_SNAPSHOT")
                .bigint("snapshoId")
                .numOutputParams(0)
                .numResultSets(1)
                .ownerClass(SnapshotSystemProcedure.class.getCanonicalName())
                .build();
        procedures.add(deleteSnapshot);
    }
    public static void CLONE_SCHEMA_SNAPSHOT(long snapshotId, String schemaName, ResultSet[] resultSets) throws Exception {
        try {
            SnapshotManager snapshotManager = EngineDriver.driver().manager().getSnapshotManager();
            snapshotManager.cloneSchemaSnapshot(snapshotId, schemaName);
        }catch (Throwable t) {
            resultSets[0] = ProcedureUtils.generateResult("Error", t.getLocalizedMessage());
            SpliceLogUtils.error(LOG, "Database backup error", t);
            t.printStackTrace();
        }
    }

    /**
     * Take a snapshot of a schema
     * @param schemaName
     * @throws Exception
     */
    public static void CREATE_SCHEMA_SNAPSHOT(String schemaName, ResultSet[] resultSets) throws Exception {
        try {
            schemaName = EngineUtils.validateSchema(schemaName);
            EngineUtils.checkSchemaVisibility(schemaName);

            SnapshotManager snapshotManager = EngineDriver.driver().manager().getSnapshotManager();
            snapshotManager.createSchemaSnapshot(schemaName);
        }catch (Throwable t) {
            resultSets[0] = ProcedureUtils.generateResult("Error", t.getLocalizedMessage());
            SpliceLogUtils.error(LOG, "Database backup error", t);
            t.printStackTrace();
        }
    }


    /**
     * delete a snapshot
     * @param snapshotId
     * @throws Exception
     */
    public static void DELETE_SNAPSHOT(long snapshotId, ResultSet[] resultSets) throws Exception
    {
        try {

            SnapshotManager snapshotManager = EngineDriver.driver().manager().getSnapshotManager();
            snapshotManager.deleteSnapshot(snapshotId);
        }catch (Throwable t) {
            resultSets[0] = ProcedureUtils.generateResult("Error", t.getLocalizedMessage());
            SpliceLogUtils.error(LOG, "Delete snapshot error", t);
            t.printStackTrace();
        }
    }
}
