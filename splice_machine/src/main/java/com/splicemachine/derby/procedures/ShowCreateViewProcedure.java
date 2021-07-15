/*
 * Copyright (c) 2012 - 2021 Splice Machine, Inc.
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

package com.splicemachine.derby.procedures;

import com.splicemachine.EngineDriver;
import com.splicemachine.db.iapi.error.PublicAPI;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.dictionary.TableDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.ViewDescriptor;
import com.splicemachine.db.impl.jdbc.EmbedConnection;
import com.splicemachine.db.impl.sql.catalog.Procedure;
import com.splicemachine.db.shared.common.sanity.SanityManager;
import com.splicemachine.derby.utils.EngineUtils;
import com.splicemachine.pipeline.ErrorState;
import com.splicemachine.pipeline.Exceptions;
import com.splicemachine.procedures.ProcedureUtils;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import splice.com.google.common.collect.Lists;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

public class ShowCreateViewProcedure extends BaseAdminProcedures {

    public static Procedure getProcedure() {
        return Procedure.newBuilder().name("SHOW_CREATE_VIEW")
                .numOutputParams(0)
                .numResultSets(1)
                .varchar("schemaName", 128)
                .varchar("viewName", 128)
                .ownerClass(ShowCreateViewProcedure.class.getCanonicalName())
                .build().debugCheck();
    }

    @SuppressFBWarnings(value="SQL_NONCONSTANT_STRING_PASSED_TO_EXECUTE", justification="Intentional")
    public static void SHOW_CREATE_VIEW(String schemaName, String tableName, ResultSet[] resultSet)
            throws SQLException, StandardException
    {
        String ddl = SHOW_CREATE_VIEW_CORE(schemaName, tableName);
        resultSet[0] = ProcedureUtils.generateResult("DDL", ddl + ";");
    }

    public static String SHOW_CREATE_VIEW_CORE(String schemaName, String viewName) throws SQLException {
        Connection connection = getDefaultConn();

        schemaName = EngineUtils.validateSchema(schemaName);
        viewName = EngineUtils.validateTable(viewName);
        try {
            TableDescriptor td = EngineUtils.verifyTableExists(connection, schemaName, viewName);

            if (td.getTableType() != TableDescriptor.VIEW_TYPE) {
                throw ErrorState.LANG_INVALID_OPERATION_NOT_A_VIEW.newException("SHOW CREATE VIEW", "\"" +
                        schemaName + "\".\"" + viewName + "\"");
            }
            if(SanityManager.DEBUG) {
                SanityManager.ASSERT(connection instanceof EmbedConnection);
                SanityManager.ASSERT(((EmbedConnection)connection).getLanguageConnection() != null);
            }

            ViewDescriptor vd = ((EmbedConnection)connection).getLanguageConnection().getDataDictionary().getViewDescriptor(td);
            return vd.getViewText();
        } catch (StandardException e) {
            throw PublicAPI.wrapStandardException(Exceptions.parseException(e));
        }
    }
}
