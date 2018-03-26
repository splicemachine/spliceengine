/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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
 *
 */

package com.splicemachine.derby.iapi.sql.execute;

import com.splicemachine.db.iapi.services.monitor.Monitor;
import com.splicemachine.db.iapi.services.stream.HeaderPrintWriter;
import com.splicemachine.db.iapi.sql.ParameterValueSet;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.execute.ExecPreparedStatement;
import com.splicemachine.derby.stream.iapi.DataSetProcessor;

import javax.annotation.Nonnull;

/**
 * Created by dgomezferro on 24/08/2017.
 */
public class FileStatementLogger implements StatementLogger {
    @Override
    public void logExecutionStart(@Nonnull String xactId, int sessionId, String dbName, String drdaId, String uuid, DataSetProcessor.Type processorType, ExecPreparedStatement ps, ParameterValueSet pvs) {
        HeaderPrintWriter istream = Monitor.getStream();
        String pvsString = "";
        if (pvs != null && pvs.getParameterCount() > 0) {
            pvsString = " with " + pvs.getParameterCount() +
                    " parameters " + pvs.toString();
        }
        istream.printStatement(LanguageConnectionContext.xidStr +
                xactId +
                "), " +
                LanguageConnectionContext.lccStr +
                sessionId +
                "), " +
                LanguageConnectionContext.dbnameStr +
                dbName +
                "), " +
                LanguageConnectionContext.drdaStr +
                drdaId +
                "), " +
                LanguageConnectionContext.uuidStr +
                uuid +
                "), " +
                LanguageConnectionContext.execStr +
                processorType +
                "), Executing prepared statement: " +
                ps.getSource() +
                " :End prepared statement" +
                pvsString);
    }

    @Override
    public void logExecutionEnd(@Nonnull String xactId, int sessionId, String dbName, String drdaId, String uuid) {
        HeaderPrintWriter istream = Monitor.getStream();
        istream.printStatement(LanguageConnectionContext.xidStr +
                xactId +
                "), " +
                LanguageConnectionContext.lccStr +
                sessionId +
                "), " +
                LanguageConnectionContext.dbnameStr +
                dbName +
                "), " +
                LanguageConnectionContext.drdaStr +
                drdaId +
                "), " +
                LanguageConnectionContext.uuidStr +
                uuid +
                "), Finished executing prepared statement");
    }
}
