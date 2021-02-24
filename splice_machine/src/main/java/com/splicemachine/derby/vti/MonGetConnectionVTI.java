/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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

package com.splicemachine.derby.vti;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.ResultColumnDescriptor;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.impl.jdbc.EmbedResultSetMetaData;
import com.splicemachine.db.impl.sql.catalog.SYSMONGETCONNECTIONViewInfoProvider;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.db.vti.CompileTimeSchema;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.stream.control.MaterializedControlDataSet;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.DataSetProcessor;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.vti.iapi.DatasetProvider;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class MonGetConnectionVTI implements DatasetProvider, CompileTimeSchema {
    private OperationContext<SpliceOperation> operationContext;

    private final Long applicationHandle; // null -> all connections.
    private final Integer member; // -1 (or null) current database, -2 all active databases.
    private final Integer systemApplications; // 0 (or null) system application info is not returned, 1 system application info is returned.

    public MonGetConnectionVTI() {
        this(null, null, null);
    }

    public MonGetConnectionVTI(Long applicationHandle) {
        this(applicationHandle, null, null);
    }

    public MonGetConnectionVTI(Long applicationHandle, Integer member) {
        this(applicationHandle, member, null);
    }

    @SuppressFBWarnings(value = "BX_UNBOXING_IMMEDIATELY_REBOXED", justification = "intentional, as per DB2 API spec")
    public MonGetConnectionVTI(Long applicationHandle, Integer member, Integer systemApplications) {
        this.applicationHandle = applicationHandle;
        this.member = member == null ? -1 : member;
        this.systemApplications = systemApplications;
    }

    @Override
    public DataSet<ExecRow> getDataSet(SpliceOperation op, DataSetProcessor dsp, ExecRow execRow) throws StandardException {
        operationContext = dsp.createOperationContext(op);
        List<ExecRow> rows = new ArrayList<>();
        ExecRow valueRow = new ValueRow(SYSMONGETCONNECTIONViewInfoProvider.COLUMN_COUNT);
        SYSMONGETCONNECTIONViewInfoProvider.makeCompileTimeRow(valueRow);
        // TODO: add logic to pull connections and add row for each connection similar to below.
        // TODO: opening connections to other region servers from here should be done securely.
        return new MaterializedControlDataSet<>(rows );
    }

    public static ResultSetMetaData getMetaData() throws SQLException {
        return metadata;
    }

    @Override
    public OperationContext getOperationContext() {
        return operationContext;
    }

    /*
     * Metadata
     */
    private static final ResultColumnDescriptor[] columnInfo = Arrays
            .stream(SYSMONGETCONNECTIONViewInfoProvider.buildCompileTimeColumnList())
            .map(c -> EmbedResultSetMetaData.getResultColumnDescriptor(c.getName(), c.getType()))
            .toArray(ResultColumnDescriptor[]::new);

    private static final ResultSetMetaData metadata = new EmbedResultSetMetaData(columnInfo);
}
