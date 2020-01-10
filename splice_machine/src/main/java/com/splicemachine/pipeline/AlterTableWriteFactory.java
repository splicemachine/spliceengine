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

package com.splicemachine.pipeline;

import java.io.IOException;

import com.splicemachine.ddl.DDLMessage;
import com.splicemachine.derby.ddl.*;
import com.splicemachine.pipeline.api.PipelineExceptionFactory;
import com.splicemachine.pipeline.context.PipelineWriteContext;
import com.splicemachine.pipeline.contextfactory.LocalWriteFactory;
import com.splicemachine.pipeline.writehandler.SnapshotIsolatedWriteHandler;
import com.splicemachine.pipeline.writehandler.WriteHandler;
import com.splicemachine.si.api.filter.TransactionReadController;
import com.splicemachine.si.impl.DDLFilter;

/**
 * The write intercepting side of alter table.<br/>
 * Intercepts writes to old conglom and forwards them to new.
 */
public class AlterTableWriteFactory implements LocalWriteFactory{
    private final DDLMessage.DDLChange ddlChange;
    private final AlterTableDDLDescriptor ddlDescriptor;
    private final TransactionReadController readController;

    private AlterTableWriteFactory(DDLMessage.DDLChange ddlChange,
                                   AlterTableDDLDescriptor ddlDescriptor,
                                   TransactionReadController readController) {
        this.ddlChange = ddlChange;
        this.ddlDescriptor = ddlDescriptor;
        this.readController = readController;
    }

    public static AlterTableWriteFactory create(DDLMessage.DDLChange ddlChange,
                                                TransactionReadController readController,
                                                PipelineExceptionFactory exceptionFactory) {
        DDLMessage.DDLChangeType changeType = ddlChange.getDdlChangeType();
        // TODO: JC - DB-4004 - ADD_PRIMARY_KEY
        if (changeType == DDLMessage.DDLChangeType.DROP_PRIMARY_KEY) {
            return new AlterTableWriteFactory(ddlChange,
                                              new TentativeDropPKConstraintDesc(ddlChange.getTentativeDropPKConstraint(),
                                                                                exceptionFactory),
                                              readController);
        } else {
            throw new RuntimeException("Unknown DDLChangeType: "+changeType);
        }
    }

    @Override
    public void addTo(PipelineWriteContext ctx, boolean keepState, int expectedWrites) throws IOException {
        RowTransformer transformer = ddlDescriptor.createRowTransformer();
        WriteHandler writeHandler = ddlDescriptor.createWriteHandler(transformer);
        DDLFilter ddlFilter = readController.newDDLFilter(DDLUtils.getLazyTransaction(ddlChange.getTxnId()));
        ctx.addLast(new SnapshotIsolatedWriteHandler(writeHandler, ddlFilter));
    }

    @Override
    public boolean canReplace(LocalWriteFactory newContext){
        return false;
    }

    @Override
    public void replace(LocalWriteFactory newFactory){
        throw new UnsupportedOperationException();
    }

    @Override
    public long getConglomerateId() {
        return ddlDescriptor.getConglomerateNumber();
    }
}
