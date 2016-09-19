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

package com.splicemachine.pipeline;

import java.io.IOException;

import com.splicemachine.SqlExceptionFactory;
import com.splicemachine.ddl.DDLMessage;
import com.splicemachine.derby.ddl.*;
import com.splicemachine.pipeline.api.PipelineExceptionFactory;
import com.splicemachine.pipeline.context.PipelineWriteContext;
import com.splicemachine.pipeline.contextfactory.LocalWriteFactory;
import com.splicemachine.pipeline.writehandler.SnapshotIsolatedWriteHandler;
import com.splicemachine.pipeline.writehandler.WriteHandler;
import com.splicemachine.si.api.data.ExceptionFactory;
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
