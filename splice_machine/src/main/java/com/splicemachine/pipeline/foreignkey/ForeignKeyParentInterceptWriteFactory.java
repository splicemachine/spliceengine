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

package com.splicemachine.pipeline.foreignkey;

import com.splicemachine.db.iapi.sql.StatementType;
import com.splicemachine.ddl.DDLMessage;
import com.splicemachine.pipeline.api.PipelineExceptionFactory;
import com.splicemachine.pipeline.context.PipelineWriteContext;
import com.splicemachine.pipeline.contextfactory.LocalWriteFactory;
import splice.com.google.common.collect.Lists;

import java.util.List;

/**
 * LocalWriteFactory for ForeignKeyParentInterceptWriteHandler -- see that class for details.
 */
class ForeignKeyParentInterceptWriteFactory implements LocalWriteFactory{

    private final String parentTableName;
    private final List<Long> referencingIndexConglomerateNumbers = Lists.newArrayList();
    private final PipelineExceptionFactory exceptionFactory;
    private final List<DDLMessage.FKConstraintInfo> constraintInfos = Lists.newArrayList();
    private boolean hasCascadingDeleteFK = false;
    private boolean constraintsChanged = true;

    ForeignKeyParentInterceptWriteFactory(String parentTableName,
                                          List<Long> referencingIndexConglomerateNumbers,
                                          PipelineExceptionFactory exceptionFactory,
                                          List<DDLMessage.FKConstraintInfo> fkConstraintInfo) {
        this.parentTableName = parentTableName;
        this.exceptionFactory=exceptionFactory;
        this.referencingIndexConglomerateNumbers.addAll(referencingIndexConglomerateNumbers);
        this.constraintInfos.addAll(fkConstraintInfo);
    }

    @Override
    public void addTo(PipelineWriteContext ctx, boolean keepState, int expectedWrites) {
        ctx.addLast(new ForeignKeyParentInterceptWriteHandler(parentTableName, referencingIndexConglomerateNumbers, constraintInfos, exceptionFactory));
    }

    @Override
    public long getConglomerateId() {
        throw new UnsupportedOperationException("not used");
    }

    /**
     * If a FK is dropped or the child table is dropped remove it from the list of conglomerates we check.
     */
    public void removeReferencingIndexConglomerateNumber(long conglomerateNumber) {
        int idx = this.referencingIndexConglomerateNumbers.indexOf(conglomerateNumber);
        if(idx != -1) {
            this.referencingIndexConglomerateNumbers.remove(conglomerateNumber);
            this.constraintInfos.remove(idx); // at index
            constraintsChanged = true;
        }
    }

    public void addReferencingIndexConglomerateNumber(List<Long> referencingIndexConglomerateNumbers,
                                                      List<DDLMessage.FKConstraintInfo> fkConstraintInfos) {
        this.referencingIndexConglomerateNumbers.addAll(referencingIndexConglomerateNumbers);
        this.constraintInfos.addAll(fkConstraintInfos);
        constraintsChanged = true;
    }

    public boolean hasCascadingDeleteFK() {
        if(!constraintsChanged) {
            return hasCascadingDeleteFK;
        }
        hasCascadingDeleteFK = false;
        for(DDLMessage.FKConstraintInfo constraintInfo : constraintInfos) {
           if(constraintInfo.getDeleteRule() == StatementType.RA_CASCADE) {
               hasCascadingDeleteFK = true;
               break;
           }
        }
        constraintsChanged = false;
        return hasCascadingDeleteFK;
    }

    @Override
    public boolean canReplace(LocalWriteFactory newContext){
        return false;
    }

    @Override
    public void replace(LocalWriteFactory newFactory){
        throw new UnsupportedOperationException();
    }
}
