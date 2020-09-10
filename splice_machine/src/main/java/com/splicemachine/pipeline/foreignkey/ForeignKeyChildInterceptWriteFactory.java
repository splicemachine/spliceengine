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

import splice.com.google.common.primitives.Longs;
import com.splicemachine.ddl.DDLMessage.*;
import com.splicemachine.pipeline.api.PipelineExceptionFactory;
import com.splicemachine.pipeline.context.PipelineWriteContext;
import com.splicemachine.pipeline.contextfactory.LocalWriteFactory;
import java.io.IOException;

/**
 * LocalWriteFactory for ForeignKeyInterceptWriteHandler -- see that class for details.
 */
class ForeignKeyChildInterceptWriteFactory implements LocalWriteFactory{

    /* The base-table or unique-index conglomerate that this FK references. */
    private final long referencedConglomerateNumber;
    private final FKConstraintInfo fkConstraintInfo;
    private final PipelineExceptionFactory exceptionFactory;

    ForeignKeyChildInterceptWriteFactory(long referencedConglomerateNumber, FKConstraintInfo fkConstraintInfo,
                                         PipelineExceptionFactory exceptionFactory) {
        this.referencedConglomerateNumber = referencedConglomerateNumber;
        this.fkConstraintInfo = fkConstraintInfo;
        this.exceptionFactory = exceptionFactory;
    }

    @Override
    public void addTo(PipelineWriteContext ctx, boolean keepState, int expectedWrites) throws IOException {
        ctx.addLast(new ForeignKeyChildInterceptWriteHandler(referencedConglomerateNumber, fkConstraintInfo,exceptionFactory));
    }

    @Override
    public long getConglomerateId() {
        throw new UnsupportedOperationException("not used");
    }



    @Override
    public int hashCode() {
        return Longs.hashCode(this.referencedConglomerateNumber);
    }

    // Equality is based on the referenced conglomerate.  Within the FK backing index where the WriteHandlers from
    // this factory will be installed we only need one WriteHandler for each referenced conglomerate number.
    @Override
    public boolean equals(Object o) {
        return o == this || (o instanceof ForeignKeyChildInterceptWriteFactory) &&
                ((ForeignKeyChildInterceptWriteFactory)o).referencedConglomerateNumber == this.referencedConglomerateNumber;
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
