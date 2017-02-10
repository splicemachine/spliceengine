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
 */

package com.splicemachine.derby.stream.function;

import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.sql.execute.ExecutionFactory;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.impl.sql.execute.operations.ProjectRestrictOperation;
import com.splicemachine.derby.stream.iapi.OperationContext;
import org.apache.commons.collections.iterators.SingletonIterator;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Iterator;

/**
 * Created by jleach on 5/1/15.
 */
public class ProjectRestrictFlatMapFunction<Op extends SpliceOperation> extends SpliceFlatMapFunction<Op,LocatedRow,LocatedRow> {
    protected boolean initialized;
    protected ProjectRestrictOperation op;
    protected ExecutionFactory executionFactory;

    public ProjectRestrictFlatMapFunction() {
        super();
    }

    public ProjectRestrictFlatMapFunction(OperationContext<Op> operationContext) {
        super(operationContext);
    }

    @Override
    public Iterator<LocatedRow> call(LocatedRow from) throws Exception {
        if (!initialized) {
            initialized = true;
            op = (ProjectRestrictOperation) getOperation();
            executionFactory = op.getExecutionFactory();
        }
        op.setCurrentRow(from.getRow());
        op.source.setCurrentRow(from.getRow());
        if (!op.getRestriction().apply(from.getRow())) {
            operationContext.recordFilter();
            return Collections.<LocatedRow>emptyList().iterator();
        }
        ExecRow preCopy = op.doProjection(from.getRow());
        LocatedRow locatedRow = new LocatedRow(from.getRowLocation(), preCopy);
        op.setCurrentLocatedRow(locatedRow);
        return new SingletonIterator(locatedRow);
    }
}
