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

package com.splicemachine.derby.stream.function;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.impl.sql.execute.operations.NormalizeOperation;
import com.splicemachine.derby.stream.iapi.OperationContext;
import org.apache.commons.collections.iterators.SingletonIterator;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Iterator;

/**
 * Created by jleach on 11/4/15.
 */
public class NormalizeFunction extends SpliceFlatMapFunction<NormalizeOperation, ExecRow, ExecRow> {
    private static final long serialVersionUID = 7780564699906451370L;
    private boolean requireNotNull;

    public NormalizeFunction() {
    }

    public NormalizeFunction(OperationContext<NormalizeOperation> operationContext, boolean requireNotNull) {
        super(operationContext);
        this.requireNotNull = requireNotNull;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeBoolean(requireNotNull);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        requireNotNull = in.readBoolean();
    }

    @Override
    public Iterator<ExecRow> call(ExecRow sourceRow) throws Exception {

        NormalizeOperation normalize = operationContext.getOperation();
        normalize.source.setCurrentRow(sourceRow);
        if (sourceRow != null) {
            ExecRow normalized = null;
            try {
                normalized = normalize.normalizeRow(sourceRow, requireNotNull);
            } catch (StandardException e) {
                if (operationContext!=null && operationContext.isPermissive()) {
                    operationContext.recordBadRecord(e.getLocalizedMessage() + sourceRow.toString(), e);
                    return Collections.<ExecRow>emptyList().iterator();
                }
                throw e;
            }
            getActivation().setCurrentRow(normalized, normalize.getResultSetNumber());
            return new SingletonIterator(normalized.getClone());
        }else return Collections.<ExecRow>emptyList().iterator();
    }


}
