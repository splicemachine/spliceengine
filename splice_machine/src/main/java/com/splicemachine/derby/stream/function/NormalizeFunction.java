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

package com.splicemachine.derby.stream.function;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.impl.sql.execute.operations.NormalizeOperation;
import com.splicemachine.derby.stream.iapi.OperationContext;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;

/**
 * Created by jleach on 11/4/15.
 */
public class NormalizeFunction extends SpliceFlatMapFunction<NormalizeOperation, LocatedRow, LocatedRow> {
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
    public Iterable<LocatedRow> call(LocatedRow sourceRow) throws Exception {

        NormalizeOperation normalize = operationContext.getOperation();
        normalize.source.setCurrentLocatedRow(sourceRow);
        if (sourceRow != null) {
            ExecRow normalized = null;
            try {
                normalized = normalize.normalizeRow(sourceRow.getRow(), requireNotNull);
            } catch (StandardException e) {
                if (operationContext!=null && operationContext.isPermissive()) {
                    operationContext.recordBadRecord(e.getLocalizedMessage() + sourceRow.toString(), e);
                    return Collections.emptyList();
                }
                throw e;
            }
            getActivation().setCurrentRow(normalized, normalize.getResultSetNumber());
            return Collections.singletonList(new LocatedRow(sourceRow.getRowLocation(), normalized.getClone()));
        }else return Collections.emptyList();
    }


}
