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

import com.splicemachine.db.iapi.services.io.ArrayUtil;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.impl.sql.execute.operations.JoinOperation;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.utils.Pair;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 *
 * This class filters out nulls based on the hash keys provided.
 *
 */
public class InnerJoinNullFilterFunction extends SplicePredicateFunction<JoinOperation,ExecRow> {
    private boolean initialized = false;
    private int[] hashKeys;
    public InnerJoinNullFilterFunction() {
        super();
    }

    public InnerJoinNullFilterFunction(OperationContext<JoinOperation> operationContext, int[] hashKeys) {
        super(operationContext);
        assert hashKeys!=null && hashKeys.length >0 : "Bad Hash Keys Passed into Null Filter Function";
        this.hashKeys = hashKeys;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        ArrayUtil.writeIntArray(out,hashKeys);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        hashKeys = ArrayUtil.readIntArray(in);
    }

    @Override
    public boolean apply(@Nullable ExecRow locatedRow) {
        try {
            ExecRow row = locatedRow;
            for (int i = 0; i< hashKeys.length; i++) {
                if (row.getColumn(hashKeys[i]+1).isNull()) {
                    operationContext.recordFilter();
                    return false;
                }
            }
            operationContext.getOperation().setCurrentRow(locatedRow);
            return true;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean hasNativeSparkImplementation() {
        return true;
    }

    @Override
    public Pair<Dataset<Row>, OperationContext> nativeTransformation(Dataset<Row> input, OperationContext context) {
        Column andCols = null;
        for (int i : hashKeys) {
            Column col = input.col("c"+i).isNotNull();
            if (andCols ==null)
                andCols = col;
            else
                andCols = andCols.and(col);
        }
        return Pair.newPair(input.filter(andCols), null);
    }

}
