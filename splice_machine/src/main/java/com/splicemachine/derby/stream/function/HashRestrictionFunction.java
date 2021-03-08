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

import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.derby.impl.sql.execute.operations.CrossJoinOperation;
import com.splicemachine.derby.impl.sql.execute.operations.JoinOperation;
import com.splicemachine.derby.stream.iapi.OperationContext;
import org.apache.spark.sql.Row;
import scala.Tuple2;

import javax.annotation.Nullable;

/**
 * Created by jleach on 4/22/15.
 */
public class HashRestrictionFunction extends SplicePredicateFunction<JoinOperation,Tuple2<ExecRow, Tuple2<ExecRow, ExecRow>>> {
    public HashRestrictionFunction() {
        super();
    }

    public HashRestrictionFunction(OperationContext<JoinOperation> operationContext) {
        super(operationContext);
    }

    @Override
    public boolean apply(@Nullable Tuple2<ExecRow, Tuple2<ExecRow, ExecRow>> tuple) {
        int[] leftKeys = operationContext.getOperation().getLeftHashKeys();
        int[] rightKeys = operationContext.getOperation().getRightHashKeys();
        DataValueDescriptor[] left = tuple._1().getRowArray();
        DataValueDescriptor[] right = tuple._2()._2().getRowArray();
        try {
            for(int i = 0; i < leftKeys.length; ++i) {
                DataValueDescriptor lv = left[leftKeys[i]];
                DataValueDescriptor rv = right[rightKeys[i]];
                if (lv.compare(rv) != 0)
                    return false;
            }
            return true;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
