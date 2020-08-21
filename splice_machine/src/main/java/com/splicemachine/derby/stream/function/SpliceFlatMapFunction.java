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

import splice.com.google.common.base.Function;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.stream.iapi.OperationContext;
import javax.annotation.Nullable;
import java.util.Iterator;

/**
 * Created by dgomezferro on 4/4/14.
 */
public abstract class SpliceFlatMapFunction<Op extends SpliceOperation, From, To>
		extends AbstractSpliceFunction<Op> implements ExternalizableFlatMapFunction<From, To>, Function<From,Iterator<To>> {
	public SpliceFlatMapFunction() {
	}

	public SpliceFlatMapFunction(OperationContext<Op> operationContext) {
        super(operationContext);
	}

    @Nullable
    @Override
    public Iterator<To> apply(@Nullable From  from) {
        try {
            return call(from);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
