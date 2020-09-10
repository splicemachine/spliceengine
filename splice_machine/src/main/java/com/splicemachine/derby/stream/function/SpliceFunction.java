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
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.stream.iapi.OperationContext;

import javax.annotation.Nullable;
import java.io.Serializable;

/**
 * Created by dgomezferro on 4/4/14.
 */
public abstract class SpliceFunction<Op extends SpliceOperation, From, To>
    extends AbstractSpliceFunction<Op>
		implements ExternalizableFunction<From, To>, splice.com.google.common.base.Function<From,To>, Serializable {

	public SpliceFunction() {
        super();
	}
	public SpliceFunction(OperationContext<Op> operationContext) {
        super(operationContext);
	}

    @Nullable
    @Override
    public To apply(@Nullable From from) {
        try {
            return call(from);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
