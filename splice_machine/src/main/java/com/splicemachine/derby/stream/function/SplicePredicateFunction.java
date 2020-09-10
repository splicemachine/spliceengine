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

import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.stream.iapi.OperationContext;
import splice.com.google.common.base.Predicate;

/**
 * Created by jleach on 4/22/15.
 */
public abstract class SplicePredicateFunction<Op extends SpliceOperation, From>
        extends AbstractSpliceFunction<Op>
        implements ExternalizableFunction<From, Boolean>, Predicate<From> {

    public SplicePredicateFunction() {
        super();
    }
    public SplicePredicateFunction(OperationContext<Op> operationContext) {
        super(operationContext);
    }

    @Override
    public Boolean call(From from) throws Exception {
        return apply(from);
    }


}
