/*
 * Copyright (c) 2012 - 2019 Splice Machine, Inc.
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
package com.splicemachine.derby.stream.spark;

import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.stream.function.SplicePairFunction;
import scala.Tuple2;

/**
 * Created by jleach on 6/20/17.
 */
public class EmptySparkPairDataSet<V> extends SplicePairFunction<SpliceOperation, V, Object, Object> {

    public EmptySparkPairDataSet() {}

    @Override
    public Object genKey(V v) {
        return null;
    }

    @Override
    public Object genValue(V v) {
        return v;
    }

    @Override
    public Tuple2<Object, Object> call(V value) throws Exception {
        return new Tuple2<Object, Object>(null,value);
    }
}
