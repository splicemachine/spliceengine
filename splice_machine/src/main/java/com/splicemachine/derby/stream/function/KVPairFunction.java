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
import com.splicemachine.kvpair.KVPair;
import scala.Tuple2;

/**
 * Created by jyuan on 10/19/15.
 */
public class KVPairFunction extends SplicePairFunction<SpliceOperation,KVPair,byte[],KVPair> {
    private int counter = 0;
    public KVPairFunction() {
        super();
    }

    public KVPairFunction(OperationContext<SpliceOperation> operationContext) {
        super(operationContext);
    }

    @Override
    public Tuple2<byte[], KVPair> call(KVPair kvPair) throws Exception {
        if (kvPair ==null)
            return null;
        return new Tuple2<>(kvPair.getRowKey(),kvPair);
    }

    @Override
    public byte[] genKey(KVPair kvPair) {
        counter++;
        return kvPair.getRowKey();
    }

    @Override
    public KVPair genValue(KVPair kvPair) {
        return kvPair;
    }
}
