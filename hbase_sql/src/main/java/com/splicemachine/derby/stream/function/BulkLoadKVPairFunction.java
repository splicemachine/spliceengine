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

import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.kvpair.KVPair;
import scala.Tuple2;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * Created by jyuan on 10/9/17.
 */
public class BulkLoadKVPairFunction<Op extends SpliceOperation>
        extends SpliceFlatMapFunction<Op, Iterator<KVPair>, Tuple2<Long, Tuple2<byte[], byte[]>>> {

    private long conglomerateId;

    public BulkLoadKVPairFunction() {}

    public BulkLoadKVPairFunction(long conglomerateId) {
        this.conglomerateId = conglomerateId;
    }

    @Override
    public Iterator<Tuple2<Long, Tuple2<byte[], byte[]>>> call(Iterator<KVPair> kvPairIterator) throws Exception {
        if (!kvPairIterator.hasNext())
            return Collections.EMPTY_LIST.iterator();

        List<Tuple2<Long, Tuple2<byte[], byte[]>>> outList = new ArrayList<>();
        while (kvPairIterator.hasNext()) {
            KVPair kvPair = kvPairIterator.next();
            if (kvPair == null)
                continue;
            Tuple2<byte[], byte[]> kv = new Tuple2(kvPair.getRowKey(), kvPair.getValue());
            outList.add(new Tuple2(conglomerateId, kv));
        }

        return outList.iterator();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeLong(conglomerateId);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        conglomerateId = in.readLong();
    }
}
