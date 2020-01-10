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
import com.splicemachine.kvpair.KVPair;
import scala.Tuple2;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Iterator;

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
        return new NullFilterKVPairIterator(conglomerateId, kvPairIterator);
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

class NullFilterKVPairIterator<K,V> implements Iterator<Tuple2<Long, Tuple2<byte[], byte[]>>> {
    Iterator<KVPair> delegate;
    long conglomerateId;
    KVPair kvPair;
    boolean rowConsumed = true;

    public NullFilterKVPairIterator(long cId, Iterator<KVPair> delegate) {
        this.delegate = delegate;
        this.conglomerateId = cId;
    }

    @Override
    public boolean hasNext() {
        // we need to check rowConsumed to make sure the same result is return in case hasNext() is called multiple times
        // before a call of next()
        if (!rowConsumed)
            return true;
        while (delegate.hasNext()) {
            kvPair = delegate.next();
            if (kvPair != null) {
                rowConsumed = false;
                return true;
            }
        }
        return false;
    }

    @Override
    public Tuple2<Long, Tuple2<byte[], byte[]>> next() {
        rowConsumed = true;
        Tuple2<byte[], byte[]> kv = new Tuple2(kvPair.getRowKey(), kvPair.getValue());
        return new Tuple2(conglomerateId, kv);
    }
}
