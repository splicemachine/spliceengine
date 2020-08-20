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

package com.splicemachine.ck.decoder;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.utils.marshall.EntryDataDecoder;
import com.splicemachine.derby.utils.marshall.dvd.DescriptorSerializer;
import com.splicemachine.utils.IntArrays;
import com.splicemachine.utils.Pair;
import org.apache.hadoop.hbase.Cell;

import java.util.BitSet;

public abstract class UserDataDecoder {

    public Pair<BitSet, ExecRow> decode(final Cell c) throws StandardException {
        Pair<ExecRow, DescriptorSerializer[]> p = getExecRowAndDescriptors();
        ExecRow er = p.getFirst();
        DescriptorSerializer[] serializerDescriptors = p.getSecond();
        EntryDataDecoder entryDataDecoder = new EntryDataDecoder(IntArrays.count(er.nColumns()), null, serializerDescriptors);
        entryDataDecoder.set(c.getValueArray(), c.getValueOffset(), c.getValueLength());
        entryDataDecoder.decode(er);
        BitSet bitSet = new BitSet(er.nColumns());
        for(int i = 0; i < er.nColumns(); ++i) {
            if(entryDataDecoder.getFieldDecoder().isSet(i)) {
                bitSet.set(i);
            }
        }
        return new Pair<>(bitSet, er);
    }

    public abstract Pair<ExecRow, DescriptorSerializer[]> getExecRowAndDescriptors() throws StandardException;
}
