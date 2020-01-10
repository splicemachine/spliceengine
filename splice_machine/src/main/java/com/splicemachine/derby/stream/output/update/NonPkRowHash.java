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

package com.splicemachine.derby.stream.output.update;

import com.splicemachine.SpliceKryoRegistry;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.derby.utils.DerbyBytesUtil;
import com.splicemachine.derby.utils.marshall.EntryDataHash;
import com.splicemachine.derby.utils.marshall.dvd.DescriptorSerializer;
import com.splicemachine.encoding.MultiFieldEncoder;
import com.splicemachine.storage.EntryEncoder;
import com.carrotsearch.hppc.BitSet;

/*
* Entity for encoding rows when primary keys have not been modified
*/
public class NonPkRowHash extends EntryDataHash {
    private final FormatableBitSet finalHeapList;

    public NonPkRowHash(int[] keyColumns,
                        boolean[] keySortOrder,
                        DescriptorSerializer[] serializers,
                        FormatableBitSet finalHeapList) {
        super(keyColumns, keySortOrder,serializers);
        this.finalHeapList = finalHeapList;
    }

    @Override
    protected void pack(MultiFieldEncoder encoder, ExecRow currentRow) throws StandardException {
        encoder.reset();
        DataValueDescriptor[] dvds = currentRow.getRowArray();
        for(int i=finalHeapList.anySetBit();i>=0;i=finalHeapList.anySetBit(i)){
            int position = keyColumns[i];
            DataValueDescriptor dvd = dvds[position];
            DescriptorSerializer serializer = serializers[position];
            serializer.encode(encoder,dvd,false);
        }
    }

    @Override
    protected EntryEncoder buildEntryEncoder() {
        BitSet notNullFields = new BitSet(finalHeapList.size());
        BitSet scalarFields = new BitSet();
        BitSet floatFields = new BitSet();
        BitSet doubleFields = new BitSet();
        for(int i=finalHeapList.anySetBit();i>=0;i=finalHeapList.anySetBit(i)){
            DescriptorSerializer serializer = serializers[keyColumns[i]];
            notNullFields.set(i - 1);
            DataValueDescriptor dvd = currentRow.getRowArray()[keyColumns[i]];
            if(serializer.isScalarType()){
                scalarFields.set(i-1);
            }else if(DerbyBytesUtil.isFloatType(dvd)){
                floatFields.set(i-1);
            }else if(DerbyBytesUtil.isDoubleType(dvd)){
                doubleFields.set(i-1);
            }
        }
        return EntryEncoder.create(SpliceKryoRegistry.getInstance(),currentRow.nColumns(),
                notNullFields,scalarFields,floatFields,doubleFields);
    }

    @Override
    protected BitSet getNotNullFields(ExecRow row, BitSet notNullFields) {
        notNullFields.clear();
        for(int i=finalHeapList.anySetBit();i>=0;i=finalHeapList.anySetBit(i)){
            notNullFields.set(i-1);
        }
        return notNullFields;
    }
}

