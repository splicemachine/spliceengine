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

package com.splicemachine.derby.stream.output.update;

import com.carrotsearch.hppc.BitSet;
import com.splicemachine.SpliceKryoRegistry;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.RowLocation;
import com.splicemachine.derby.utils.DerbyBytesUtil;
import com.splicemachine.derby.utils.marshall.EntryDataHash;
import com.splicemachine.derby.utils.marshall.dvd.DescriptorSerializer;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.encoding.MultiFieldEncoder;
import com.splicemachine.storage.EntryDecoder;
import com.splicemachine.storage.EntryEncoder;
import com.splicemachine.storage.index.BitIndex;
import com.splicemachine.storage.index.DenseCompressedBitIndex;
import com.splicemachine.storage.index.SparseBitIndex;
import com.splicemachine.storage.index.UncompressedBitIndex;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;

/**
 * Entity for encoding rows when the Primary Key has been modified
 * Created by jleach on 5/5/15.
 */
public class PkRowHash extends EntryDataHash{
    private ResultSupplier supplier;
    private EntryDecoder resultDecoder;
    private final FormatableBitSet finalHeapList;
    private final int[] colPositionMap;

    @SuppressFBWarnings(value="EI_EXPOSE_REP2", justification="Intentional")
    public PkRowHash(int[] keyColumns,
                     boolean[] keySortOrder,
                     FormatableBitSet finalHeapList,
                     int[] colPositionMap,
                     ResultSupplier supplier,
                     DescriptorSerializer[] serializers){
        super(keyColumns,keySortOrder,serializers);
        this.finalHeapList=finalHeapList;
        this.colPositionMap=colPositionMap;
        this.supplier=supplier;
    }

    @Override
    public byte[] encode() throws StandardException, IOException{

        RowLocation location=(RowLocation)currentRow.getColumn(currentRow.nColumns()).getObject(); //the location to update is always at the end
        //convert Result into put under the new row key
        supplier.setLocation(location.getBytes());

        if(resultDecoder==null)
            resultDecoder=new EntryDecoder();

        supplier.setResult(resultDecoder);

        // Merge the BitIndex of the old row with the
        // BitIndex of the new row via a new buildEntryEncoder method.
        BitIndex bitIndexForFetchedRow = resultDecoder.getCurrentIndex();
        BitSet fieldsToUpdate = (BitSet)bitIndexForFetchedRow.getFields().clone();
        BitSet scalarFields   = (BitSet)bitIndexForFetchedRow.getScalarFields().clone();
        BitSet floatFields    = (BitSet)bitIndexForFetchedRow.getFloatFields().clone();
        BitSet doubleFields   = (BitSet)bitIndexForFetchedRow.getDoubleFields().clone();

        if(entryEncoder==null)
            entryEncoder=buildEntryEncoder(fieldsToUpdate, scalarFields, floatFields, doubleFields);
        else {
            addBitSetsForFinalHeapList(fieldsToUpdate, scalarFields, floatFields, doubleFields);
            entryEncoder.reset(fieldsToUpdate, scalarFields, floatFields, doubleFields);
        }
        pack(entryEncoder.getEntryEncoder(), currentRow, fieldsToUpdate);
        return entryEncoder.encode();
    }

    // A version of buildEntryEncoder that combines bits representing
    // non-null columns in the PK row we're going to update with bits
    // representing columns (both null and non-null) in finalHeapList,
    // which holds the new column values to write in the replacement row.
    protected EntryEncoder buildEntryEncoder(BitSet fieldsToUpdate,
                                             BitSet scalarFields,
                                             BitSet floatFields,
                                             BitSet doubleFields) {
        addBitSetsForFinalHeapList(fieldsToUpdate, scalarFields, floatFields, doubleFields);

        return EntryEncoder.create(SpliceKryoRegistry.getInstance(),currentRow.nColumns(),
                fieldsToUpdate,scalarFields,floatFields,doubleFields);
    }

    // For each column in finalHeapList, add bits to the proper passed-in BitSets
    // Depending on the column data type.
    protected void addBitSetsForFinalHeapList(BitSet fieldsToUpdate,
                                              BitSet scalarFields,
                                              BitSet floatFields,
                                              BitSet doubleFields) {
        for(int i=finalHeapList.anySetBit();i>=0;i=finalHeapList.anySetBit(i)){
            DescriptorSerializer serializer = serializers[colPositionMap[i]];
            fieldsToUpdate.set(i - 1);
            DataValueDescriptor dvd = currentRow.getRowArray()[colPositionMap[i]];
            if(serializer.isScalarType()){
                scalarFields.set(i-1);
            }else if(DerbyBytesUtil.isFloatType(dvd)){
                floatFields.set(i-1);
            }else if(DerbyBytesUtil.isDoubleType(dvd)){
                doubleFields.set(i-1);
            }
        }
    }

    @Override
    protected void pack(MultiFieldEncoder updateEncoder,
                        ExecRow currentRow) throws StandardException, IOException{
        BitIndex index=resultDecoder.getCurrentIndex();
        pack(updateEncoder, currentRow, index.getFields());
    }

    protected void pack(MultiFieldEncoder updateEncoder,
                        ExecRow currentRow,
                        BitSet fieldsToUpdate) throws StandardException, IOException{
        MultiFieldDecoder getFieldDecoder=resultDecoder.getEntryDecoder();
        BitSet oldPresentFields=resultDecoder.getCurrentIndex().getFields();

        for(int pos=fieldsToUpdate.nextSetBit(0);pos>=0;pos=fieldsToUpdate.nextSetBit(pos+1)){
            if(finalHeapList.isSet(pos+1)){
                DataValueDescriptor dvd=currentRow.getRowArray()[colPositionMap[pos+1]];
                DescriptorSerializer serializer=serializers[colPositionMap[pos+1]];
                serializer.encode(updateEncoder,dvd,false);
                // Only skip over the next field if it is present.
                if (oldPresentFields.get(pos))
                    resultDecoder.seekForward(getFieldDecoder,pos);
            }else{
                //use the index to get the correct offsets
                int offset=getFieldDecoder.offset();
                resultDecoder.seekForward(getFieldDecoder,pos);
                int limit=getFieldDecoder.offset()-1-offset;
                updateEncoder.setRawBytes(getFieldDecoder.array(),offset,limit);
            }
        }
    }

    public void close() throws IOException{
        if(supplier!=null)
            supplier.close();
        if(resultDecoder!=null)
            resultDecoder.close();
        super.close();
    }
}

