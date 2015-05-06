package com.splicemachine.derby.stream.temporary.update;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.RowLocation;
import com.splicemachine.derby.utils.marshall.EntryDataHash;
import com.splicemachine.derby.utils.marshall.dvd.DescriptorSerializer;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.encoding.MultiFieldEncoder;
import com.splicemachine.storage.EntryDecoder;
import com.splicemachine.storage.index.BitIndex;
import java.io.IOException;

/**
 * Created by jleach on 5/5/15.
 */
		/*
				 * Entity for encoding rows when the Primary Key has been modified
				 */
public class PkRowHash extends EntryDataHash {
    private ResultSupplier supplier;
    private EntryDecoder resultDecoder;
    private final FormatableBitSet finalHeapList;
    private final int[] colPositionMap;

    public PkRowHash(int[] keyColumns,
                     boolean[] keySortOrder,
                     FormatableBitSet finalHeapList,
                     int[] colPositionMap,
                     ResultSupplier supplier,
                     DescriptorSerializer[] serializers) {
        super(keyColumns, keySortOrder,serializers);
        this.finalHeapList = finalHeapList;
        this.colPositionMap = colPositionMap;
        this.supplier = supplier;
    }

    @Override
    public byte[] encode() throws StandardException, IOException {
        if(entryEncoder==null)
            entryEncoder = buildEntryEncoder();

        RowLocation location= (RowLocation)currentRow.getColumn(currentRow.nColumns()).getObject(); //the location to update is always at the end
        //convert Result into put under the new row key
        supplier.setLocation(location.getBytes());

        if(resultDecoder==null)
            resultDecoder = new EntryDecoder();

        supplier.setResult(resultDecoder);

        entryEncoder.reset(resultDecoder.getCurrentIndex());
        pack(entryEncoder.getEntryEncoder(),currentRow);
        return entryEncoder.encode();
    }

    @Override
    protected void pack(MultiFieldEncoder updateEncoder,
                        ExecRow currentRow) throws StandardException, IOException {
        BitIndex index = resultDecoder.getCurrentIndex();
        MultiFieldDecoder getFieldDecoder = resultDecoder.getEntryDecoder();
        for(int pos=index.nextSetBit(0);pos>=0;pos=index.nextSetBit(pos+1)){
            if(finalHeapList.isSet(pos+1)){
                DataValueDescriptor dvd = currentRow.getRowArray()[colPositionMap[pos+1]];
                DescriptorSerializer serializer = serializers[colPositionMap[pos+1]];
                serializer.encode(updateEncoder,dvd,false);
                resultDecoder.seekForward(getFieldDecoder, pos);
            }else{
                //use the index to get the correct offsets
                int offset = getFieldDecoder.offset();
                resultDecoder.seekForward(getFieldDecoder,pos);
                int limit = getFieldDecoder.offset()-1-offset;
                updateEncoder.setRawBytes(getFieldDecoder.array(),offset,limit);
            }
        }
    }
    public void close() throws IOException {
        if(supplier!=null)
            supplier.close();
        if(resultDecoder!=null)
            resultDecoder.close();
        super.close();
    }
}

