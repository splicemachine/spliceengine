package com.splicemachine.ck.decoder;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.utils.marshall.EntryDataDecoder;
import com.splicemachine.derby.utils.marshall.dvd.DescriptorSerializer;
import com.splicemachine.utils.IntArrays;
import com.splicemachine.utils.Pair;
import org.apache.hadoop.hbase.Cell;

public abstract class UserDataDecoder {

    public ExecRow decode(final Cell c) throws StandardException {
        Pair<ExecRow, DescriptorSerializer[]> p = getExecRowAndDescriptors();
        ExecRow er = p.getFirst();
        DescriptorSerializer[] serializerDescriptors = p.getSecond();
        EntryDataDecoder entryDataDecoder = new EntryDataDecoder(IntArrays.count(er.nColumns()), null, serializerDescriptors);
        entryDataDecoder.set(c.getValueArray(), c.getValueOffset(), c.getValueLength());
        entryDataDecoder.decode(er);
        return er;
    }

    protected abstract Pair<ExecRow, DescriptorSerializer[]> getExecRowAndDescriptors();
}
