package com.splicemachine.si.impl.data.light;

import com.splicemachine.si.api.data.SRowLock;
import com.splicemachine.si.api.data.STableWriter;
import com.splicemachine.utils.ByteSlice;
import com.splicemachine.utils.Pair;

import java.io.IOException;
import java.util.List;

/**
 * Created by jleach on 12/10/15.
 */
public class LTableWriter implements STableWriter<LDelete,LMutation,LOperationStatus,Pair,LPut,LTable> {
    @Override
    public void write(LTable Table, LPut lPut) throws IOException {

    }

    @Override
    public void write(LTable Table, LPut lPut, SRowLock rowLock) throws IOException {

    }

    @Override
    public void write(LTable Table, LPut lPut, boolean durable) throws IOException {

    }

    @Override
    public void write(LTable Table, List<LPut> lPuts) throws IOException {

    }

    @Override
    public LOperationStatus[] writeBatch(LTable lTable, Pair[] puts) throws IOException {
        return new LOperationStatus[0];
    }

    @Override
    public void delete(LTable Table, LDelete lDelete, SRowLock lock) throws IOException {

    }

    @Override
    public SRowLock tryLock(LTable lTable, byte[] rowKey) throws IOException {
        return null;
    }

    @Override
    public SRowLock tryLock(LTable lTable, ByteSlice rowKey) throws IOException {
        return null;
    }

    @Override
    public SRowLock lockRow(LTable Table, byte[] rowKey) throws IOException {
        return null;
    }

    @Override
    public void unLockRow(LTable Table, SRowLock lock) throws IOException {

    }

    @Override
    public boolean checkAndPut(LTable Table, byte[] family, byte[] qualifier, byte[] expectedValue, LPut lPut) throws IOException {
        return false;
    }
}
