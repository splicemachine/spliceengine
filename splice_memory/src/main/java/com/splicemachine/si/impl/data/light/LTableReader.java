package com.splicemachine.si.impl.data.light;

import com.splicemachine.collections.CloseableIterator;
import com.splicemachine.si.api.data.STableReader;

import java.io.IOException;

/**
 * Created by jleach on 12/10/15.
 */
public class LTableReader implements STableReader<LTable, LGet, LScan,LResult> {
    @Override
    public LTable open(String tableName) throws IOException {
        return null;
    }

    @Override
    public void close(LTable lTable) throws IOException {

    }

    @Override
    public String getTableName(LTable lTable) {
        return null;
    }

    @Override
    public LResult get(LTable lTable, LGet lGet) throws IOException {
        return null;
    }

    @Override
    public CloseableIterator<LResult> scan(LTable lTable, LScan lScan) throws IOException {
        return null;
    }

    @Override
    public void openOperation(LTable lTable) throws IOException {

    }

    @Override
    public void closeOperation(LTable lTable) throws IOException {

    }
}
