package com.splicemachine.si.data.light;

import com.splicemachine.si.data.hbase.RollForwardable;

public class LTable implements RollForwardable {
    final String relationIdentifier;

    public LTable(String relationIdentifier) {
        this.relationIdentifier = relationIdentifier;
    }

    @Override
    public boolean checkRegionForRow(byte[] row) {
//        throw new RuntimeException("not implemented");
        return true;
    }
}
