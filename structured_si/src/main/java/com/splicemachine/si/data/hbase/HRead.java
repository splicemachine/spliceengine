package com.splicemachine.si.data.hbase;

import com.splicemachine.si.data.api.SRead;

public interface HRead extends SRead {
    void setReadTimeRange(long minTimestamp, long maxTimestamp);
    void setReadMaxVersions();
    void setReadMaxVersions(int max);
    void addFamilyToRead(byte[] family);
    void addFamilyToReadIfNeeded(byte[] family);
}
