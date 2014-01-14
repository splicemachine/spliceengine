package com.splicemachine.si.impl.translate;

public interface Transcoder<Data1, Data2> {
    Data2 transcode(Data1 data);
    Data2 transcodeKey(Data1 key);
    Data2 transcodeFamily(Data1 family);
    Data2 transcodeQualifier(Data1 qualifier);
}
