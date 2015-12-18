package com.splicemachine.si.impl.data.light;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class LGet extends LOperationWithAttributes{
    final byte[] startTupleKey;
    final byte[] endTupleKey;
    final List<byte[]> families;
    final List<List<byte[]>> columns;
    Long effectiveTimestamp;
    int maxVersions;

    @SuppressFBWarnings("EI_EXPOSE_REP2")
    public LGet(byte[] startTupleKey,byte[] endTupleKey,List<byte[]> families,List<List<byte[]>> columns,
                Long effectiveTimestamp){
        this.startTupleKey=startTupleKey;
        this.endTupleKey=endTupleKey;
        this.families=families;
        this.columns=columns;
        this.effectiveTimestamp=effectiveTimestamp;
    }

    public LGet(byte[] startTupleKey,byte[] endTupleKey,List<byte[]> families,List<List<byte[]>> columns,
                Long effectiveTimestamp,int maxVersions){
        this(startTupleKey,endTupleKey,families,columns,effectiveTimestamp);
        this.maxVersions=maxVersions;
    }

    @Override
    public String toString(){
        return String.format("LGET { startTupleKey=%s, endTupleKey=%s, familes=%s, columns=%s, effectiveTimestamp=%s, maxVersions=%d}",
                Arrays.toString(startTupleKey),Arrays.toString(endTupleKey),families,columns,effectiveTimestamp,maxVersions);
    }


}
