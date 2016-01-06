package com.splicemachine.storage;

/**
 * @author Scott Fines
 *         Date: 1/7/16
 */
public class HPartitionLoad implements PartitionLoad{
    private final int storefileSizeMB;
    private final int memStoreSizeMB;
    private final int storefileIndexSizeMB;

    public HPartitionLoad(int storefileSizeMB,int memStoreSizeMB,int storefileIndexSizeMB){
        this.storefileSizeMB=storefileSizeMB;
        this.memStoreSizeMB=memStoreSizeMB;
        this.storefileIndexSizeMB=storefileIndexSizeMB;
    }

    @Override
    public int getStorefileSizeMB(){
        return storefileSizeMB;
    }

    @Override
    public int getMemStoreSizeMB(){
        return memStoreSizeMB;
    }

    @Override
    public int getStorefileIndexSizeMB(){
        return storefileIndexSizeMB;
    }
}
