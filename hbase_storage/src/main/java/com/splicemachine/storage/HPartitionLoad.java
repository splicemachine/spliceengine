package com.splicemachine.storage;

/**
 * @author Scott Fines
 *         Date: 1/7/16
 */
public class HPartitionLoad implements PartitionLoad{
    private final int storefileSizeMB;
    private final int memStoreSizeMB;
    private final int storefileIndexSizeMB;
    private final String name;

    public HPartitionLoad(String name,int storefileSizeMB,int memStoreSizeMB,int storefileIndexSizeMB){
        this.storefileSizeMB=storefileSizeMB;
        this.memStoreSizeMB=memStoreSizeMB;
        this.storefileIndexSizeMB=storefileIndexSizeMB;
        this.name = name;
    }

    @Override
    public String getPartitionName(){
        return name;
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
