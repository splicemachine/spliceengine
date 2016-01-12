package com.splicemachine.storage;

/**
 * @author Scott Fines
 *         Date: 1/12/16
 */
public class MPartitionLoad implements PartitionLoad{
    private final String partitionName;

    public MPartitionLoad(String partitionName){
        this.partitionName=partitionName;
    }

    @Override
    public int getStorefileSizeMB(){
        return 0;
    }

    @Override
    public int getMemStoreSizeMB(){
        return 1;
    }

    @Override
    public int getStorefileIndexSizeMB(){
        return 0;
    }

    @Override
    public String getPartitionName(){
        return partitionName;
    }
}
