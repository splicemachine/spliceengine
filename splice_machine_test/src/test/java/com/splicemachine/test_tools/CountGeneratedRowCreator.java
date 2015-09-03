package com.splicemachine.test_tools;

/**
 * @author Scott Fines
 *         Date: 8/28/15
 */
public abstract class CountGeneratedRowCreator implements RowCreator{
    private final int batchSize;
    private final int maxCount;
    protected int position = 0;

    public CountGeneratedRowCreator(int maxCount){
        this(maxCount,1);
    }

    public CountGeneratedRowCreator(int maxCount,int batchSize){
        this.maxCount=maxCount;
        this.batchSize = batchSize;
    }

    @Override
    public boolean advanceRow(){
        return position++<=maxCount;
    }

    @Override public int batchSize(){ return batchSize; }

    @Override
    public void reset(){
       position = 0;
    }
}
