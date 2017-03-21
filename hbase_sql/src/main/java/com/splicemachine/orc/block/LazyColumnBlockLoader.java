package com.splicemachine.orc.block;

/**
 * Created by jleach on 3/17/17.
 */
public interface LazyColumnBlockLoader<T extends ColumnBlock>
{
    void load(T block);
}