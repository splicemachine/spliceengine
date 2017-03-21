package com.splicemachine.orc.block;

import org.apache.spark.sql.types.DataType;

import java.io.IOException;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

/**
 * Created by jleach on 3/17/17.
 */
public class LazyColumnBlockLoaderImpl
        implements LazyColumnBlockLoader<LazyColumnBlock>
{
    //private final int expectedBatchId = batchId;
    private final int columnIndex;
    private final DataType type;
    private boolean loaded;

    public LazyColumnBlockLoaderImpl(int columnIndex, DataType type)
    {
        this.columnIndex = columnIndex;
        this.type = requireNonNull(type, "type is null");
    }

    @Override
    public final void load(LazyColumnBlock lazyBlock)
    {
        if (loaded) {
            return;
        }
/*
        checkState(batchId == expectedBatchId);

        try {
            Block block = recordReader.readBlock(type, columnIndex);
            lazyBlock.setBlock(block);
        }
        catch (IOException e) {
            if (e instanceof OrcCorruptionException) {
                throw new PrestoException(HIVE_BAD_DATA, e);
            }
            throw new PrestoException(HIVE_CURSOR_ERROR, e);
        }

        loaded = true;
        */
    }
}
