package org.apache.hadoop.hive.ql.io.orc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

/**
 * In the OrcFileWriter we see a lot of contention on the MemoryManager.
 * Use this MemoryManager to avoid the contention if you won't need the MemoryManager.
 */
public class NullMemoryManager
        extends MemoryManager
{
    public NullMemoryManager(Configuration conf)
    {
        super(conf);
    }

    @Override
    void addWriter(Path path, long requestedAllocation, Callback callback) {}

    @Override
    void removeWriter(Path path) {}

    @Override
    long getTotalMemoryPool()
    {
        return 0;
    }

    @Override
    double getAllocationScale()
    {
        return 0;
    }

    @Override
    void addedRow() {}
}