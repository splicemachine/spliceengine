package org.apache.hadoop.hive.ql.io.orc;

import org.apache.hadoop.conf.Configuration;

/**
 * Allow access to certain package private methods of WriterOptions,
 * primarily used for providing the memory manager to the writer
 */
public class OrcWriterOptions
        extends OrcFile.WriterOptions
{
    public OrcWriterOptions(Configuration conf)
    {
        super(conf);
    }

    @Override
    public OrcWriterOptions memory(MemoryManager value)
    {
        super.memory(value);
        return this;
    }
}
