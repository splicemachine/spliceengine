package com.splicemachine.orc;

import com.splicemachine.orc.AbstractOrcDataSource;
import io.airlift.units.DataSize;
import org.apache.hadoop.fs.FSDataInputStream;

import java.io.IOException;

import static java.lang.String.format;

public class HdfsOrcDataSource
        extends AbstractOrcDataSource
{
    private final FSDataInputStream inputStream;

    public HdfsOrcDataSource(String name, long size, DataSize maxMergeDistance, DataSize maxReadSize, DataSize streamBufferSize, FSDataInputStream inputStream)
    {
        super(name, size, maxMergeDistance, maxReadSize, streamBufferSize);
        this.inputStream = inputStream;
    }

    @Override
    public void close() throws IOException {
        inputStream.close();
    }

    @Override
    protected void readInternal(long position, byte[] buffer, int bufferOffset, int bufferLength)
            throws IOException {
        try {
            inputStream.readFully(position, buffer, bufferOffset, bufferLength);
        }
        catch (Exception e) {
            String message = format("HDFS error reading from %s at position %s", this, position);
            if (e.getClass().getSimpleName().equals("BlockMissingException")) {
                message = message + ": Block Missing Exception";
            }
            throw new IOException(message, e);
        }
    }
}

