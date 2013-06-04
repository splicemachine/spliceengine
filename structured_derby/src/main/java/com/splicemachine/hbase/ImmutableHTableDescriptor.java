package com.splicemachine.hbase;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;

import java.io.IOException;
import java.util.Map;

/**
 * Drop-in replacement for UnmodifyableHTableDescriptor, because HTable does
 * not expose that interface
 * @author Scott Fines
 * Created on: 3/8/13
 */
class ImmutableHTableDescriptor extends HTableDescriptor{

    ImmutableHTableDescriptor() { }

    ImmutableHTableDescriptor(HTableDescriptor desc) {
        super(desc);
    }

    @Override
    public void addFamily(HColumnDescriptor family) {
        throw new UnsupportedOperationException("Read only HTableDescriptor");
    }

    @Override
    public HColumnDescriptor removeFamily(byte[] column) {
        throw new UnsupportedOperationException("Read only HTableDescriptor");
    }

    @Override
    public void setReadOnly(boolean readOnly) {
        throw new UnsupportedOperationException("Read only HTableDescriptor");
    }

    @Override
    public void setValue(String key, String value) {
        throw new UnsupportedOperationException("Read only HTableDescriptor");
    }

    @Override
    public void setValue(byte[] key, byte[] value) {
        throw new UnsupportedOperationException("Read only HTableDescriptor");
    }

    @Override
    public void setMaxFileSize(long maxFileSize) {
        throw new UnsupportedOperationException("Read only HTableDescriptor");
    }

    @Override
    public void setMemStoreFlushSize(long memstoreFlushSize) {
        throw new UnsupportedOperationException("Read only HTableDescriptor");
    }

    @Override
    @Deprecated
    public void setOwnerString(String ownerString) {
        throw new UnsupportedOperationException("Read only HTableDescriptor");
    }

    @Override
    public void addCoprocessor(String className) throws IOException {
        throw new UnsupportedOperationException("Read only HTableDescriptor");
    }

    @Override
    public void addCoprocessor(String className, Path jarFilePath, int priority, Map<String, String> kvs) throws IOException {
        throw new UnsupportedOperationException("Read only HTableDescriptor");
    }
}
