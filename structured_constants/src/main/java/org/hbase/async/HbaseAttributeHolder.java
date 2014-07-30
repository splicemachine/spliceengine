package org.hbase.async;

import com.google.common.collect.Maps;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

/**
 * @author Scott Fines
 * Date: 7/15/14
 */
public class HbaseAttributeHolder extends FilterBase {

    public HbaseAttributeHolder() { }

    private Map<String,byte[]> attributes;

    public Map<String, byte[]> getAttributes() { return attributes; }

    @Override
    public void write(DataOutput out) throws IOException {
        WritableUtils.writeVInt(out,attributes.size());
        for(Map.Entry<String,byte[]> entry:attributes.entrySet()){
            org.apache.hadoop.hbase.util.Bytes.writeByteArray(out,entry.getKey().getBytes());
            org.apache.hadoop.hbase.util.Bytes.writeByteArray(out,entry.getValue());
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        int size = (int)WritableUtils.readVLong(in);
        attributes = Maps.newHashMap();
        for(int i=0;i<size;i++){
            byte[] keyBytes = org.apache.hadoop.hbase.util.Bytes.readByteArray(in);
            byte[] valueBytes = org.apache.hadoop.hbase.util.Bytes.readByteArray(in);
            attributes.put(new String(keyBytes),valueBytes);
        }
    }
}
