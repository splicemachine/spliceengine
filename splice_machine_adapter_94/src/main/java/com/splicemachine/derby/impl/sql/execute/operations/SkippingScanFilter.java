package com.splicemachine.derby.impl.sql.execute.operations;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Pair;

import com.splicemachine.hbase.AbstractSkippingScanFilter;

import java.util.List;

/**
 * @author Scott Fines
 *         Date: 7/22/14
 */
public class SkippingScanFilter extends AbstractSkippingScanFilter<KeyValue> {
    public SkippingScanFilter() {
    	super();
    }

    public SkippingScanFilter(List<Pair<byte[], byte[]>> startStopKeys, List<byte[]> predicates) {
    	super(startStopKeys,predicates);
    }


    @Override
    public ReturnCode filterKeyValue(KeyValue kv) {
    	return internalFilter(kv);
    }
}
