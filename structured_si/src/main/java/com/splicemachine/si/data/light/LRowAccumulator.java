package com.splicemachine.si.data.light;

import com.google.common.collect.Maps;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.encoding.MultiFieldEncoder;
import com.splicemachine.si.impl.RowAccumulator;
import com.splicemachine.utils.kryo.KryoPool;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Map;

public class LRowAccumulator implements RowAccumulator {
		private Map<byte[],byte[]> accumulation = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);

    @Override
    public boolean isOfInterest(KeyValue value) {
        return true;
    }

    @Override
		public boolean accumulate(KeyValue keyValue) throws IOException {
				byte[] packedData = keyValue.getValue();
				MultiFieldDecoder decoder = MultiFieldDecoder.wrap(packedData, KryoPool.defaultPool());
				int numEntries = decoder.decodeNextInt();
				for(int i=0;i<numEntries;i++){
						byte[] qual = decoder.decodeNextBytesUnsorted();
						byte[] val = decoder.decodeNextBytesUnsorted();
						if(!accumulation.containsKey(qual))
								accumulation.put(qual,val);
				}
//    	Map<String,Object> packedRow = (Map<String,Object>) keyValue.value;
//    	for (String k : packedRow.keySet()) {
//              if (!accumulation.containsKey(k)) {
//                  accumulation.put(k, packedRow.get(k));
//              }
//          }
				return true;
		}

    @Override
    public boolean isFinished() {
        return false;
    }

    @Override
    public byte[] result() {
				MultiFieldEncoder encoder = MultiFieldEncoder.create(KryoPool.defaultPool(), 2 * accumulation.size() + 1);
				encoder.encodeNext(accumulation.size());
				for(Map.Entry<byte[],byte[]> data:accumulation.entrySet()){
						encoder.encodeNextUnsorted(data.getKey());
						encoder.encodeNextUnsorted(data.getValue());
				}
				return encoder.build();
    }

		@Override public long getBytesVisited() { return 0; }

}
