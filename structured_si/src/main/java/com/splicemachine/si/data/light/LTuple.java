package com.splicemachine.si.data.light;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.encoding.MultiFieldEncoder;
import com.splicemachine.utils.kryo.KryoPool;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.OperationWithAttributes;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.*;

public class LTuple extends OperationWithAttributes{
    final byte[] key;
    final List<KeyValue> values;
    final Integer lock;

    public LTuple(byte[] key, List<KeyValue> values) {
				this(key,values,Maps.<String,byte[]>newHashMap(),null);
    }

    public LTuple(byte[] key, List<KeyValue> values, Map<String, byte[]> attributes) {
				this(key,values,attributes,null);
    }

		public LTuple(byte[] key, List<KeyValue> values, Map<String, byte[]> attributes, Integer lock) {
				this.key = key;
				this.values = values;
				this.lock = lock;
				for(Map.Entry<String,byte[]> attributePair:attributes.entrySet()){
						super.setAttribute(attributePair.getKey(),attributePair.getValue());
				}
		}

    public LTuple(byte[] key, List<KeyValue> values, Integer lock) {
				this(key,values,Maps.<String,byte[]>newHashMap(),lock);
    }

    public LTuple pack(byte[] familyToPack, byte[] packedQualifier) {
        List<KeyValue> newValues = Lists.newArrayList();
				byte[] newRowKey = null;
        Long newTimestamp = null;
        boolean nothingPacked = true;
        Map<byte[], byte[]> packedRow = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
        for (KeyValue keyValue : values) {
						if(keyValue.matchingFamily(familyToPack)){
                packedRow.put(keyValue.getQualifier(), keyValue.getValue());
                if (nothingPacked) {
                    newRowKey = keyValue.getKey();
                } else {
                    if (!keyValue.matchingRow(newRowKey)) {
                        throw new RuntimeException("rowkey mis-match");
                    }
                }
                if (nothingPacked) {
                    newTimestamp = keyValue.getTimestamp();
                } else {
                    if (!newTimestamp.equals(keyValue.getTimestamp())) {
                        throw new RuntimeException("timestamp mis-match");
                    }
                }
                nothingPacked = false;
            } else {
                newValues.add(keyValue);
            }
        }
        if (!nothingPacked) {
						MultiFieldEncoder encoder =MultiFieldEncoder.create(KryoPool.defaultPool(),packedRow.size()*2+1);
						encoder.encodeNext(packedRow.size());
						for(Map.Entry<byte[],byte[]> packedRowColumn:packedRow.entrySet()){
								byte[] qual = packedRowColumn.getKey();
								byte[] val = packedRowColumn.getValue();
								encoder.encodeNextUnsorted(qual).encodeNextUnsorted(val);
						}
            newValues.add(new KeyValue(newRowKey, familyToPack, packedQualifier, newTimestamp, encoder.build()));
        }
        return new LTuple(key, newValues, new HashMap<String, byte[]>(getAttributesMap()));
    }

		public static List<KeyValue> unpack(List<KeyValue> values,byte[] familyToUnpack){
				List<KeyValue> newValues = Lists.newArrayList();
				for (KeyValue keyValue : values) {
						if(keyValue.matchingFamily(familyToUnpack)){
								byte[] packedData = keyValue.getValue();
								MultiFieldDecoder decoder = MultiFieldDecoder.wrap(packedData,KryoPool.defaultPool());
								int numEntries = decoder.decodeNextInt();
								for(int i=0;i<numEntries;i++){
										byte[] qual = decoder.decodeNextBytesUnsorted();
										byte[] val = decoder.decodeNextBytesUnsorted();
										final KeyValue newKeyValue = new KeyValue(keyValue.getRow(), keyValue.getFamily(),qual, keyValue.getTimestamp(), val);
										newValues.add(newKeyValue);
								}

////                Map<byte[], Object> packedRow = (Map<byte[], Object>) keyValue.value;
//                for (byte[] k : packedRow.keySet()) {
//                    final LKeyValue newKeyValue = new LKeyValue(keyValue.rowKey, keyValue.family, k, keyValue.timestamp, packedRow.get(k));
//                    newValues.add(newKeyValue);
//                }
						} else {
								newValues.add(keyValue);
						}
				}
				return newValues;
		}

    public LTuple unpackTuple(byte[] familyToUnpack) {
        return new LTuple(key, unpack(values,familyToUnpack), new HashMap<String, byte[]>(getAttributesMap()));
    }

		/*
		 * Methods for interface compliance with OperationWithAttributes, we don't actually use them anywhere.
		 */
		@Override public Map<String, Object> getFingerprint() { throw new UnsupportedOperationException(); }
		@Override public Map<String, Object> toMap(int maxCols) { throw new UnsupportedOperationException(); }

		public List<KeyValue> getValues(){
				List<KeyValue> keyValues = Lists.newArrayList(values);
				LStore.sortValues(keyValues);
				return keyValues;
		}

		@Override
    public String toString() {
        return "<" + Bytes.toString(key) + " " + values + ">";
    }

}
