package com.splicemachine.si.data.light;

import com.google.common.collect.Lists;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.si.data.api.SDataLib;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.*;

public class LDataLib implements SDataLib<LTuple, LTuple, LGet, LGet> {

    @Override
    public byte[] newRowKey(Object... args) {
        StringBuilder builder = new StringBuilder();
        for (Object a : args) {
            Object toAppend = a;
            if (a instanceof Short) {
                toAppend = String.format("%1$06d", a);
            } else if (a instanceof Long) {
                toAppend = String.format("%1$020d", a);
            } else if (a instanceof Byte) {
                toAppend = String.format("%1$02d", a);
            }
            builder.append(toAppend);
        }
        return Bytes.toBytes(builder.toString());
    }

		private boolean nullSafeComparison(Object o1, Object o2) {
				if(o1==null){
						return o2==null;
				}else if(o2==null) return false;

				if(o1 instanceof byte[] && o2 instanceof byte[])
						return Arrays.equals((byte[])o1,(byte[])o2);
				else
						return o1.equals(o2);

//        return (o1 == null && o2 == null) || ((o1 != null) && o1.equals(o2));
    }

    public boolean valuesMatch(Object family1, Object family2) {
        return nullSafeComparison(family1, family2);
    }

    @Override
    public byte[] encode(Object value) {
				if(value instanceof String){
						return Bytes.toBytes((String)value);
				}else if(value instanceof Boolean)
						return Bytes.toBytes((Boolean)value);
				else if(value instanceof Integer)
						return Bytes.toBytes((Integer)value);
				else if(value instanceof Long)
						return Bytes.toBytes((Long)value);
				else if(value instanceof Byte)
						return new byte[]{(Byte)value};
				else if(value instanceof Short)
						return Bytes.toBytes((Short)value);
				else
						return (byte[])value;
    }


		@SuppressWarnings("unchecked")
		@Override
    public <T> T decode(byte[] value, Class<T> type) {
        if (!(value instanceof byte[])) {
            return (T)value;
        }

				if(byte[].class.equals(type))
						return (T)value;
				if(String.class.equals(type))
						return (T)Bytes.toString(value);
				else if(Long.class.equals(type))
						return (T)(Long)Bytes.toLong(value);
				else if(Integer.class.equals(type)){
						if(value.length<4)
								return (T)new Integer(-1);
						return (T)(Integer)Bytes.toInt(value);
				}else if(Boolean.class.equals(type))
						return (T)(Boolean)Bytes.toBoolean(value);
				else if(Byte.class.equals(type))
						return (T)(Byte) value[0];
				else
						throw new RuntimeException("types don't match " + value.getClass().getName() + " " + type.getName() + " " + value);
    }

		@Override
    public void addKeyValueToPut(LTuple put, byte[] family, byte[] qualifier, long timestamp, byte[] value) {
        addKeyValueToTuple(put, family, qualifier, timestamp, value);
    }

    private void addKeyValueToTuple(LTuple tuple, Object family, Object qualifier, long timestamp, byte[] value) {
				KeyValue newCell = new KeyValue(tuple.key, (byte[]) family, (byte[]) qualifier, timestamp, value);
    			tuple.values.add(newCell);        
    }

		@Override
    public LTuple newPut(byte[] key) {
        return newPut(key, null);
    }

    @Override
    public LTuple newPut(byte[] key, Integer lock) {
        return new LTuple(key, new ArrayList<KeyValue>(), lock);
    }

		@Override
    public LGet newGet(byte[] rowKey, List<byte[]> families, List<List<byte[]>> columns, Long effectiveTimestamp) {
        return new LGet(rowKey, rowKey, families, columns, effectiveTimestamp);
    }

    @Override
    public byte[] getGetRow(LGet get) {
        return get.startTupleKey;
    }

    @Override
    public void setGetTimeRange(LGet get, long minTimestamp, long maxTimestamp) {
        assert minTimestamp == 0L;
        get.effectiveTimestamp = maxTimestamp - 1;
    }

    @Override
    public void setGetMaxVersions(LGet get) {
    }

    @Override
    public void setGetMaxVersions(LGet get, int max) {
    }

    @Override
    public void addFamilyToGet(LGet get, byte[] family) {
        get.families.add(family);
    }

    @Override
    public void addFamilyToGetIfNeeded(LGet get, byte[] family) {
        ensureFamilyDirect(get, family);
    }

    @Override
    public void setScanTimeRange(LGet get, long minTimestamp, long maxTimestamp) {
        assert minTimestamp == 0L;
        get.effectiveTimestamp = maxTimestamp - 1;
    }

    @Override
    public void setScanMaxVersions(LGet get) {
    }

		@Override
    public void addFamilyToScan(LGet get, byte[] family) {
        get.families.add(family);
    }

    @Override
    public void addFamilyToScanIfNeeded(LGet get, byte[] family) {
        ensureFamilyDirect(get, family);
    }

    private void ensureFamilyDirect(LGet lGet, byte[] family) {
        if (lGet.families.isEmpty() && (lGet.columns == null || lGet.columns.isEmpty())) {
        } else {
            if (lGet.families.contains(family)) {
            } else {
                lGet.families.add(family);
            }
        }
    }

    @Override
    public LGet newScan(byte[] startRowKey, byte[] endRowKey, List<byte[]> families, List<List<byte[]>> columns, Long effectiveTimestamp) {
        return new LGet(startRowKey, endRowKey, families, columns, effectiveTimestamp);
    }

		@Override
    public byte[] getPutKey(LTuple put) {
        return getTupleKey(put);
    }

    private byte[] getTupleKey(Object result) {
        return ((LTuple) result).key;
    }

    private List<KeyValue> getValuesForColumn(Result tuple, byte[] family, byte[] qualifier) {
        KeyValue[] values = tuple.raw();
        List<KeyValue> results = Lists.newArrayList();
				for (KeyValue v : values) {
						if(v.matchingColumn(family,qualifier)){
								results.add(v);
						}
            if (valuesMatch(v.getFamily(), family) && valuesMatch(v.getFamily(), qualifier)) {
                results.add(v);
            }
        }
        LStore.sortValues(results);
        return results;
    }

    private List<KeyValue> getValuesForFamily(Result tuple, byte[] family) {
        KeyValue[] values = tuple.raw();
        List<KeyValue> results = Lists.newArrayList();
        for (KeyValue v: values) {
						if(v.matchingFamily(family))
                results.add(v);
        }
        return results;
    }

		@Override
		public List<KeyValue> listResult(Result result) {
				List<KeyValue> values = Lists.newArrayList(result.raw());
				LStore.sortValues(values);
				return values;
    }

    @Override
    public Iterable<KeyValue> listPut(LTuple put) {
				List<KeyValue> values = Lists.newArrayList(put.values);
				LStore.sortValues(values);
				return values;
    }


		@Override
    public LTuple newDelete(byte[] rowKey) {
        return newPut(rowKey, null);
    }

    @Override
    public void addKeyValueToDelete(LTuple delete, byte[] family, byte[] qualifier, long timestamp) {
    	addKeyValueToTuple(delete, family, qualifier, timestamp, null);
    }

		@Override
		public KVPair toKVPair(LTuple lTuple) {
				return new KVPair(lTuple.key,lTuple.values.get(0).getValue());
		}

		@Override
		public LTuple toPut(KVPair kvPair, byte[] family, byte[] column, long longTransactionId) {
				KeyValue kv = new KeyValue(kvPair.getRow(),family,column,longTransactionId,kvPair.getValue());
				LTuple tuple = new LTuple(kvPair.getRow(),Lists.newArrayList(kv));
				return tuple;
		}

		@Override
		public LGet newGet(byte[] rowKey, List<byte[]> families,List<List<byte[]>> columns, Long effectiveTimestamp, int maxVersions) {
			return new LGet(rowKey, rowKey,families,columns,effectiveTimestamp,maxVersions);
		}
}
