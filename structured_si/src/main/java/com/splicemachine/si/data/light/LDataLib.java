package com.splicemachine.si.data.light;

import com.google.common.collect.Lists;
import com.splicemachine.constants.SIConstants;
import com.splicemachine.constants.bytes.BytesUtil;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.hbase.KeyValueUtils;
import com.splicemachine.metrics.MetricFactory;
import com.splicemachine.si.data.api.SDataLib;
import com.splicemachine.si.impl.SICompactionState;
import com.splicemachine.utils.ByteSlice;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.*;

public class LDataLib implements SDataLib<KeyValue,LTuple, LTuple, LGet, LGet> {

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
		public <T> T decode(byte[] value, int offset, int length, Class<T> type) {
				if (!(value instanceof byte[])) {
						return (T)value;
				}

				if(byte[].class.equals(type))
						return (T)value;
				if(String.class.equals(type))
						return (T)Bytes.toString(value,offset,length);
				else if(Long.class.equals(type))
						return (T)(Long)Bytes.toLong(value,offset);
				else if(Integer.class.equals(type)){
						if(length<4)
								return (T)new Integer(-1);
						return (T)(Integer)Bytes.toInt(value,offset);
				}else if(Boolean.class.equals(type))
						return (T)(Boolean) BytesUtil.toBoolean(value, offset);
				else if(Byte.class.equals(type))
						return (T)(Byte) value[offset];
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

		@Override
		public void setWriteToWAL(LTuple put, boolean writeToWAL) {
			// no op
		}

		@Override
		public void addFamilyQualifierToDelete(LTuple delete, byte[] family,
				byte[] qualifier, long timestamp) {
	    	addKeyValueToTuple(delete, family, qualifier, timestamp, null);						
		}

		@Override
		public void addDataToDelete(LTuple delete, KeyValue data, long timestamp) {
		    	addKeyValueToTuple(delete, data.getFamily(), data.getQualifier(), timestamp, null);			
		}

		@Override
		public boolean singleMatchingColumn(KeyValue element, byte[] family,
				byte[] qualifier) {
			return KeyValueUtils.singleMatchingColumn(element, family, qualifier);
		}

		@Override
		public boolean singleMatchingFamily(KeyValue element, byte[] family) {
			return KeyValueUtils.singleMatchingFamily(element, family);
		}

		@Override
		public boolean singleMatchingQualifier(KeyValue element, byte[] qualifier) {
			return KeyValueUtils.singleMatchingQualifier(element, qualifier);
		}

		@Override
		public boolean matchingValue(KeyValue element, byte[] value) {
			return KeyValueUtils.matchingValue(element, value);
		}

		@Override
		public boolean matchingFamilyKeyValue(KeyValue element, KeyValue other) {
			return KeyValueUtils.matchingFamilyKeyValue(element, other);
		}

		@Override
		public boolean matchingQualifierKeyValue(KeyValue element, KeyValue other) {
			return KeyValueUtils.matchingQualifierKeyValue(element, other);
		}

		@Override
		public boolean matchingRowKeyValue(KeyValue element, KeyValue other) {
			return KeyValueUtils.matchingRowKeyValue(element, other);
		}

		@Override
		public KeyValue newValue(KeyValue element, byte[] value) {
			return KeyValueUtils.newKeyValue(element, value);
		}

		@Override
		public KeyValue newValue(byte[] rowKey, byte[] family, byte[] qualifier,
				Long timestamp, byte[] value) {
			return KeyValueUtils.newKeyValue(rowKey, family, qualifier, timestamp, value);
		}

		@Override
		public boolean isAntiTombstone(KeyValue element, byte[] antiTombstone) {		
			byte[] buffer = element.getBuffer();
			int valueOffset = element.getValueOffset();
			int valueLength = element.getValueLength();
			return Bytes.equals(antiTombstone,0,antiTombstone.length,buffer,valueOffset,valueLength);
		}

		@Override
		public Comparator getComparator() {
			return KeyValue.COMPARATOR;
		}

		@Override
		public long getTimestamp(KeyValue element) {
			return element.getTimestamp();
		}

		@Override
		public String getFamilyAsString(KeyValue element) {
			return Bytes.toString(element.getFamily());
		}

		@Override
		public String getQualifierAsString(KeyValue element) {
			return Bytes.toString(element.getQualifier());
		}

		@Override
		public void setRowInSlice(KeyValue element, ByteSlice slice) {
	        slice.set(element.getBuffer(),element.getRowOffset(),element.getRowLength());
		}

		@Override
		public boolean isFailedCommitTimestamp(KeyValue Element) {
	        return Element.getValueLength() == 1 && Element.getBuffer()[Element.getValueOffset()] == SIConstants.SNAPSHOT_ISOLATION_FAILED_TIMESTAMP[0];
		}

		@Override
		public KeyValue newTransactionTimeStampKeyValue(KeyValue element,
				byte[] value) {
	        return new KeyValue(element.getBuffer(),element.getRowOffset(),element.getRowLength(),SIConstants.DEFAULT_FAMILY_BYTES,0,1,SIConstants.SNAPSHOT_ISOLATION_COMMIT_TIMESTAMP_COLUMN_BYTES,0,1,element.getTimestamp(), KeyValue.Type.Put,value,0,value==null ? 0 : value.length);
		}

		@Override
		public long getValueLength(KeyValue element) {
			return element.getValueLength();
		}

		@Override
		public long getValueToLong(KeyValue element) {
			return Bytes.toLong(element.getBuffer(), element.getValueOffset(), element.getValueLength());
		}

		@Override
		public byte[] getDataFamily(KeyValue element) {
			return element.getFamily();
		}

		@Override
		public byte[] getDataQualifier(KeyValue element) {
			return element.getQualifier();
		}

		@Override
		public byte[] getDataValue(KeyValue element) {
			return element.getValue();
		}

		@Override
		public Result newResult(List<KeyValue> values) {
			return new Result(values);
		}

		@Override
		public KeyValue[] getDataFromResult(Result result) {
			return result.raw();
		}

		@Override
		public byte[] getDataRow(KeyValue element) {
			return element.getRow();
		}

		@Override
		public KeyValue getColumnLatest(Result result, byte[] family,
				byte[] qualifier) {
			return result.getColumnLatest(family, qualifier);
		}

		@Override
		public byte[] getDataValueBuffer(KeyValue element) {
			return element.getBuffer();
		}

		@Override
		public int getDataValueOffset(KeyValue element) {
			return element.getValueOffset();
		}

		@Override
		public int getDataValuelength(KeyValue element) {
			return element.getValueLength();
		}
		
		@Override
		public int getLength(KeyValue element) {
			return element.getLength();
		}

		@Override
		public byte[] getDataRowBuffer(KeyValue element) {
			return element.getBuffer();
		}

		@Override
		public int getDataRowOffset(KeyValue element) {
			return element.getRowOffset();
		}

		@Override
		public int getDataRowlength(KeyValue element) {
			return element.getRowLength();
		}

		@Override
		public boolean regionScannerNext(RegionScanner regionScanner,
				List<KeyValue> data) throws IOException {
			return regionScanner.next(data);
		}

		@Override
		public void setThreadReadPoint(RegionScanner delegate) {
			throw new RuntimeException("Not Implemented");			
		}

		@Override
		public boolean regionScannerNextRaw(RegionScanner regionScanner,
				List<KeyValue> data) throws IOException {
			throw new RuntimeException("Not Implemented");
		}

		@Override
		public RegionScanner getBufferedRegionScanner(HRegion region,
				RegionScanner delegate, LGet scan, int bufferSize,
				MetricFactory metricFactory) {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public Filter getActiveTransactionFilter(long beforeTs, long afterTs,
				byte[] destinationTable) {
			throw new RuntimeException("Not Implemented");
		}

		@Override
		public InternalScanner getCompactionScanner(InternalScanner scanner,
				SICompactionState state) {
			throw new RuntimeException("Not Implemented");
		}

		@Override
		public boolean internalScannerNext(InternalScanner internalScanner,
				List<KeyValue> data) throws IOException {
			throw new RuntimeException("Not Implemented");
		}
}
