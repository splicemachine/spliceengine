package com.splicemachine.si.data.hbase;

import com.google.common.collect.Iterables;
import com.splicemachine.constants.SIConstants;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.constants.bytes.BytesUtil;
import com.splicemachine.hbase.BufferedRegionScanner;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.hbase.KeyValueUtils;
import com.splicemachine.hbase.MeasuredRegionScanner;
import com.splicemachine.metrics.MetricFactory;
import com.splicemachine.si.coprocessors.SICompactionScanner;
import com.splicemachine.si.data.api.SDataLib;
import com.splicemachine.si.impl.SICompactionState;
import com.splicemachine.si.impl.region.ActiveTxnFilter;
import com.splicemachine.utils.ByteSlice;

import org.apache.hadoop.hbase.HServerLoad;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.MultiVersionConsistencyControl;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Implementation of SDataLib that is specific to the HBase operation and result types.
 */
public class HDataLib implements SDataLib<KeyValue,Put, Delete, Get, Scan> {

		@Override
    public byte[] newRowKey(Object... args) {
        List<byte[]> bytes = new ArrayList<byte[]>();
        for (Object a : args) {
            bytes.add(convertToBytes(a, a.getClass()));
        }
        return BytesUtil.concat(bytes);
    }

		@Override
    public byte[] getPutKey(Put put) {
        return put.getRow();
    }

		@Override
    public List<KeyValue> listResult(Result result) {
        return result.list();
    }

    @Override
    public Iterable<KeyValue> listPut(Put put) {
        return Iterables.concat(put.getFamilyMap().values());
    }

		@Override
    public byte[] encode(Object value) {
        return convertToBytes(value, value.getClass());
    }

    @SuppressWarnings("unchecked")
		@Override
    public <T> T decode(byte[] value, Class<T> type) {
        if (value == null) {
            return null;
        }
				if (type.equals(Boolean.class)) {
            return (T)(Boolean)Bytes.toBoolean(value);
        } else if (type.equals(Short.class)) {
            return (T)(Short)Bytes.toShort(value);
        } else if (type.equals(Integer.class)) {
						if(value.length<4) return (T)new Integer(-1);
            return (T)(Integer)Bytes.toInt(value);
        } else if (type.equals(Long.class)) {
            return (T)(Long)Bytes.toLong(value);
        } else if (type.equals(Byte.class)) {
            if (value.length > 0) {
                return (T)(Byte)value[0];
            } else {
                return null;
            }
        } else if (type.equals(String.class)) {
            return (T)Bytes.toString(value);
        }
        throw new RuntimeException("unsupported type conversion: " + type.getName());
    }

		@SuppressWarnings("unchecked")
		@Override
		public <T> T decode(byte[] value, int offset, int length,Class<T> type) {
				if (value == null) {
						return null;
				}
				if (type.equals(Boolean.class)) {
						return (T)(Boolean)BytesUtil.toBoolean(value,offset);
				} else if (type.equals(Short.class)) {
						return (T)(Short)Bytes.toShort(value,offset);
				} else if (type.equals(Integer.class)) {
						if(length<4) return (T)new Integer(-1);
						return (T)(Integer)Bytes.toInt(value,offset);
				} else if (type.equals(Long.class)) {
						return (T)(Long)Bytes.toLong(value,offset);
				} else if (type.equals(Byte.class)) {
						if (length > 0) {
								return (T)(Byte)value[offset];
						} else {
								return null;
						}
				} else if (type.equals(String.class)) {
						return (T)Bytes.toString(value,offset,length);
				}
				throw new RuntimeException("unsupported type conversion: " + type.getName());
		}

		@Override
    public void addKeyValueToPut(Put put, byte[] family, byte[] qualifier, long timestamp, byte[] value) {
        if (timestamp <0) {
            put.add(family, qualifier, value);
        } else {
            put.add(family, qualifier, timestamp, value);
        }
    }

		@Override
    public Put newPut(byte[] key) {
        return new Put(key);
    }

    @Override
    public Put newPut(byte[] key, Integer lock) {
        return new Put(key, lock);
    }

		@Override
    public Get newGet(byte[] rowKey, List<byte[]> families, List<List<byte[]>> columns, Long effectiveTimestamp) {
        Get get = new Get(rowKey);
        if (families != null) {
            for (byte[] f : families) {
                get.addFamily(f);
            }
        }
        if (columns != null) {
            for (List<byte[]> c : columns) {
                get.addColumn(c.get(0), c.get(1));
            }
        }
        if (effectiveTimestamp != null) {
            try {
                get.setTimeRange(effectiveTimestamp, Long.MAX_VALUE);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return get;
    }

    @Override
    public byte[] getGetRow(Get get) {
        return get.getRow();
    }

    @Override
    public void setGetTimeRange(Get get, long minTimestamp, long maxTimestamp) {
        try {
            get.setTimeRange(minTimestamp, maxTimestamp);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void setGetMaxVersions(Get get) {
        get.setMaxVersions();
    }

    @Override
    public void setGetMaxVersions(Get get, int max) {
        try {
            get.setMaxVersions(max);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void addFamilyToGet(Get get, byte[] family) {
        get.addFamily(family);
    }

    @Override
    public void addFamilyToGetIfNeeded(Get get, byte[] family) {
        if (get.hasFamilies()) {
            get.addFamily(family);
        }
    }

    @Override
    public Scan newScan(byte[] startRowKey, byte[] endRowKey, List<byte[]> families, List<List<byte[]>> columns, Long effectiveTimestamp) {
        Scan scan = new Scan();
        scan.setStartRow(startRowKey);
        scan.setStopRow(endRowKey);
        if (families != null) {
            for (byte[] f : families) {
                scan.addFamily(f);
            }
        }
        if (columns != null) {
            for (List<byte[]> c : columns) {
                scan.addColumn(c.get(0), c.get(1));
            }
        }
        if (effectiveTimestamp != null) {
            try {
                scan.setTimeRange(effectiveTimestamp, Long.MAX_VALUE);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return scan;
    }

    @Override
    public void setScanTimeRange(Scan scan, long minTimestamp, long maxTimestamp) {
        try {
            scan.setTimeRange(minTimestamp, maxTimestamp);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void setScanMaxVersions(Scan scan) {
        scan.setMaxVersions();
    }

		@Override
    public void addFamilyToScan(Scan scan, byte[] family) {
        scan.addFamily(family);
    }

    @Override
    public void addFamilyToScanIfNeeded(Scan scan, byte[] family) {
        if(scan.hasFamilies()) {
            scan.addFamily(family);
        }
    }

    @Override
    public Delete newDelete(byte[] rowKey) {
        return new Delete(rowKey);
    }

    static byte[] convertToBytes(Object value, Class clazz) {
        if (clazz == String.class) {
            return Bytes.toBytes((String) value);
        } else if (clazz == Integer.class) {
            return Bytes.toBytes((Integer) value);
        } else if (clazz == Short.class) {
            return Bytes.toBytes((Short) value);
        } else if (clazz == Long.class) {
            return Bytes.toBytes((Long) value);
        } else if (clazz == Boolean.class) {
            return Bytes.toBytes((Boolean) value);
        } else if (clazz == byte[].class) {
            return (byte[]) value;
        } else if (clazz == Byte.class) {
            return new byte[] {(Byte) value};
        }
        throw new RuntimeException("Unsupported class " + clazz.getName() + " for " + value);
    }

		@Override
		public KVPair toKVPair(Put put) {
				return new KVPair(put.getRow(),put.get(SpliceConstants.DEFAULT_FAMILY_BYTES,SIConstants.PACKED_COLUMN_BYTES).get(0).getValue());
		}

		@Override
		public Put toPut(KVPair kvPair, byte[] family, byte[] column, long longTransactionId) {
        Put put = kvPair.toPut(family, column, longTransactionId);
        return put;
		}

		@Override
		public Get newGet(byte[] rowKey, List<byte[]> families,List<List<byte[]>> columns, Long effectiveTimestamp,int maxVersions) {
			Get get = newGet(rowKey,families,columns,effectiveTimestamp);
			try {
				get.setMaxVersions(maxVersions);
			} catch (IOException e) {
				throw new RuntimeException(e + "Exception setting max versions");
			}
			return get;
		}

		@Override
		public void setWriteToWAL(Put put, boolean writeToWAL) {
			put.setWriteToWAL(writeToWAL);
		}

		@Override
		public void addFamilyQualifierToDelete(Delete delete, byte[] family,
				byte[] qualifier, long timestamp) {
			delete.deleteColumn(family, qualifier,timestamp);
		}

		@Override
		public void addDataToDelete(Delete delete, KeyValue data, long timestamp) {
			delete.deleteColumn(data.getFamily(), data.getQualifier(), timestamp);
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
		public Result newResult(List<KeyValue> element) {
			return new Result(element);
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
		public boolean regionScannerNextRaw(RegionScanner regionScanner,
				List<KeyValue> data) throws IOException {
			return regionScanner.nextRaw(data,null);
		}
		
		@Override
		public void setThreadReadPoint(RegionScanner delegate) {
			MultiVersionConsistencyControl.setThreadReadPoint(delegate.getMvccReadPoint());
		}

		@Override
		public MeasuredRegionScanner<KeyValue> getBufferedRegionScanner(HRegion region,
				RegionScanner delegate, Scan scan, int bufferSize,
				MetricFactory metricFactory) {
			return new BufferedRegionScanner(region,delegate,scan,bufferSize,metricFactory,this);
		}

		@Override
		public Filter getActiveTransactionFilter(long beforeTs, long afterTs,
				byte[] destinationTable) {
			return new ActiveTxnFilter(beforeTs,afterTs,destinationTable);
		}

		@Override
		public InternalScanner getCompactionScanner(InternalScanner scanner,
				SICompactionState state) {
			 return new SICompactionScanner(state,scanner,this);			
		}

		@Override
		public boolean internalScannerNext(InternalScanner internalScanner,
				List<KeyValue> data) throws IOException {
			return internalScanner.next(data);
		}

		@Override
		public boolean isDataInRange(KeyValue data, Pair<byte[], byte[]> range) {
			return BytesUtil.isKeyValueInRange(data, range);
		}

		@Override
		public byte[] getDataQualifierBuffer(KeyValue element) {
			return element.getBuffer();
		}

		@Override
		public int getDataQualifierOffset(KeyValue element) {
			return element.getQualifierOffset();
		}

		@Override
		public KeyValue matchKeyValue(Iterable<KeyValue> kvs,
				byte[] columnFamily, byte[] qualifier) {
			for(KeyValue kv:kvs){
				if(kv.matchingColumn(columnFamily,qualifier))
						return kv;
			}
			return null;
		}

		@Override
		public KeyValue matchKeyValue(KeyValue[] kvs, byte[] columnFamily,
				byte[] qualifier) {
			for(KeyValue kv:kvs){
				if(kv.matchingColumn(columnFamily,qualifier))
						return kv;
			}
			return null;
		}

		@Override
		public KeyValue matchDataColumn(KeyValue[] kvs) {
			return matchKeyValue(kvs, SpliceConstants.DEFAULT_FAMILY_BYTES,
					SpliceConstants.PACKED_COLUMN_BYTES);
		}

		@Override
		public KeyValue matchDataColumn(List<KeyValue> kvs) {
			return matchKeyValue(kvs, SpliceConstants.DEFAULT_FAMILY_BYTES,
					SpliceConstants.PACKED_COLUMN_BYTES);
		}

		@Override
		public KeyValue matchDataColumn(Result result) {
			return matchDataColumn(result.raw());
		}

		@Override
		public boolean matchingQualifier(KeyValue element, byte[] qualifier) {
			return element.matchingQualifier(qualifier);
		}
		
}