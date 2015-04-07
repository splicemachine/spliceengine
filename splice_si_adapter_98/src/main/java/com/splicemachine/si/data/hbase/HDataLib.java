package com.splicemachine.si.data.hbase;

import com.google.common.collect.Iterables;
import com.splicemachine.constants.SIConstants;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.constants.bytes.BytesUtil;
import com.splicemachine.hbase.*;
import com.splicemachine.metrics.MetricFactory;
import com.splicemachine.si.coprocessors.SICompactionScanner;
import com.splicemachine.si.data.api.SDataLib;
import com.splicemachine.si.impl.SICompactionState;
import com.splicemachine.si.impl.region.ActiveTxnFilter;
import com.splicemachine.utils.ByteSlice;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Implementation of SDataLib that is specific to the HBase operation and result types.
 */
public class HDataLib implements SDataLib<Cell,Put, Delete, Get, Scan> {
    private static final String ISOLATION_LEVEL = "_isolationlevel_";
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
    public List<Cell> listResult(Result result) {
        return result.listCells();
    }

    @Override
    public Iterable<Cell> listPut(Put put) {
        return Iterables.concat(put.getFamilyCellMap().values());
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
		public Put newPut(ByteSlice key) {
				return new Put(key.array(),key.offset(),key.length());
		}

		@Override
    public Put newPut(byte[] key, Integer lock) {
        return new Put(key, lock);
    }

		@Override
    public Get newGet(byte[] rowKey, List<byte[]> families, List<List<byte[]>> columns, Long effectiveTimestamp) {
        Get get = new Get(rowKey);
        get.setAttribute(ISOLATION_LEVEL,IsolationLevel.READ_UNCOMMITTED.toBytes());
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
        scan.setIsolationLevel(IsolationLevel.READ_UNCOMMITTED);
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
				return new KVPair(put.getRow(),
								put.get(SpliceConstants.DEFAULT_FAMILY_BYTES,SIConstants.PACKED_COLUMN_BYTES).get(0).getValue());
		}

		@Override
		public Put toPut(KVPair kvPair, byte[] family, byte[] column, long longTransactionId) {
				ByteSlice rowKey = kvPair.rowKeySlice();
				ByteSlice val = kvPair.valueSlice();
				Put put = newPut(rowKey);
				KeyValue kv = new KeyValue(rowKey.array(),rowKey.offset(),rowKey.length(),
								family,0,family.length,
								column,0,column.length,
								longTransactionId,
								KeyValue.Type.Put,val.array(),val.offset(),val.length());
				try {
						put.add(kv);
				} catch (IOException ignored) {
						/*
						 * This exception only appears to occur if the row in the Cell does not match
						 * the row that's set in the Put. This is definitionally not the case for the above
						 * code block, so we shouldn't have to worry about this error. As a result, throwing
						 * a RuntimeException here is legitimate
						 */
						throw new RuntimeException(ignored);
				}
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
		public void addDataToDelete(Delete delete, Cell data, long timestamp) {
			delete.deleteColumn(CellUtil.cloneFamily(data), CellUtil.cloneQualifier(data), timestamp);
		}
		@Override
		public boolean singleMatchingColumn(Cell element, byte[] family,
				byte[] qualifier) {
			return CellUtils.singleMatchingColumn(element, family, qualifier);
		}

		@Override
		public boolean singleMatchingFamily(Cell element, byte[] family) {
			return CellUtils.singleMatchingFamily(element, family);
		}

		@Override
		public boolean singleMatchingQualifier(Cell element, byte[] qualifier) {
			return CellUtils.singleMatchingQualifier(element, qualifier);
		}

		@Override
		public boolean matchingValue(Cell element, byte[] value) {
			return CellUtils.matchingValue(element, value);
		}

		@Override
		public boolean matchingFamilyKeyValue(Cell element, Cell other) {
			return CellUtils.matchingFamilyKeyValue(element, other);
		}

		@Override
		public boolean matchingQualifierKeyValue(Cell element, Cell other) {
			return CellUtils.matchingQualifierKeyValue(element, other);
		}

		@Override
		public boolean matchingRowKeyValue(Cell element, Cell other) {
			return CellUtils.matchingRowKeyValue(element, other);
		}

		@Override
		public Cell newValue(Cell element, byte[] value) {
			return CellUtils.newKeyValue(element, value);
		}

		@Override
		public Cell newValue(byte[] rowKey, byte[] family, byte[] qualifier,
				Long timestamp, byte[] value) {
			return CellUtils.newKeyValue(rowKey, family, qualifier, timestamp, value);
		}

		@Override
		public boolean isAntiTombstone(Cell element, byte[] antiTombstone) {		
			byte[] buffer = element.getValueArray();
			int valueOffset = element.getValueOffset();
			int valueLength = element.getValueLength();
			return Bytes.equals(antiTombstone,0,antiTombstone.length,buffer,valueOffset,valueLength);
		}

		@Override
		public Comparator getComparator() {
			return KeyValue.COMPARATOR;
		}

		@Override
		public long getTimestamp(Cell element) {
			return element.getTimestamp();
		}

		@Override
		public String getFamilyAsString(Cell element) {
			return Bytes.toString(CellUtil.cloneFamily(element));
		}

		@Override
		public String getQualifierAsString(Cell element) {
			return Bytes.toString(CellUtil.cloneQualifier(element));
		}

		@Override
		public void setRowInSlice(Cell element, ByteSlice slice) {
	        slice.set(element.getRowArray(),element.getRowOffset(),element.getRowLength());
		}

		@Override
		public boolean isFailedCommitTimestamp(Cell element) {
	        return element.getValueLength() == 1 && element.getValueArray()[element.getValueOffset()] == SIConstants.SNAPSHOT_ISOLATION_FAILED_TIMESTAMP[0];
		}

		@Override
		public Cell newTransactionTimeStampKeyValue(Cell element, byte[] value) {
	        return new KeyValue(element.getRowArray(),element.getRowOffset(),element.getRowLength(),SIConstants.DEFAULT_FAMILY_BYTES,0,1,SIConstants.SNAPSHOT_ISOLATION_COMMIT_TIMESTAMP_COLUMN_BYTES,0,1,element.getTimestamp(), KeyValue.Type.Put,value,0,value==null ? 0 : value.length);
		}

		@Override
		public long getValueLength(Cell element) {
			return element.getValueLength();
		}

		@Override
		public long getValueToLong(Cell element) {
			return Bytes.toLong(element.getValueArray(), element.getValueOffset(), element.getValueLength());
		}

		@Override
		public byte[] getDataFamily(Cell element) {
			return CellUtil.cloneFamily(element);
		}

		@Override
		public byte[] getDataQualifier(Cell element) {
			return CellUtil.cloneQualifier(element);
		}

		@Override
		public byte[] getDataValue(Cell element) {
			return CellUtil.cloneValue(element);
		}

		@Override
		public Result newResult(List<Cell> element) {
			return Result.create(element);
		}

		@Override
		public Cell[] getDataFromResult(Result result) {
			return result.rawCells();
		}

		@Override
		public byte[] getDataRow(Cell element) {
			return CellUtil.cloneRow(element);
		}

		@Override
		public Cell getColumnLatest(Result result, byte[] family,
				byte[] qualifier) {
			return result.getColumnLatestCell(family, qualifier);
		}

		@Override
		public byte[] getDataValueBuffer(Cell element) {
			return element.getValueArray();
		}

		@Override
		public int getDataValueOffset(Cell element) {
			return element.getValueOffset();
		}

		@Override
		public int getDataValuelength(Cell element) {
			return element.getValueLength();
		}

		@Override
		public int getLength(Cell element) {
			return element.getQualifierLength()+element.getFamilyLength()+element.getRowLength()+element.getValueLength() + element.getTagsLength();
		}

		@Override
		public byte[] getDataRowBuffer(Cell element) {
			return element.getRowArray();
		}

		@Override
		public int getDataRowOffset(Cell element) {
			return element.getRowOffset();
		}

		@Override
		public int getDataRowlength(Cell element) {
			return element.getRowLength();
		}

		@Override
		public boolean regionScannerNext(RegionScanner regionScanner,
				List<Cell> data) throws IOException {
			return regionScanner.next(data);
		}

		@Override
		public boolean regionScannerNextRaw(RegionScanner regionScanner,
				List<Cell> data) throws IOException {
			return regionScanner.nextRaw(data);
		}
		
		@Override
		public void setThreadReadPoint(RegionScanner delegate) {
			ThreadLocal<Long> perThreadReadPoint = new ThreadLocal<Long>(){
                @Override protected Long initialValue(){
                return Long.MAX_VALUE;
                }
            };
		}

		@Override
		public MeasuredRegionScanner<Cell> getBufferedRegionScanner(HRegion region,
				RegionScanner delegate, Scan scan, int bufferSize,
				MetricFactory metricFactory) {
			return new BufferedRegionScanner(region,delegate,scan,bufferSize,metricFactory,this);
		}

		@Override
		public MeasuredRegionScanner<Cell> getRateLimitedRegionScanner(HRegion region,
																																	 RegionScanner delegate,
																																	 Scan scan,
																																	 int bufferSize,
																																	 int readsPerSecond,
																																	 MetricFactory metricFactory) {
				MeasuredRegionScanner<Cell> d = getBufferedRegionScanner(region,delegate,scan,bufferSize,metricFactory);
				return new RateLimitedRegionScanner(d,readsPerSecond);
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
				List<Cell> data) throws IOException {
			return internalScanner.next(data);
		}

		@Override
		public boolean isDataInRange(Cell data, Pair<byte[], byte[]> range) {
			return CellUtils.isKeyValueInRange(data, range);
		}

		@Override
		public byte[] getDataQualifierBuffer(Cell element) {
			return element.getQualifierArray();
		}

		@Override
		public int getDataQualifierOffset(Cell element) {
			return element.getQualifierOffset();
		}

		@Override
		public Cell matchKeyValue(Iterable<Cell> kvs, byte[] columnFamily,
				byte[] qualifier) {
			for(Cell kv:kvs){
				if(CellUtils.matchingColumn(kv,columnFamily,qualifier))
						return kv;
			}
			return null;
		}

		@Override
		public Cell matchKeyValue(Cell[] kvs, byte[] columnFamily,
				byte[] qualifier) {
			int size = kvs != null?kvs.length:0;
			for (int i = 0; i<size; i++) {
				Cell kv = kvs[i];
				if(CellUtils.matchingColumn(kv,columnFamily,qualifier))
					return kv;
			}
			return null;
		}

		@Override
		public Cell matchDataColumn(Cell[] kvs) {
			return matchKeyValue(kvs, SpliceConstants.DEFAULT_FAMILY_BYTES,
					SpliceConstants.PACKED_COLUMN_BYTES);
		}

		@Override
		public Cell matchDataColumn(List<Cell> kvs) {			
			int size = kvs!=null?kvs.size():0;
			for (int i = 0; i<size;i++) {
				Cell kv = kvs.get(i);
				if(CellUtils.matchingColumn(kv,SpliceConstants.DEFAULT_FAMILY_BYTES,
						SpliceConstants.PACKED_COLUMN_BYTES))
						return kv;
			}
			return null;
		}

		@Override
		public Cell matchDataColumn(Result result) {
			return matchDataColumn(result.rawCells());
		}

		@Override
		public boolean matchingQualifier(Cell element, byte[] qualifier) {
			return CellUtil.matchingQualifier(element, qualifier);
		}
}
