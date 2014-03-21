package com.splicemachine.si.data.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.carrotsearch.hppc.IntObjectOpenHashMap;
import com.carrotsearch.hppc.cursors.IntObjectCursor;
import com.google.common.collect.Iterables;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import com.splicemachine.constants.SIConstants;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.constants.bytes.BytesUtil;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.si.data.api.SDataLib;
import com.splicemachine.si.data.api.SRowLock;

/**
 * Implementation of SDataLib that is specific to the HBase operation and result types.
 */
public class HDataLib implements SDataLib<Mutation, Put, Delete, Get, Scan> {

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
    public Put newPut(byte[] key, SRowLock lock) {
        // TODO: jc - interface imbalance
        return new Put(key);
    }

    @Override
    public Mutation[] toMutationArray(IntObjectOpenHashMap<Mutation> mutations) {
				Mutation[] mutes = new Mutation[mutations.size()];
				int i=0;
				for(IntObjectCursor<Mutation> mutation:mutations){
						mutes[i] = mutation.value;
						i++;
				}
				return mutes;
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

    @Override
    public void addKeyValueToDelete(Delete delete, byte[] family, byte[] qualifier, long timestamp) {
        delete.deleteColumn(family, qualifier, timestamp);
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
				return new KVPair(put.getRow(), CellUtil.cloneValue(put.get(SpliceConstants.DEFAULT_FAMILY_BYTES,SIConstants.PACKED_COLUMN_BYTES).get(0)));
		}

		@Override
		public Put toPut(KVPair kvPair, byte[] family, byte[] column, long longTransactionId) {
				return kvPair.toPut(family,column,longTransactionId);
		}

		@Override
		public Get newGet(byte[] rowKey, List<byte[]> families,List<List<byte[]>> columns, Long effectiveTimestamp,int maxVersions) {
			Get get = newGet(rowKey,families,columns,effectiveTimestamp);
			try {
				get.setMaxVersions(1);
			} catch (IOException e) {
				throw new RuntimeException(e + "Exception setting max versions");
			}
			return get;
		}
}
