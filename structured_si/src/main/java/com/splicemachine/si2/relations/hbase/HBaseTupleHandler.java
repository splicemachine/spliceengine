package com.splicemachine.si2.relations.hbase;

import com.splicemachine.si2.relations.api.TupleGet;
import com.splicemachine.si2.relations.api.TupleHandler;
import com.splicemachine.si2.relations.api.TuplePut;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HBaseTupleHandler implements TupleHandler {

	static byte[] concat(List<byte[]> list) {
	    // copied from splice BytesUtil
			int length = 0;
		for (byte[] bytes : list) {
			length += bytes.length;
		}
		byte[] result = new byte[length];
		int pos = 0;
		for (byte[] bytes : list) {
			System.arraycopy(bytes, 0, result, pos, bytes.length);
			pos += bytes.length;
		}
		return result;
	}

	@Override
	public Object makeTupleKey(Object... args) {
		List<byte[]> bytes = new ArrayList<byte[]>();
		for (Object a : args) {
			bytes.add(HBaseTupleHandler.convertToBytes(a));
		}
		return HBaseTupleHandler.concat(bytes);
	}

	@Override
	public Object makeFamily(String familyIdentifier) {
		return Bytes.toBytes(familyIdentifier);
	}

	@Override
	public Object makeQualifier(Object qualifierIdentifier) {
		return HBaseTupleHandler.convertToBytes(qualifierIdentifier);
	}

	private boolean nullSafeComparison(Object o1, Object o2) {
		return (o1 == null && o2 == null) || ((o1 != null) && o1.equals(o2));
	}

	@Override
	public boolean familiesMatch(Object family1, Object family2) {
		return nullSafeComparison(family1, family2);
	}

	@Override
	public boolean qualifiersMatch(Object qualifier1, Object qualifier2) {
		return qualifier1.equals(qualifier2);
	}

	@Override
	public Object getKey(Object tuple) {
		return ((Result) tuple).getRow();
	}

	@Override
	public List getCellsForColumn(Object tuple, Object family, Object qualifier) {
		return ((Result) tuple).getColumn((byte[]) family, (byte[]) qualifier);
	}

	@Override
	public Object getLatestCellForColumn(Object tuple, Object family, Object qualifier) {
		return ((Result) tuple).getValue((byte[]) family, (byte[]) qualifier);
	}

	@Override
	public List getCells(Object tuple) {
		return ((Result) tuple).list();
	}

	@Override
	public Object getCellFamily(Object cell) {
		return ((KeyValue) cell).getFamily();
	}

	@Override
	public Object getCellQualifier(Object cell) {
		return ((KeyValue) cell).getQualifier();
	}

	@Override
	public Object getCellValue(Object cell) {
		return ((KeyValue) cell).getValue();
	}

	@Override
	public long getCellTimestamp(Object cell) {
		return ((KeyValue) cell).getTimestamp();
	}

	@Override
	public Object makeValue(Object value) {
		return convertToBytes(value);
	}

	@Override
	public Object fromValue(Object value, Class type) {
		if (type.equals(Boolean.class)) {
			return Bytes.toBoolean((byte[]) value);
		}
		throw new RuntimeException("unsupported type conversion: " + type.getName());
	}

	@Override
	public void addCellToTuple(Object tuple, Object family, Object qualifier, Long timestamp, Object value) {
		final Put put = ((HBaseTuplePut) tuple).put;
		put.add((byte[]) family, (byte[]) qualifier, timestamp, (byte[]) value);
	}

	@Override
	public void addAttributeToTuple(Object tuple, String attributeName, Object value) {
		if (tuple instanceof HBaseTuplePut) {
			((HBaseTuplePut) tuple).put.setAttribute(attributeName, (byte[]) value);
		} else {
			throw new RuntimeException("unsupported type for attribute "  + tuple.getClass().getName());
		}
	}

	@Override
	public Object getAttribute(Object tuple, String attributeName) {
		if (tuple instanceof HBaseTuplePut) {
			return ((HBaseTuplePut) tuple).put.getAttribute(attributeName);
		}
		throw new RuntimeException("unsupported type for attribute "  + tuple.getClass().getName());
	}

	@Override
	public TuplePut makeTuplePut(Object key, List cells) {
		if (cells == null) {
			cells = new ArrayList();
		}
		try {
			Put put = new Put((byte[]) key);
			for (Object o : cells) {
				put.add((KeyValue) o);
			}
			return new HBaseTuplePut(put);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public TupleGet makeTupleGet(Object startTupleKey, Object endTupleKey, List<Object> families, List<List<Object>> columns, Long effectiveTimestamp) {
		try {
			if (startTupleKey.equals(endTupleKey)) {
				return new HBaseGetTupleGet( getSingleRow((byte[]) startTupleKey, families, columns, effectiveTimestamp) );
			}
			return new HBaseScanTupleGet( getManyRows((byte[]) startTupleKey, (byte[]) endTupleKey, families, columns, effectiveTimestamp) );
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private Get getSingleRow(byte[] startTupleKey, List<Object> families, List<List<Object>> columns,
								  Long effectiveTimestamp)
			throws IOException {
		Get get = new Get(startTupleKey);
		if (families != null) {
			for (Object f : families) {
				get.addFamily((byte[]) f);
			}
		}
		if (columns != null) {
			for (List c : columns) {
				get.addColumn((byte[]) c.get(0), (byte[]) c.get(1));
			}
		}
		if (effectiveTimestamp != null) {
			get.setTimeRange(effectiveTimestamp, Long.MAX_VALUE);
		}
		return get;
	}

	private Scan getManyRows(byte[] startTupleKey, byte[] endTupleKey, List<Object> families,
								 List<List<Object>> columns, Long effectiveTimestamp)
			throws IOException {
		Scan scan = new Scan();
		scan.setStartRow(startTupleKey);
		scan.setStopRow(endTupleKey);
		for (Object f : families) {
			scan.addFamily((byte[]) f);
		}
		for (List c : columns) {
			scan.addColumn((byte[]) c.get(0), (byte[]) c.get(1));
		}
		if (effectiveTimestamp != null) {
			scan.setTimeRange(effectiveTimestamp, Long.MAX_VALUE);
		}
		return scan;
	}

	static byte[] convertToBytes(Object qualifierIdentifier) {
		if (qualifierIdentifier instanceof String) {
			return Bytes.toBytes((String) qualifierIdentifier);
		} else if (qualifierIdentifier instanceof Integer) {
			return Bytes.toBytes((Integer) qualifierIdentifier);
		}
		throw new RuntimeException("Unsupported qualifier identifier class " + qualifierIdentifier.getClass().getName() + " for " + qualifierIdentifier);
	}
}
