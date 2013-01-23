package com.ir.hbase.client.index;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.mortbay.log.Log;

import com.google.gson.Gson;
import com.ir.constants.SchemaConstants;
import com.ir.constants.bytes.SortableByteUtil;
import com.ir.hbase.client.structured.Column;
import com.ir.hbase.coprocessor.index.IndexRegionObserver;
import com.ir.hbase.coprocessor.index.IndexUtils;

public class Index {
	private static Logger LOG = Logger.getLogger(Index.class);

	private List<IndexColumn> indexColumns = new ArrayList<IndexColumn>();
	private List<Column> additionalColumns = new ArrayList<Column>();

	private static Gson gson = new Gson();
	private String indexName;
	
	public Index(String indexName) {
		this.indexName = indexName;
	}
	public String getIndexName() {
		return indexName;
	}
	public void setIndexColumns(List<IndexColumn> indexColumns) {
		this.indexColumns = indexColumns;
	}
	public List<IndexColumn> getIndexColumns() {
		return indexColumns;
	}
	public List<Column> getAddColumns() {
		return additionalColumns;
	}
	public void setAddColumns(List<Column> additionalColumns) {
		this.additionalColumns = additionalColumns;
	}
	public boolean containsColumn(Column column) {
		return indexColumns.contains(column) || additionalColumns.contains(column);
	}
	public void addIndexColumn(IndexColumn icolumn) {
		this.indexColumns.add(icolumn);
	}
	public void addAdditionalCol(Column column) {
		this.additionalColumns.add(column);
	}
	public IndexColumn getIndexColumn(String family, String column) {
		for (IndexColumn icol : indexColumns) {
			if (icol.getFamily().equals(family) && icol.getColumnName().equals(column))
				return icol;
		}
		throw new RuntimeException("No such Index Column exist.");
	}
	public static Index toIndex(String json) {
		return gson.fromJson(json, Index.class);
	}
	public String toJSon() {
		return gson.toJson(this);
	}
	public List<String> getAllFamilies() {
		List<String> allFamilies = new ArrayList<String>();
		for (Column col : getAllColumns()) {
			if (!allFamilies.contains(col.getFamily())) {
				allFamilies.add(col.getFamily());
			}
		}
		return allFamilies;
	}
	public List<Column> getAllColumns() {
		List<Column> allColumns = new ArrayList<Column>();
		allColumns.addAll(indexColumns);
		allColumns.addAll(additionalColumns);
		return allColumns;
	}
	public byte[] createIndexKey(byte[] row, Map<byte[], byte[]> baseRowData) {
		if (LOG.isDebugEnabled())
			LOG.debug("Create index key for " + indexName);
		List<byte[]> keyComponents = new ArrayList<byte[]>();
		keyComponents.add(Bytes.toBytes(indexName));
		keyComponents.add(SchemaConstants.INDEX_DELIMITER);
		for (IndexColumn column : indexColumns) {
			byte[] columnName = column.makeColumn();
			if (baseRowData.containsKey(columnName)) {
				if (LOG.isDebugEnabled())
					LOG.debug("Index column " + column.getColumnName() + " order " + column.getOrder().toString() + " has data " + Bytes.toString(baseRowData.get(column.makeColumn())));
				byte[] temp = baseRowData.get(column.makeColumn());
				keyComponents.add(dataToBytes(column, Arrays.copyOfRange(temp, 0, temp.length)));
				keyComponents.add(SchemaConstants.INDEX_DELIMITER);	
			} else {
				if (LOG.isDebugEnabled())
					LOG.debug("Index column " + column.getColumnName() + " order " + column.getOrder().toString() + " doesn't have data ");
				//Base row data doesn't contain column 
				keyComponents.add(dataToBytes(column, null));
				keyComponents.add(SchemaConstants.INDEX_DELIMITER);
				/*throw new RuntimeException("Base row data doesn't contain column " + column.getFullColumnName());*/
			}
		}
		int keyLength = 0;
		for (byte[] bytes : keyComponents) {
			keyLength += bytes.length;
		}
		byte[] encodedIdentifier = null;
		if (row != null) {
			encodedIdentifier = identifierEncode(row);
			keyLength += encodedIdentifier.length;
		}
		byte[] indexKey = new byte[keyLength];
		int pos = 0;
		for (byte[] bytes : keyComponents) {
			System.arraycopy(bytes, 0, indexKey, pos, bytes.length);
			pos += bytes.length;
		}
		if (encodedIdentifier != null) {
			System.arraycopy(encodedIdentifier, 0, indexKey, pos, encodedIdentifier.length);
		}
		return indexKey;
	}
	public byte[] createIndexScanKey(Map<String, Object> scanColumns) {
		List<byte[]> keyComponents = new ArrayList<byte[]>();
		keyComponents.add(Bytes.toBytes(indexName));
		keyComponents.add(SchemaConstants.INDEX_DELIMITER);
		for (IndexColumn column : indexColumns) {
			String columnFullName = column.getFullColumnName();
			if (scanColumns.containsKey(columnFullName) && scanColumns.get(columnFullName) != null) {
				keyComponents.add(dataToBytes(column, scanColumns.get(columnFullName)));
				keyComponents.add(SchemaConstants.INDEX_DELIMITER);	
			} else {
				break;
			}
		}
		int keyLength = 0;
		for (byte[] bytes : keyComponents) {
			keyLength += bytes.length;
		}
		byte[] indexKey = new byte[keyLength];
		int pos = 0;
		for (byte[] bytes : keyComponents) {
			System.arraycopy(bytes, 0, indexKey, pos, bytes.length);
			pos += bytes.length;
		}
		return indexKey;
	}
	
	public static byte[] dataToBytes(IndexColumn column, Object data) {
		return dataToBytes(column, data, true, true);
	}

	public static byte[] dataToBytes(IndexColumn column, Object data, boolean autoInvert, boolean includeEof) {
		byte[] dataAsBytes;
		if (data != null) {
			if (!(data instanceof byte[])) {
				if (LOG.isDebugEnabled())
					LOG.debug("Change " + column.getType().toString() + " object " + data + " to sortable bytes, include Eof " + includeEof);
				dataAsBytes = IndexUtils.toSortableBytesFromObject(data, column.getType(), includeEof);
			} else {
				dataAsBytes = IndexUtils.toSortable(Arrays.copyOfRange((byte[]) data, 0, ((byte[]) data).length), column.getType(), includeEof);
			}
		} else {
			dataAsBytes = new byte[0];
		}

		int totalLength = SchemaConstants.FIELD_FLAGS_SIZE + dataAsBytes.length;

		byte[] bytes = new byte[totalLength];
		if (data == null) {
			bytes[0] = setNullFlag((byte)0);
		}

		System.arraycopy(dataAsBytes, 0, bytes, 1, dataAsBytes.length);

		if (autoInvert && column.getOrder() == IndexColumn.Order.DESCENDING) {
			SortableByteUtil.invertBits(bytes, 0, bytes.length);
		}
		
		return bytes;
	}

	public static byte setNullFlag(byte flags) {
		return (byte)(flags | 0x01);
	}

	public static byte[] identifierEncode(byte[] bytes) {
		byte[] result = new byte[bytes.length + Bytes.SIZEOF_INT];
		System.arraycopy(bytes, 0, result, 0, bytes.length);
		Bytes.putInt(result, bytes.length, bytes.length);
		return result;
	}

	public static byte[] identifierDecode(byte[] bytes, boolean inverted) {
		int keyLength = Bytes.toInt(bytes, bytes.length - Bytes.SIZEOF_INT);
		byte[] result = new byte[keyLength];
		System.arraycopy(bytes, bytes.length - keyLength - Bytes.SIZEOF_INT, result, 0, keyLength);
		return result;
	}
}