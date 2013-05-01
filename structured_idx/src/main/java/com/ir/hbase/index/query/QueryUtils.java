package com.ir.hbase.index.query;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import com.ir.constants.SpliceConstants;
import com.ir.constants.bytes.BytesUtil;
import com.ir.hbase.client.index.Index;
import com.ir.hbase.client.index.IndexColumn;

public class QueryUtils {
	private static Logger LOG = Logger.getLogger(QueryUtils.class);
	public static byte[] generateIndexKeyForQuery(Index index, Map<String,ScannerKeyElement> scanColumns, boolean endKey) {
		if (LOG.isTraceEnabled()) {
			LOG.trace("Generating index Key for Query using " + index.getIndexName());
		}
		
		List<byte[]> keyComponents = new ArrayList<byte[]>();
		if (scanColumns.size() > 0) {
			keyComponents.add(index.getIndexName().getBytes());
			keyComponents.add(SpliceConstants.INDEX_DELIMITER);
			for (IndexColumn column : index.getIndexColumns()) {
				String columnFullName = column.getFullColumnName();
				if (scanColumns.containsKey(columnFullName)) {
					Object value = scanColumns.get(columnFullName).getObject();
					byte[] bytes = Index.dataToBytes(column, value, true, scanColumns.get(columnFullName).getIncludeEof());
					if (LOG.isTraceEnabled()) {
						LOG.trace("After change to stortable " + Bytes.toString(bytes) + " into field " + columnFullName);
					}
					if (scanColumns.get(columnFullName).getIncrement()) {
						if (LOG.isTraceEnabled()) {
							LOG.trace("Incrementing value " + value + " for field " + columnFullName);
						}
						BytesUtil.incrementAtIndex(bytes, bytes.length - 1);
						keyComponents.add(bytes);
					} else {
						if (LOG.isTraceEnabled()) {
							LOG.trace("Not Incrementing value " + value + " for field " + columnFullName);
						}
						keyComponents.add(bytes);
//						keyComponents.add(SpliceConstants.INDEX_DELIMITER);	
					}
				} else {
					break;
				}
			}
		} else {
			if (endKey) { // Have to capture the end key when index is used for sort only - JL
				byte[] bytes = Bytes.toBytes(index.getIndexName());
				BytesUtil.incrementAtIndex(bytes, bytes.length - 1);
				keyComponents.add(bytes);
						
			} else {
				keyComponents.add(Bytes.toBytes(index.getIndexName()));				
				keyComponents.add(SpliceConstants.INDEX_DELIMITER);				
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
	
}
