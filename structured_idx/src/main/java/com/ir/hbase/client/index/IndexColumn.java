package com.ir.hbase.client.index;

import java.util.Arrays;

import com.ir.constants.bytes.SortableByteUtil;
import com.ir.hbase.client.structured.Column;
import com.ir.hbase.coprocessor.index.IndexUtils;

public class IndexColumn extends Column {
	public enum Order { ASCENDING, DESCENDING }
	private Order order = Order.ASCENDING;
    private boolean caseSensitive = true;	
	public IndexColumn() {
		super();
	}
	public IndexColumn(String columnName, Order order, Type type) {
		this.columnName = columnName;
		this.order = order;
		this.type = type;
	}
	public IndexColumn(String familyName, String columnName, Order order, Type type) {
		this.columnName = columnName;
		this.familyName = familyName;
		this.order = order;
		this.type = type;
	}
	public Order getOrder() {
		return order;
	}
	public void setOrder(Order order) {
		this.order = order;
	}
	public boolean isCaseSensitive() {
		return caseSensitive;
	}
	public void setCaseSensitive(boolean caseSensitive) {
		this.caseSensitive = caseSensitive;
	}
	//Change a value to byte array based on the rule defined by this index column
	public byte[] toBytes(byte[] data, boolean autoInvert) {
		byte[] sortable =  IndexUtils.toSortable(Arrays.copyOfRange(data, 0, data.length), this.type, true);
		if (autoInvert && this.order == Order.DESCENDING) {
			return SortableByteUtil.invertBits(sortable, 0, sortable.length);
		} else {
			return sortable;
		}
	}
}
