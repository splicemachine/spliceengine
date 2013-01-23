package com.ir.hbase.client.structured;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.gson.Gson;
import com.ir.constants.HBaseConstants;

public class Column implements Comparable<Column> {
	protected String columnName;
	protected String familyName = HBaseConstants.DEFAULT_FAMILY;
	protected String description;
	protected boolean nullable;
	private static Gson gson = new Gson();
	public enum Type { 
		STRING, 
		INTEGER,
		BYTE,
		DATE,
		CALENDAR,
		FLOAT,
		LONG,
		DOUBLE,
		BOOLEAN,
		BIGINT,
		NOT_SUPPORTED,
	}
	public Column() {}
	
	public Column(String columnName) {
		this.columnName = columnName;
	}
	
	public Column(String family, String columnName) {
		this.columnName = columnName;
		this.familyName = family;
	}
	
	public Column(String family, String columnName, Type type) {
		this.columnName = columnName;
		this.familyName = family;
		this.type = type;
	}
	
	public String getFullColumnName() {
		return familyName + HBaseConstants.FAMILY_SEPARATOR + columnName;
	}
	
	public static Column toColumn(String json) {
		return gson.fromJson(json, Column.class);
	}
	public String toJSon() {
		return gson.toJson(this);
	}
	
	protected Type type;
	
	public String getColumnName() {
		return columnName;
	}
	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}
	public String getDescription() {
		return description;
	}
	public void setDescription(String description) {
		this.description = description;
	}
	public boolean isNullable() {
		return nullable;
	}
	public void setNullable(boolean nullable) {
		this.nullable = nullable;
	}
	public Type getType() {
		return type;
	}
	public void setType(Type type) {
		this.type = type;
	}		
	public String getFamily() {
		return familyName;
	}
	public void setFamily(String familyName) {
		this.familyName = familyName;
	}	
	@Override
	public boolean equals(Object obj) {
		if (obj instanceof Column) {
			return this.familyName.equals(((Column) obj).familyName) && this.columnName.equals(((Column) obj).columnName);
		}
		return false;
	}

	@Override
	public int compareTo(Column column) {
		if (this.familyName.equals(column.familyName)) {
			if (this.columnName.equals(column.columnName)) {
				return 0;
			} else {
				return Bytes.compareTo(Bytes.toBytes(this.columnName), Bytes.toBytes(column.columnName));
			}
		} else {
			return Bytes.compareTo(Bytes.toBytes(this.familyName), Bytes.toBytes(column.familyName));
		}
	}
	//return family:column bytes
	public byte[] makeColumn() {
		return KeyValue.makeColumn(Bytes.toBytes(this.familyName), Bytes.toBytes(this.columnName));
	}
}
