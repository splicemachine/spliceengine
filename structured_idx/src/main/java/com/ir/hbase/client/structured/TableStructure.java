package com.ir.hbase.client.structured;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.gson.Gson;
import com.ir.constants.SpliceConstants;
import com.ir.constants.SpliceConstants;

public class TableStructure extends Object {
	private Map<String, Family> families = new HashMap<String, Family>();
	private static Gson gson = new Gson();
	private String tableName;

	public enum TableType {
		BASE,
		SYSTEM
	}
	public TableStructure() {
	}
	public TableStructure(String tableName) {
		this.tableName = tableName;
	}
	public String getTableName() {
		return tableName;
	}
	public TableType tableType = TableType.BASE;

	public TableType getTableType() {
		return tableType;
	}
	public void setTableType(TableType tableType) {
		this.tableType = tableType;
	}
	public String toJSon() {
		return gson.toJson(this);
	}
	public static TableStructure fromJSON(String json) {
		return gson.fromJson(json, TableStructure.class);
	}
	public boolean hasFamily(String familyName) {
		return families.containsKey(familyName);
	}
	public boolean hasFamilies() {
		return !families.isEmpty();
	}
	public Family getFamily(String familyName) {
		return families.get(familyName);
	}
	public Collection<Family> getFamilies() {
		return families.values();
	}
	public Set<String> getAllFamilyNames() {
		return families.keySet();
	}
	public void addFamily(Family family) {
		this.families.put(family.getFamilyName(), family);
	}
	
	public Column getColumn(String columnName) {
		for (Family family: this.families.values()) {
			for (Column column: family.getColumns()) {
				if (column.getColumnName().equals(columnName))
					return column;
			}
		}
		return null;
	}
	
	public static HTableDescriptor setTableStructure(HTableDescriptor desc, TableStructure tableStructure) {
		desc.setValue(SpliceConstants.TABLE_STRUCTURE, gson.toJson(tableStructure));
		return desc;
	}
	
	public static HTableDescriptor generateDefaultStructuredData(String tableName) {
		return generateDefaultStructuredData(null, tableName);
	} 

	public static HTableDescriptor generateDefaultStructuredData(TableStructure tableStructure, String tableName) {
		HTableDescriptor desc = new HTableDescriptor(tableName);
		desc.addFamily(new HColumnDescriptor(SpliceConstants.DEFAULT_FAMILY.getBytes(),
				SpliceConstants.DEFAULT_VERSIONS,
				SpliceConstants.DEFAULT_COMPRESSION,
				SpliceConstants.DEFAULT_IN_MEMORY,
				SpliceConstants.DEFAULT_BLOCKCACHE,
				SpliceConstants.DEFAULT_TTL,
				SpliceConstants.DEFAULT_BLOOMFILTER));
		return tableStructure == null ? desc : setTableStructure(desc, tableStructure);		
	}
	
	public static TableStructure getTableStructure(HTableDescriptor desc) {
		return gson.fromJson(desc.getValue(SpliceConstants.TABLE_STRUCTURE),TableStructure.class);
	}
	
	public static void removeTableStructure(HTableDescriptor desc) {
		desc.remove(Bytes.toBytes(SpliceConstants.TABLE_STRUCTURE));
	}
}
