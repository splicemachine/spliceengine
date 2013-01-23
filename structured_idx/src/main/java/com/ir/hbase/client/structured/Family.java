package com.ir.hbase.client.structured;

import java.util.ArrayList;
import java.util.List;

public class Family {
	private String familyName; 
	public Family(String familyName) {
		this.familyName = familyName;
	}
	private List<Column> columns = new ArrayList<Column>();
	public List<Column> getColumns() {
		return columns;
	}
	public void setColumns(List<Column> columns) {
		this.columns = columns;
	}
	public String getFamilyName() {
		return familyName;
	}
	public void setFamilyName(String familyName) {
		this.familyName = familyName;
	}
	public void addColumn(Column column) {
		if (column.getFamily().equals(familyName))
			this.columns.add(column);
		else if (column.getFamily() == null) {
			column.setFamily(familyName);
			this.columns.add(column);
		}
		else
			throw new RuntimeException("Column family doesn't match.");
	}
} 
