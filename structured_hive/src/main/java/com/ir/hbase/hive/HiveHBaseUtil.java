package com.ir.hbase.hive;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;

import com.google.gson.Gson;
import com.ir.constants.SpliceConstants;
import com.ir.hbase.client.structured.Column;
import com.ir.hbase.client.structured.Family;
import com.ir.hbase.client.structured.TableStructure;

public class HiveHBaseUtil {
	public static Gson gson = new Gson();
	public static TableStructure generateTableStructure(Table tbl) {
		TableStructure tableStructure = new TableStructure();
		Family family = new Family(SpliceConstants.DEFAULT_FAMILY);
		System.out.println("Storage" + tbl.getSd().toString());
		for (FieldSchema fieldSchema: tbl.getSd().getCols()) {
			System.out.println("fieldSchema " + fieldSchema.toString());
			Column column = new Column();
			column.setColumnName(fieldSchema.getName());
			column.setDescription(fieldSchema.getComment());
			column.setType(hiveToHBase(fieldSchema.getType()));
			column.setFamily(SpliceConstants.DEFAULT_FAMILY);
			family.addColumn(column);
		}
		tableStructure.addFamily(family);
		return tableStructure;
	}

	public static TableStructure generateTableStructure(String columnTypesString, String columnString) {
		TableStructure tableStructure = new TableStructure();
		Family family = new Family(SpliceConstants.DEFAULT_FAMILY);
		String[] columns = columnString.split(",");
		String[] columnTypes = columnTypesString.split(":");
		for (int i=0;i<columns.length;i++) {
			Column column = new Column();
			column.setColumnName(columns[i]);
			column.setType(hiveToHBase(columnTypes[i]));
			family.addColumn(column);
		}	
		tableStructure.addFamily(family);
		return tableStructure;
	}

	public static Column.Type hiveToHBase(String type) {
		if (type.equals("tinyint"))
			return Column.Type.INTEGER;
		else if (type.equals("smallint"))
			return Column.Type.INTEGER;
		else if (type.equals("int"))
			return Column.Type.INTEGER;
		else if (type.equals("bigint"))
			return Column.Type.LONG;
		else if (type.equals("float"))
			return Column.Type.FLOAT;
		else if (type.equals("double"))
			return Column.Type.DOUBLE;		
		else if (type.equals("boolean"))
			return Column.Type.BOOLEAN;		
		else if (type.equals("string"))
			return Column.Type.STRING;
		else 
			throw new RuntimeException("UnSupported Column Data Type of " + type);
	}
}
