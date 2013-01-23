package com.ir.hbase.client.index;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.gson.Gson;
import com.ir.constants.SchemaConstants;

public class IndexTableStructure {
	private Map<String, Index> indexes = new HashMap<String, Index>();
	private static Gson gson = new Gson();
	private String tableName;
	
	public IndexTableStructure() {
	}
	public IndexTableStructure(String tableName) {
		this.tableName = tableName;
	}
	public String getTableName() {
		return tableName;
	}
	public Collection<Index> getIndexes() {
		return indexes.values();
	}
	public Index getIndex(String indexName) {
		return indexes.get(indexName);
	}

	public boolean hasIndexes() {
		return !indexes.isEmpty();
	}
	public boolean hasIndex(String indexName) {
		return indexes.containsKey(indexName);
	}
	public void addIndex(Index index) {
		this.indexes.put(index.getIndexName(), index);
	}
	public String toJSon() {
		return gson.toJson(this);
	}
	public static IndexTableStructure fromJSon(String json) {
		return gson.fromJson(json, IndexTableStructure.class);
	}
	public static HTableDescriptor setIndexStructure(HTableDescriptor desc, IndexTableStructure itableStructure) {
		desc.setValue(SchemaConstants.INDEXTABLE_STRUCTURE, gson.toJson(itableStructure));
		return desc;
	}
	public static IndexTableStructure getIndexTableStructure(HTableDescriptor desc) {
		return gson.fromJson(desc.getValue(SchemaConstants.INDEXTABLE_STRUCTURE), IndexTableStructure.class);
	}
	public static void removeIndexTableStructure(HTableDescriptor desc) {
		desc.remove(Bytes.toBytes(SchemaConstants.INDEXTABLE_STRUCTURE));
	}
}
