package com.ir.hbase.hive.objectinspector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import com.ir.hbase.client.structured.Column;
import com.ir.hbase.client.structured.Family;
import com.ir.hbase.client.structured.TableStructure;
import com.ir.hbase.hive.BytesUtil;
import com.ir.hbase.hive.HiveHBaseUtil;

public class HBaseStructObjectInspector extends StructObjectInspector {
	public static final Log LOG = LogFactory.getLog(HBaseStructObjectInspector.class);
	List<HBaseStructField> structFieldRefs = new ArrayList<HBaseStructField>();
	Map<String,HBaseStructField> structFieldRefsByName = new HashMap<String,HBaseStructField>();
	
	public HBaseStructObjectInspector(TableStructure tableStructure) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("Creating HBase Struct Object Inspector for " + tableStructure.getTableName());
			if (LOG.isTraceEnabled()) {
				LOG.debug("Table Structure: " + HiveHBaseUtil.gson.toJson(tableStructure));
			}
		}

		for (Family family: tableStructure.getFamilies()) {
			for (Column column: family.getColumns()) {
				HBaseStructField field = new HBaseStructField(column);
				structFieldRefs.add(field);
				structFieldRefsByName.put(column.getColumnName(), field); // Make it as quick as possible for lookups...
			}
		}
	}

	@Override
	public String getTypeName() {
		return this.getClass().getName();
	}

	@Override
	public Category getCategory() {
		return Category.STRUCT;
	}

	@Override
	public List<? extends StructField> getAllStructFieldRefs() {
		return structFieldRefs;
	}

	@Override
	public StructField getStructFieldRef(String fieldName) {
		return structFieldRefsByName.get(fieldName);
	}

	@Override
	public Object getStructFieldData(Object data, StructField fieldRef) {
		Result result = (Result) data;
		HBaseStructField ref = (HBaseStructField) fieldRef;
		byte[] value = result.getValue(ref.getFamily(),ref.getQualifier());
		return BytesUtil.fromBytes(value, ref.getColumnType());
	}

	@Override
	public List<Object> getStructFieldsDataAsList(Object data) {
		List<Object> objects = new ArrayList<Object>(structFieldRefs.size());
		for (StructField fieldRef :structFieldRefs) {
			objects.add(getStructFieldData(data,fieldRef));
		}
		return objects;
	}

}
