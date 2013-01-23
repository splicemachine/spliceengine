package com.ir.hbase.hive.objectinspector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import com.ir.hbase.client.structured.Column;

public class HBaseStructField implements StructField {
	public static final Log LOG = LogFactory.getLog(HBaseStructField.class);
	Column column;
	ObjectInspector objectInspector;
	private byte[] family;
	private byte[] qualifier;
	public HBaseStructField(Column column) {
		if (LOG.isTraceEnabled()) {
			LOG.trace("Creating Struct Field: " + column.getColumnName() + " : " + column.getType());
		}
		this.column = column;
		if (column.getType().equals(Column.Type.BIGINT))
			objectInspector = PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(PrimitiveCategory.LONG);
		else if (column.getType().equals(Column.Type.BOOLEAN))
			objectInspector = PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(PrimitiveCategory.BOOLEAN);
		else if (column.getType().equals(Column.Type.BYTE))
			objectInspector = PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(PrimitiveCategory.BYTE);
		else if (column.getType().equals(Column.Type.CALENDAR))
			objectInspector = PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(PrimitiveCategory.TIMESTAMP);
		else if (column.getType().equals(Column.Type.DATE))
			objectInspector = PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(PrimitiveCategory.TIMESTAMP);
		else if (column.getType().equals(Column.Type.DOUBLE))
			objectInspector = PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(PrimitiveCategory.DOUBLE);
		else if (column.getType().equals(Column.Type.DOUBLE))
			objectInspector = PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(PrimitiveCategory.DOUBLE);
		else if (column.getType().equals(Column.Type.FLOAT))
			objectInspector = PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(PrimitiveCategory.FLOAT);
		else if (column.getType().equals(Column.Type.INTEGER))
			objectInspector = PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(PrimitiveCategory.INT);
		else if (column.getType().equals(Column.Type.LONG))
			objectInspector = PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(PrimitiveCategory.LONG);
		else if (column.getType().equals(Column.Type.STRING))
			objectInspector = PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(PrimitiveCategory.STRING);	
		else {
			LOG.error("Error in creation struct field " + column.getColumnName() + " : " + column.getType());
		}
		family = column.getFamily().getBytes();
		qualifier = column.getColumnName().getBytes();
	}
	
	@Override
	public String getFieldName() {
		return column.getColumnName();
	}

	@Override
	public ObjectInspector getFieldObjectInspector() {
		return objectInspector;
	}

	@Override
	public String getFieldComment() {
		return column.getDescription();
	}

	public byte[] getFamily() {
		return family;
	}

	public void setFamily(byte[] family) {
		this.family = family;
	}

	public byte[] getQualifier() {
		return qualifier;
	}

	public void setQualifier(byte[] qualifier) {
		this.qualifier = qualifier;
	}

	public Column.Type getColumnType() {
		return column.getType();
	}
}
