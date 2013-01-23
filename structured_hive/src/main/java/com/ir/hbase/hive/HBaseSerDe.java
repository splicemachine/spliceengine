package com.ir.hbase.hive;

import java.util.List;
import java.util.Properties;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.Writable;
import org.datanucleus.store.valuegenerator.UUIDHexGenerator;
import com.ir.hbase.client.structured.TableStructure;
import com.ir.hbase.hive.objectinspector.HBaseStructField;
import com.ir.hbase.hive.objectinspector.HBaseStructObjectInspector;
import com.ir.constants.bytes.BytesUtil;

public class HBaseSerDe implements SerDe {
  public static final Log LOG = LogFactory.getLog(HBaseSerDe.class);
  public static final String HBASE_TABLE_NAME = "hbase.table.name";
  public static final String COLUMN_LIST = "columns";
  public static final String COLUMN_TYPES = "columns.types";
  
  UUIDHexGenerator generator = new UUIDHexGenerator("UUID_HEX",null); 
  private ObjectInspector cachedObjectInspector;
 
  public HBaseSerDe() throws SerDeException {
  }

  /**
   * Initialize the SerDe given parameters.
   * @see SerDe#initialize(Configuration, Properties)
   */
  @Override
  public void initialize(Configuration conf, Properties tbl) throws SerDeException {
	  if (LOG.isTraceEnabled()) {
		  LOG.trace("initiliaze method called " + tbl.toString());
	  }	 	   
	  TableStructure tableStructure = HiveHBaseUtil.generateTableStructure(tbl.getProperty(COLUMN_TYPES), tbl.getProperty(COLUMN_LIST));
	  cachedObjectInspector = new HBaseStructObjectInspector(tableStructure);		
  }

  @Override
  public Object deserialize(Writable result) throws SerDeException {
	  if (LOG.isTraceEnabled()) {
		  LOG.trace("deserialize called on " + HiveHBaseUtil.gson.toJson(result));
	  }	 	
	  return result;
  }

  @Override
  public ObjectInspector getObjectInspector() throws SerDeException {
	  if (LOG.isTraceEnabled()) {
		  LOG.trace("getObjectInspector called");
	  }	 
    return cachedObjectInspector;
  }

  @Override
  public Class<? extends Writable> getSerializedClass() {
	  if (LOG.isTraceEnabled()) {
		  LOG.trace("getSerializedClass called returning class of type " + Put.class.toString());
	  }	
    return Put.class;
  }

  @Override
  public Writable serialize(Object obj, ObjectInspector objInspector) throws SerDeException {
	  if (LOG.isTraceEnabled()) {
		  LOG.trace("serialize called for object ");
	  }	

    if (objInspector.getCategory() != Category.STRUCT) {
      throw new SerDeException(getClass().toString()
          + " can only serialize struct types, but we got: "
          + objInspector.getTypeName());
    }
    StructObjectInspector soi = (StructObjectInspector) objInspector;
    List<? extends StructField> fields = soi.getAllStructFieldRefs();
    HBaseStructObjectInspector tableFields = (HBaseStructObjectInspector) getObjectInspector();
    Put put = null;  
    for (int i=0;i<fields.size();i++) {
    	StructField structField = fields.get(i);
    	HBaseStructField hBaseStructField = (HBaseStructField) tableFields.getAllStructFieldRefs().get(i);
    	if (hBaseStructField == null)
    		throw new SerDeException("structField is null " + structField.getFieldName() + " : " + tableFields.toString());
    	if (hBaseStructField.getFamily() == null)
    		throw new SerDeException("structFieldFamily is null");
    	if (hBaseStructField.getQualifier() == null)
    		throw new SerDeException("structFieldQualifier is null");
    	if (hBaseStructField.getFieldObjectInspector() == null)
    		throw new SerDeException("structFieldObjectInspector is null");
    	PrimitiveObjectInspector ins = ((PrimitiveObjectInspector) structField.getFieldObjectInspector());
    	if (i==0) {
    	   	put = new Put(BytesUtil.toBytes(ins.getPrimitiveJavaObject(soi.getStructFieldData(obj, structField)), ins.getJavaPrimitiveClass()));
    	}
    	put.add(hBaseStructField.getFamily(), hBaseStructField.getQualifier(), 
    			BytesUtil.toBytes(ins.getPrimitiveJavaObject(soi.getStructFieldData(obj, structField)), ins.getJavaPrimitiveClass()));
    }        
    return put;
  }

  public SerDeStats getSerDeStats() {
	  if (LOG.isTraceEnabled()) {
		  LOG.trace("getSerDeStats, no stats currently ");
	  }	
    return null;
  }
}

