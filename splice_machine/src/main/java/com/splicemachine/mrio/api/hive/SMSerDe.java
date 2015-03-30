package com.splicemachine.mrio.api.hive;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.*;
import org.apache.commons.lang.SerializationUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

import com.splicemachine.mrio.MRConstants;
import com.splicemachine.mrio.api.core.NameType;
import com.splicemachine.mrio.api.core.SMSQLUtil;
import com.splicemachine.mrio.api.serde.ExecRowWritable;
import com.splicemachine.utils.SpliceLogUtils;

import java.io.IOException;
import java.io.Serializable;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.*;

public class SMSerDe implements SerDe {
	protected StructTypeInfo rowTypeInfo;
    protected ObjectInspector rowOI;
    protected SMSQLUtil sqlUtil = null;
    protected LazySimpleSerDe.SerDeParameters serdeParams;
    protected List<String> colNames = new ArrayList<String>(); // hive names
    protected List<TypeInfo> colTypes; // hive types, not Splice Types
    protected static Logger Log = Logger.getLogger(SMSerDe.class.getName());
    protected List<Object> objectCache;
    
    /**
     * An initialization function used to gather information about the table.
     * Typically, a SerDe implementation will be interested in the list of
     * column names and their types. That information will be used to help
     * perform actual serialization and deserialization of data.
     */
    //@Override
    public void initialize(Configuration conf, Properties tbl) throws SerDeException {
    	if (Log.isDebugEnabled())
    		SpliceLogUtils.debug(Log, "initialize with conf=%s, tbl=%s",conf,tbl);
        // Get a list of the table's column names.
        String spliceInputTableName = tbl.getProperty(MRConstants.SPLICE_TABLE_NAME);
        conf.set(MRConstants.SPLICE_TABLE_NAME, spliceInputTableName);
        conf.set(MRConstants.SPLICE_JDBC_STR, tbl.getProperty(MRConstants.SPLICE_JDBC_STR));
        String hbaseDir = conf.get(HConstants.HBASE_DIR);
        if (hbaseDir == null)
        	hbaseDir = System.getProperty(HConstants.HBASE_DIR);
        if (hbaseDir == null)
        	throw new SerDeException("hbase root directory not set, please include hbase.rootdir in config or via -D system property ...");
        conf.set(HConstants.HBASE_DIR, hbaseDir);
        
        if (sqlUtil == null)
            sqlUtil = SMSQLUtil.getInstance(tbl.getProperty(MRConstants.SPLICE_JDBC_STR));
        String colNamesStr = tbl.getProperty(Constants.LIST_COLUMNS);
        colNames.clear();
        for (String split: colNamesStr.split(","))
        	colNames.add(split.toUpperCase());
        String colTypesStr = tbl.getProperty(Constants.LIST_COLUMN_TYPES);
        colTypes = TypeInfoUtils.getTypeInfosFromTypeString(colTypesStr);
        objectCache = new ArrayList<Object>(colTypes.size());
        if (spliceInputTableName != null) {
            spliceInputTableName = spliceInputTableName.trim().toUpperCase();
            try {
                if (!sqlUtil.checkTableExists(spliceInputTableName))
                	throw new SerDeException(String.format("table %s does not exist...",spliceInputTableName));
            	conf.set(MRConstants.SPLICE_SCAN_INFO, sqlUtil.getTableScannerBuilder(spliceInputTableName, colNames).getTableScannerBuilderBase64String());
			} catch (Exception e) {
				throw new SerDeException(e);
			}
        } 
         
    	if (Log.isDebugEnabled())
    		SpliceLogUtils.debug(Log, "generating hive info colNames=%s, colTypes=%s",colNames,colTypes);

        
        rowTypeInfo = (StructTypeInfo) TypeInfoFactory.getStructTypeInfo(colNames, colTypes);
        rowOI = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(rowTypeInfo);
        serdeParams = LazySimpleSerDe.initSerdeParams(conf, tbl, getClass().getName());
        Log.info("--------Finished initialize");
    }

    /**
     * This method does the work of deserializing a record into Java objects
     * that Hive can work with via the ObjectInspector interface.
     */
    //@Override
    public Object deserialize(Writable blob) throws SerDeException {
    	if (Log.isTraceEnabled())
    		SpliceLogUtils.trace(Log, "deserialize " + blob);
        ExecRowWritable rowWritable = (ExecRowWritable) blob;
        objectCache.clear();
            ExecRow val = rowWritable.get();
            if (val == null)
                return null;
            DataValueDescriptor[] dvd = val.getRowArray();
            if (dvd == null || dvd.length == 0)
                return objectCache;
            for (int i = 0; i< dvd.length; i++) {
            	objectCache.add(hiveTypeToObject(colTypes.get(i).getTypeName(),dvd[i]));            	
            }
        return objectCache;
    }

    /**
     * Return an ObjectInspector for the row of data
     */
    //@Override
    public ObjectInspector getObjectInspector() throws SerDeException {
    	if (Log.isDebugEnabled())
    		SpliceLogUtils.trace(Log, "getObjectInspector");
        return rowOI;
    }

    /**
     * Unimplemented
     */
    //@Override
    public SerDeStats getSerDeStats() {
    	if (Log.isDebugEnabled())
    		SpliceLogUtils.trace(Log, "serdeStats");
        return null;
    }

    /**
     * Return the class that stores the serialized data representation.
     */
    //@Override
    public Class<? extends Writable> getSerializedClass() {
        Log.debug("********" + Thread.currentThread().getStackTrace()[1].getMethodName());
        return ExecRowWritable.class;
    }

    /**
     * This method takes an object representing a row of data from Hive, and
     * uses the ObjectInspector to get the data for each column and serialize
     * it.
     */
    //@Override
    public Writable serialize(Object obj, ObjectInspector oi)
            throws SerDeException {
    	ExecRow row = null;
    	int[] execRowFormatIds = null;
        try {
			List<NameType> nameTypes = sqlUtil.getTableStructure(null);
			execRowFormatIds = sqlUtil.getExecRowFormatIds(colNames, nameTypes);
			row = sqlUtil.getExecRow(execRowFormatIds);
			if (row == null)
				throw new SerDeException("ExecRow Cannot be Null");
		} catch (SQLException | StandardException | IOException e1) {
			throw new SerDeException(e1);
		}    	
    	if (Log.isTraceEnabled())
    		SpliceLogUtils.trace(Log, "serialize with obj=%s, oi=%s",obj,oi);
        if (oi.getCategory() != ObjectInspector.Category.STRUCT) {
            throw new SerDeException(getClass().toString()
                    + " can only serialize struct types, but we got: "
                    + oi.getTypeName());
        }

        StructObjectInspector soi = (StructObjectInspector) oi;
        List<? extends StructField> fields = soi.getAllStructFieldRefs();
        List<Object> list = soi.getStructFieldsDataAsList(obj);
        List<? extends StructField> declaredFields =
                (serdeParams.getRowTypeInfo() != null &&
                        ((StructTypeInfo) serdeParams.getRowTypeInfo())
                                .getAllStructFieldNames().size() > 0) ?
                        ((StructObjectInspector) getObjectInspector()).getAllStructFieldRefs()
                        : null;
        try {

        	DataValueDescriptor dvd;
            for (int i = 0; i < fields.size(); i++) {
                StructField field = fields.get(i);
                dvd = row.getColumn(i+1);
                ObjectInspector fieldOI = field.getFieldObjectInspector();
                Object fieldObj = soi.getStructFieldData(obj, field);
                PrimitiveObjectInspector primOI = (PrimitiveObjectInspector) fieldOI;
                Object data = primOI.getPrimitiveJavaObject(fieldObj);
                
                if (primOI.getPrimitiveCategory() == PrimitiveCategory.INT)
                    dvd.setValue((Integer) data);
                else if (primOI.getPrimitiveCategory() == PrimitiveCategory.STRING)
                	dvd.setValue((String) data);
                else if (primOI.getPrimitiveCategory() == PrimitiveCategory.BINARY)
                	dvd.setValue((SerializationUtils.serialize((Serializable) data))); // is this right?  Should just be a byte[]
                else if (primOI.getPrimitiveCategory() == PrimitiveCategory.BOOLEAN)
                    dvd.setValue((Boolean) data);
                else if (primOI.getPrimitiveCategory() == PrimitiveCategory.DECIMAL)
                    dvd.setValue((String) data);
                else if (primOI.getPrimitiveCategory() == PrimitiveCategory.DOUBLE)
                    dvd.setValue((Double) data);
                else if (primOI.getPrimitiveCategory() == PrimitiveCategory.FLOAT)
                    dvd.setValue((Float) data);
                else if (primOI.getPrimitiveCategory() == PrimitiveCategory.LONG)
                	dvd.setValue((Long) data);
                else if (primOI.getPrimitiveCategory() == PrimitiveCategory.SHORT)
                	dvd.setValue((Short) data);
                else if (primOI.getPrimitiveCategory() == PrimitiveCategory.TIMESTAMP)
                	dvd.setValue((Timestamp) data);
                else {
                	throw new SerDeException(String.format("Hive Type %s Not Supported Yet",primOI.getPrimitiveCategory()));
                }
            }

        } catch (StandardException e) {
            // TODO Auto-generated catch block
            throw new RuntimeException("Serialized Object To Java Type Error");
        }
        ExecRowWritable rowWritable = new ExecRowWritable(execRowFormatIds);
        rowWritable.set(row);
        return rowWritable;
    }
    
    /**
     * Replace with Lazy eventually
     * 
     */
    private static Object hiveTypeToObject(String hiveType, DataValueDescriptor dvd) throws SerDeException {
        final String lctype = hiveType.toLowerCase();
        try {
	        switch(lctype) {
		        case "string":
		        	return dvd.getString();
		        case "float":
		            return dvd.getFloat();
		        case "double": 
		            return dvd.getDouble();
		        case "booolean":
		        	return dvd.getBoolean();
		        case "tinyint":
		        case "int":
		        	return dvd.getInt();
		        case "smallint":
		        	return dvd.getShort();
		        case "bigint":
		        	return dvd.getLong();
		        case "timestamp":
		        	return dvd.getLong();
		        case "binary":
		        	return dvd.getBytes();
		        default:
		        	throw new SerDeException("Unrecognized column type: " + hiveType);
	        }        
        } catch (StandardException se) {
        	throw new SerDeException(se);
        }
    
    }

}