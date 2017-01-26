/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.mrio.api.hive;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.*;
import com.splicemachine.derby.stream.iapi.ScanSetBuilder;
import com.splicemachine.derby.stream.output.WriteReadUtils;
import org.apache.commons.lang.SerializationUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;
import com.splicemachine.mrio.MRConstants;
import com.splicemachine.mrio.api.core.NameType;
import com.splicemachine.mrio.api.core.SMSQLUtil;
import com.splicemachine.mrio.api.serde.ExecRowWritable;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import java.io.IOException;
import java.io.Serializable;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.*;

// TODO FIX HIVE INTEGRATION JL

public class SMSerDe implements SerDe {
	protected StructTypeInfo rowTypeInfo;
    protected ObjectInspector rowOI;
    protected SMSQLUtil sqlUtil = null;
//    protected SerDeParameters serdeParams;
    protected List<String> colNames = new ArrayList<String>(); // hive names
    protected List<TypeInfo> colTypes; // hive types, not Splice Types
    protected static Logger Log = Logger.getLogger(SMSerDe.class.getName());
    protected List<Object> objectCache;
    protected String tableName;
    
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
        tableName = tbl.getProperty(MRConstants.SPLICE_TABLE_NAME);
        String hbaseDir = null;
        if (conf != null) {
            hbaseDir = conf.get(HConstants.HBASE_DIR);
        }
        if (hbaseDir == null)
        	hbaseDir = System.getProperty(HConstants.HBASE_DIR);
        if (hbaseDir == null)
        	throw new SerDeException("hbase root directory not set, please include hbase.rootdir in config or via -D system property ...");
        if (conf != null) {
            conf.set(MRConstants.SPLICE_INPUT_TABLE_NAME, tableName);
            conf.set(MRConstants.SPLICE_JDBC_STR, tbl.getProperty(MRConstants.SPLICE_JDBC_STR));
            conf.set(HConstants.HBASE_DIR, hbaseDir);
            if (conf.get(HiveConf.ConfVars.POSTEXECHOOKS.varname) == null) {
                conf.set(HiveConf.ConfVars.POSTEXECHOOKS.varname, "com.splicemachine.mrio.api.hive.PostExecHook");
            }
            if (conf.get(HiveConf.ConfVars.ONFAILUREHOOKS.varname) == null) {
                conf.set(HiveConf.ConfVars.ONFAILUREHOOKS.varname, "com.splicemachine.mrio.api.hive.FailureExecHook");
            }
        }

        if (sqlUtil == null)
            sqlUtil = SMSQLUtil.getInstance(tbl.getProperty(MRConstants.SPLICE_JDBC_STR));
        String colNamesStr = tbl.getProperty(Constants.LIST_COLUMNS);
        colNames.clear();
        for (String split: colNamesStr.split(","))
        	colNames.add(split.toUpperCase());
        String colTypesStr = tbl.getProperty(Constants.LIST_COLUMN_TYPES);
        colTypes = TypeInfoUtils.getTypeInfosFromTypeString(colTypesStr);
        objectCache = new ArrayList<Object>(colTypes.size());
        if (tableName != null) {
            tableName = tableName.trim().toUpperCase();
            try {
                if (!sqlUtil.checkTableExists(tableName))
                	throw new SerDeException(String.format("table %s does not exist...",tableName));
                if (conf != null) {
                    ScanSetBuilder tableScannerBuilder = sqlUtil.getTableScannerBuilder(tableName, colNames);
                    conf.set(MRConstants.SPLICE_SCAN_INFO, tableScannerBuilder.base64Encode());

                  //  TableContext tableContext = sqlUtil.createTableContext(tableName, tableScannerBuilder);
                  //  conf.set(MRConstants.SPLICE_TBLE_CONTEXT, tableContext.getTableContextBase64String());
                }
			} catch (Exception e) {
				throw new SerDeException(e);
			}
        } 
         
    	if (Log.isDebugEnabled())
    		SpliceLogUtils.debug(Log, "generating hive info colNames=%s, colTypes=%s",colNames,colTypes);

        
        rowTypeInfo = (StructTypeInfo) TypeInfoFactory.getStructTypeInfo(colNames, colTypes);
        rowOI = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(rowTypeInfo);
        //serdeParams = LazySimpleSerDe.initSerdeParams(conf, tbl, getClass().getName());
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
			List<NameType> nameTypes = sqlUtil.getTableStructure(tableName);
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

        try {

        	DataValueDescriptor dvd;
            for (int i = 0; i < fields.size(); i++) {
                StructField field = fields.get(i);
                dvd = row.getColumn(i+1);
                ObjectInspector fieldOI = field.getFieldObjectInspector();
                Object fieldObj = soi.getStructFieldData(obj, field);
                PrimitiveObjectInspector primOI = (PrimitiveObjectInspector) fieldOI;
                Object data = primOI.getPrimitiveJavaObject(fieldObj);

                PrimitiveCategory primitiveCategory = primOI.getPrimitiveCategory();
                switch (primitiveCategory) {
                    case BYTE:
                        dvd.setValue(((Byte) data).byteValue());
                        break;
                    case INT:
                        dvd.setValue(((Integer) data).intValue());
                        break;
                    case VARCHAR:
                        dvd.setValue(((HiveVarchar)data).getValue());
                        break;
                    case CHAR:
                        dvd.setValue(((HiveChar)data).getValue());
                        break;
                    case STRING:
                        dvd.setValue((String) data);
                        break;
                    case BINARY:
                        dvd.setValue((SerializationUtils.serialize((Serializable) data))); // is this right?  Should just be a byte[]
                        break;
                    case BOOLEAN:
                        dvd.setValue(((Boolean) data).booleanValue());
                        break;
                    case DECIMAL:
                        dvd.setValue(((HiveDecimal) data).doubleValue());
                        break;
                    case DOUBLE:
                        dvd.setValue(((Double) data).doubleValue());
                        break;
                    case FLOAT:
                        dvd.setValue(((Float) data).floatValue());
                        break;
                    case LONG:
                        dvd.setValue(((Long) data).longValue());
                        break;
                    case SHORT:
                        dvd.setValue(((Short) data).shortValue());
                        break;
                    case TIMESTAMP:
                        dvd.setValue((Timestamp) data);
                        break;
                    case DATE:
                        dvd.setValue((java.sql.Date) data);
                        break;
                    default:
                        throw new SerDeException(String.format("Hive Type %s Not Supported Yet",primOI.getPrimitiveCategory()));
                }
            }

        } catch (StandardException e) {
            // TODO Auto-generated catch block
            throw new RuntimeException("Serialized Object To Java Type Error");
        }
        ExecRowWritable rowWritable = new ExecRowWritable(WriteReadUtils.getExecRowFromTypeFormatIds(execRowFormatIds));
        rowWritable.set(row);
        return rowWritable;
    }
    
    /**
     * Replace with Lazy eventually
     * 
     */
    private static Object hiveTypeToObject(String hiveType, DataValueDescriptor dvd) throws SerDeException {
        final String lctype = trim(hiveType.toLowerCase());

        try {
	        switch(lctype) {
                case "string":
		        case "varchar":
                    HiveVarchar hiveVarchar = null;
                    String s = dvd.getString();
                    if (s!=null) {
                        hiveVarchar = new HiveVarchar();
                        hiveVarchar.setValue(s);
                    }
                    return hiveVarchar;
                case "char":
                    HiveChar hiveChar = null;
                    s = dvd.getString();
                    if (s != null) {
                        hiveChar = new HiveChar();
                        hiveChar.setValue(s);
                    }
                    return hiveChar;
		        case "float":
		            return dvd.getFloat();
		        case "double":
                    return dvd.getDouble();
                case "decimal":
                    Double d = dvd.getDouble();
                    return HiveDecimal.create(d.toString());
		        case "boolean":
		        	return dvd.getBoolean();
		        case "tinyint":
                    return dvd.getByte();
		        case "int":
		        	return dvd.getInt();
		        case "smallint":
		        	return dvd.getShort();
		        case "bigint":
		        	return dvd.getLong();
		        case "timestamp":
                    return dvd.getTimestamp(null);
                case "date":
		        	return dvd.getDate(null);
		        case "binary":
		        	return dvd.getBytes();
		        default:
		        	throw new SerDeException("Unrecognized column type: " + hiveType);
	        }        
        } catch (StandardException se) {
        	throw new SerDeException(se);
        }
    }

    private static String trim(String s) {
        if (s == null || s.length() == 0)
            return s;

        int index = s.indexOf("(");
        if (index == -1)
            return s;

        return s.substring(0, index);
    }
}