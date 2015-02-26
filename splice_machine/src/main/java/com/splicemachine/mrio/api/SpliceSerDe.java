package com.splicemachine.mrio.api;

import org.apache.commons.lang.SerializationUtils;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.types.*;
import org.apache.derby.impl.sql.execute.ValueRow;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.lazy.LazyUtils;
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

import java.io.Serializable;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.*;

public class SpliceSerDe implements SerDe {
	protected StructTypeInfo rowTypeInfo;
    protected ObjectInspector rowOI;
    protected List<Object> row = new ArrayList<Object>();
    protected SMSQLUtil sqlUtil = null;
    protected LazySimpleSerDe.SerDeParameters serdeParams;
    protected List<String> colNames;
    protected List<TypeInfo> colTypes;
    protected static Logger Log = Logger.getLogger(SpliceSerDe.class.getName());

    /**
     * An initialization function used to gather information about the table.
     * Typically, a SerDe implementation will be interested in the list of
     * column names and their types. That information will be used to help
     * perform actual serialization and deserialization of data.
     */
    //@Override
    public void initialize(Configuration conf, Properties tbl)
            throws SerDeException {
        // Get a list of the table's column names.
        String spliceInputTableName = tbl.getProperty(MRConstants.SPLICE_INPUT_TABLE_NAME);
        String spliceOutputTableName = tbl.getProperty(MRConstants.SPLICE_OUTPUT_TABLE_NAME);
        if (sqlUtil == null)
            sqlUtil = SMSQLUtil.getInstance(tbl.getProperty(MRConstants.SPLICE_JDBC_STR));

        String colNamesStr = tbl.getProperty(Constants.LIST_COLUMNS);
        colNames = Arrays.asList(colNamesStr.split(","));

        // Get a list of TypeInfos for the columns. This list lines up with the list of column names.
        String colTypesStr = tbl.getProperty(Constants.LIST_COLUMN_TYPES);
        colTypes = TypeInfoUtils.getTypeInfosFromTypeString(colTypesStr);
        
        if (spliceInputTableName != null) {
            spliceInputTableName = spliceInputTableName.trim().toUpperCase();
            try {
				sqlUtil.getTableScannerBuilder(spliceInputTableName, colNames);
			} catch (SQLException e) {
				throw new SerDeException(e);
			}
        } else if (spliceOutputTableName != null) {
            spliceOutputTableName = spliceOutputTableName.trim().toUpperCase();
        }
                
        rowTypeInfo = (StructTypeInfo) TypeInfoFactory.getStructTypeInfo(colNames, colTypes);
        rowOI = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(rowTypeInfo);
        serdeParams = LazySimpleSerDe.initSerdeParams(conf, tbl, getClass().getName());
        Log.info("--------Finished initialize");
    }

    private void fillRow(DataValueDescriptor[] dvds) throws StandardException {
    	
    	/*
        for (int pos = 0; pos < colTypes.size(); pos++) {
        	colTypes.get(0).
        	
        	
            switch (colTypes.get(pos)) {
                case java.sql.Types.INTEGER:
                    row.add(dvds[pos].getInt());
                    break;
                case java.sql.Types.BIGINT:
                    row.add(dvds[pos].getLong());
                    break;
                case java.sql.Types.SMALLINT:
                    row.add(dvds[pos].getShort());
                    break;
                case java.sql.Types.BOOLEAN:
                    row.add(dvds[pos].getBoolean());
                    break;
                case java.sql.Types.DOUBLE:
                    row.add(dvds[pos].getDouble());
                    break;
                case java.sql.Types.FLOAT:
                    row.add(dvds[pos].getFloat());
                    break;
                case java.sql.Types.CHAR:

                case java.sql.Types.VARCHAR:
                    row.add(dvds[pos].getString());

                    break;
                case java.sql.Types.BINARY:

                default:
                    row.add(dvds[pos].getBytes());
            }
        }
            */
    }

    /**
     * This method does the work of deserializing a record into Java objects
     * that Hive can work with via the ObjectInspector interface.
     */
    //@Override
    public Object deserialize(Writable blob) throws SerDeException {
        row.clear();
        Log.debug("*******" + Thread.currentThread().getStackTrace()[1].getMethodName());
        ExecRowWritable rowWritable = (ExecRowWritable) blob;

        try {
            ExecRow val = rowWritable.get();
            if (val == null)
                return null;
            DataValueDescriptor dvd[] = val.getRowArray();
            if (dvd == null || dvd.length == 0)
                return row;
            fillRow(dvd);
        } catch (StandardException e) {
            // TODO Auto-generated catch block
            throw new SerDeException("deserialization error, " + e.getCause());
        }
        return row;
    }

    /**
     * Return an ObjectInspector for the row of data
     */
    //@Override
    public ObjectInspector getObjectInspector() throws SerDeException {
        Log.debug("******" + Thread.currentThread().getStackTrace()[1].getMethodName());
        return rowOI;
    }

    /**
     * Unimplemented
     */
    //@Override
    public SerDeStats getSerDeStats() {
        Log.debug("*******" + Thread.currentThread().getStackTrace()[1].getMethodName());
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
        // Take the object and transform it into a serialized representation
        DataValueDescriptor dvds[] = new DataValueDescriptor[colTypes.size()];
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

            for (int i = 0; i < fields.size(); i++) {
                StructField field = fields.get(i);
                ObjectInspector fieldOI = field.getFieldObjectInspector();
                Object fieldObj = soi.getStructFieldData(obj, field);

                PrimitiveObjectInspector primOI = (PrimitiveObjectInspector) fieldOI;
                Object data = primOI.getPrimitiveJavaObject(fieldObj);
                if (i >= dvds.length)
                    break;

                if (primOI.getPrimitiveCategory() == PrimitiveCategory.INT)
                    dvds[i] = new SQLInteger((Integer) data);
                else if (primOI.getPrimitiveCategory() == PrimitiveCategory.STRING)
                    dvds[i] = new SQLVarchar((String) data);
                else if (primOI.getPrimitiveCategory() == PrimitiveCategory.BINARY)
                    dvds[i] = new SQLBlob(SerializationUtils.serialize((Serializable) data));
                else if (primOI.getPrimitiveCategory() == PrimitiveCategory.BOOLEAN)
                    dvds[i] = new SQLBoolean((Boolean) data);
                else if (primOI.getPrimitiveCategory() == PrimitiveCategory.DECIMAL)
                    dvds[i] = new org.apache.derby.iapi.types.SQLDecimal((String) data);
                else if (primOI.getPrimitiveCategory() == PrimitiveCategory.DOUBLE)
                    dvds[i] = new SQLDouble((Double) data);
                else if (primOI.getPrimitiveCategory() == PrimitiveCategory.FLOAT)
                    // Is it the correct way? Treat Float as SQLReal?
                    dvds[i] = new SQLReal((Float) data);
                else if (primOI.getPrimitiveCategory() == PrimitiveCategory.LONG)
                    dvds[i] = new SQLLongint((Long) data);
                else if (primOI.getPrimitiveCategory() == PrimitiveCategory.SHORT)
                    dvds[i] = new SQLSmallint((Short) data);
                else if (primOI.getPrimitiveCategory() == PrimitiveCategory.TIMESTAMP)
                    dvds[i] = new SQLTimestamp((Timestamp) data);
                // how to deal with PrimitiveCategory.VOID?
            }


        } catch (StandardException e) {
            // TODO Auto-generated catch block
            throw new RuntimeException("Serialized Object To Java Type Error");
        }

        ExecRow row = new ValueRow(dvds.length);
        row.setRowArray(dvds);
        ExecRowWritable rowWritable = null;//new ExecRowWritable(colTypes); NPE
        rowWritable.set(row);
        return rowWritable;

    }
}

