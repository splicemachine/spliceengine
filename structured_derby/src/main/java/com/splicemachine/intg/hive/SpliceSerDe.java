package com.splicemachine.intg.hive;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.SQLBlob;
import org.apache.derby.iapi.types.SQLBoolean;
import org.apache.derby.iapi.types.SQLDouble;
import org.apache.derby.iapi.types.SQLInteger;
import org.apache.derby.iapi.types.SQLLongint;
import org.apache.derby.iapi.types.SQLSmallint;
import org.apache.derby.iapi.types.SQLVarchar;
import org.apache.derby.impl.sql.execute.ValueRow;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.splicemachine.mrio.api.SQLUtil;
import com.splicemachine.mrio.api.SpliceMRConstants;

public class SpliceSerDe implements SerDe {
	 private StructTypeInfo rowTypeInfo;
	 private ObjectInspector rowOI;
	 private List<String> colNames;
	 private List<Integer> colTypes;
	 public static final String SPLICE_TABLE_NAME = "splice.table.name";
	 public static final String SPLICE_JDBC_STR = "splice.jdbc";
	 private List<Object> row = new ArrayList<Object>();
	 private SQLUtil sqlUtil = null;
	 static Logger Log = Logger.getLogger(
			 SpliceSerDe.class.getName());

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
	   String spliceTableName = tbl.getProperty(SpliceSerDe.SPLICE_TABLE_NAME);
	   spliceTableName = spliceTableName.trim();
	   if(sqlUtil == null)
		   sqlUtil = SQLUtil.getInstance(tbl.getProperty(SpliceMRConstants.SPLICE_JDBC_STR));
	   getSpliceTableStructure(spliceTableName);
	  
	   String colNamesStr = tbl.getProperty(Constants.LIST_COLUMNS);
	   colNames = Arrays.asList(colNamesStr.split(","));
	   Log.info("------col names read from property: "+colNamesStr);
	   // Get a list of TypeInfos for the columns. This list lines up with
	   // the list of column names.
	   String colTypesStr = tbl.getProperty(Constants.LIST_COLUMN_TYPES);
	   List<TypeInfo> colTypes =
	       TypeInfoUtils.getTypeInfosFromTypeString(colTypesStr);
	  
	   rowTypeInfo =
	       (StructTypeInfo) TypeInfoFactory.getStructTypeInfo(colNames, colTypes);
	   rowOI =
	       TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(rowTypeInfo);
	   for(TypeInfo typeInfo : colTypes)
	   {
		   Log.info("----- col Type: "+typeInfo.getTypeName());
	   }
	   Log.info("--------Finished initialize");
	 }

	 
	 // temporarily use this method to read table structure of Splice, 
	 // colTypes helps decoding ExecRow, which can be filled in the Hive Row.
	 // this will cost the inconsistency if reading data out of Splice at the same time DDL.
	 private void getSpliceTableStructure(String tableName)
	 {
		    HashMap<List, List> tableStructure = new HashMap<List, List>();
		    
		 	tableStructure = sqlUtil.getTableStructure(tableName);
			
	    	Iterator iter = tableStructure.entrySet().iterator();
	    	if(iter.hasNext())
	    	{
	    		Map.Entry kv = (Map.Entry)iter.next();
	    		colNames = (ArrayList<String>)kv.getKey();
	    		colTypes = (ArrayList<Integer>)kv.getValue();
	    	}
		    	
	 }
	 
	 private void fillRow(DataValueDescriptor[] dvds) throws StandardException
	 {
		
			for(int pos = 0; pos < colTypes.size(); pos++)
			{
				switch(colTypes.get(pos))
				{
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
			
		}
	 
	 /**
	  * This method does the work of deserializing a record into Java objects
	  * that Hive can work with via the ObjectInspector interface.
	  */
	 //@Override
	 public Object deserialize(Writable blob) throws SerDeException {
	   row.clear();
	   // Do work to turn the fields in the blob into a set of row fields
	   Log.debug(Thread.currentThread() .getStackTrace()[1].getMethodName());
	   ExecRowWritable rowWritable = (ExecRowWritable) blob;
	   
	   try {
		   ExecRow val = rowWritable.get();
		   if (val == null)
			   return null;
		   DataValueDescriptor dvd[] = val.getRowArray();
		   if(dvd == null || dvd.length == 0)
			   return row;
		
		//row.add(rowWritable.get().getRowArray()[0].getString());
		   fillRow(dvd);
		   
	   } catch (StandardException e) {
		// TODO Auto-generated catch block
		  throw new SerDeException("deserialization error, "+e.getCause());
	   }
	   return row;
	 }

	 /**
	  * Return an ObjectInspector for the row of data
	  */
	 //@Override
	 public ObjectInspector getObjectInspector() throws SerDeException {
		 Log.debug(Thread.currentThread() .getStackTrace()[1].getMethodName());
	   return rowOI;
	 }

	 /**
	  * Unimplemented
	  */
	 //@Override
	 public SerDeStats getSerDeStats() {
		 Log.debug(Thread.currentThread() .getStackTrace()[1].getMethodName());
	   return null;
	 }

	 /**
	  * Return the class that stores the serialized data representation.
	  */
	 //@Override
	 public Class<? extends Writable> getSerializedClass() {
		 Log.debug(Thread.currentThread() .getStackTrace()[1].getMethodName());
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
		 Log.debug(Thread.currentThread() .getStackTrace()[1].getMethodName());
		 DataValueDescriptor[] data = new DataValueDescriptor[]{new SQLInteger(1),
					new SQLVarchar("abcd")};
		 ExecRow row = new ValueRow(data.length);
			row.setRowArray(data);
		ExecRowWritable rowWritable = new ExecRowWritable(colTypes);
		rowWritable.set(row);
		return rowWritable;
		 
	 }
}
