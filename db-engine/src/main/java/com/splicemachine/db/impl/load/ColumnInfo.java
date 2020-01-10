/*
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 *
 * Some parts of this source code are based on Apache Derby, and the following notices apply to
 * Apache Derby:
 *
 * Apache Derby is a subproject of the Apache DB project, and is licensed under
 * the Apache License, Version 2.0 (the "License"); you may not use these files
 * except in compliance with the License. You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * Splice Machine, Inc. has modified the Apache Derby code in this file.
 *
 * All such Splice Machine modifications are Copyright 2012 - 2020 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.db.impl.load;

import com.splicemachine.db.iapi.jdbc.EngineConnection;
import com.splicemachine.db.iapi.reference.JDBC40Translation;
import com.splicemachine.db.iapi.services.io.DynamicByteArrayOutputStream;
import com.splicemachine.db.iapi.util.IdUtil;
import com.splicemachine.db.iapi.util.StringUtil;

import java.io.ObjectOutputStream;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

/**
 *	
 * This class provides supportto  create casting/conversions required to 
 * perform import. Import VTI  gives all the data in VARCHAR type becuase data
 * in the files is in CHAR format. There is no implicit cast availabile from
 * VARCHAR to some of the types. In cases where explicit casting is allowed, 
 * columns are casted with  explict cast to the type of table column; in case of 
 * double/real explicit casting is also not allowd , scalar fuction DOUBLE is
 * used in those cases.
 *  
 */
public class ColumnInfo {

    private ArrayList insertColumnNames;
    private ArrayList columnTypes ;
    private ArrayList jdbcColumnTypes;
	private int noOfColumns;
	private Connection conn;
	private String tableName;
	private String schemaName;
    private HashMap udtClassNames;

	/**
	 * Initialize the column type and name  information
	 * @param conn  - connection to use for metadata queries
	 * @param sName - table's schema
	 * @param tName - table Name
	 * @param insertColumnList - an optional list of column identifiers
	 * @exception SQLException on error
	 */
	public ColumnInfo(Connection conn,
					  String sName, 
					  String tName,
					  List<String> insertColumnList)
		throws SQLException  {
		insertColumnNames = new ArrayList(1);
		columnTypes = new ArrayList(1);
        jdbcColumnTypes = new ArrayList(1);
        udtClassNames = new HashMap();
		noOfColumns = 0;
		this.conn = conn;

        if (sName == null) {
            // Use the current schema if no schema is specified.
            sName = ((EngineConnection) conn).getCurrentSchemaName();
        }

		this.schemaName = sName;
		this.tableName =  tName;

		if(insertColumnList != null && ! insertColumnList.isEmpty()) {
			for (String columnName : insertColumnList) {
				if(!initializeColumnInfo(columnName.trim())) {
					if(tableExists())
						throw  LoadError.invalidColumnName(columnName.trim());
					else {
						String entityName = (schemaName !=null ? 
											 schemaName + "." + tableName :tableName); 
						throw LoadError.tableNotFound(entityName);
					}
				}
			}
		} else {
		 	//All columns in the table
			if(!initializeColumnInfo(null)) {
				String entityName = (schemaName !=null ? 
									 schemaName + "." + tableName :tableName); 
				throw LoadError.tableNotFound(entityName);
			}
		}

	}

	private boolean initializeColumnInfo(String columnPattern)
		throws SQLException {
		DatabaseMetaData dmd = conn.getMetaData();
		ResultSet rs = dmd.getColumns(null, 
									  schemaName,
									  tableName,
									  columnPattern);
		boolean foundTheColumn=false;
		while (rs.next()) {
			// 4.COLUMN_NAME String => column name
			String columnName = rs.getString(4);
			// 5.DATA_TYPE short => SQL type from java.sql.Types
			short dataType = rs.getShort(5);
			// 6.TYPE_NAME String => Data source dependent type name
			String typeName = rs.getString(6);
			// 7.COLUMN_SIZE int => column size. For char or date types
			// this is the maximum number of characters, for numeric or
			// decimal types this is precision.
			int columnSize = rs.getInt(7);
			// 9.DECIMAL_DIGITS int => the number of fractional digits
			int decimalDigits = rs.getInt(9);
			// 10.NUM_PREC_RADIX int => Radix (typically either 10 or 2)
			int numPrecRadix = rs.getInt(10);
			foundTheColumn = true;
			if(importExportSupportedType(dataType)) {
				insertColumnNames.add(columnName);
				String sqlType = getSqlType(typeName, columnSize, decimalDigits);
				columnTypes.add(sqlType);
                jdbcColumnTypes.add((int) dataType);
				noOfColumns++;

                if ( dataType == java.sql.Types.JAVA_OBJECT ) {
                    udtClassNames.put( "COLUMN" +  noOfColumns, getUDTClassName( dmd, typeName ) );
                }
			}else {
				rs.close();
				throw
					LoadError.nonSupportedTypeColumn(columnName,typeName);
			}

		}

		rs.close();
		return foundTheColumn;
	}

	private String getSqlType(String typeName, int columnSize, int decimalDigits) {
	    if (typeName.equals("VARCHAR () FOR BIT DATA")) {
            return  "VARCHAR (" + columnSize + ") FOR BIT DATA";
        }
        else if (typeName.equals("CHAR () FOR BIT DATA")) {
            return  "CHAR (" + columnSize + ") FOR BIT DATA";
        }
        else {
            return typeName +  getTypeOption(typeName , columnSize , columnSize , decimalDigits);
        }
    }
    // look up the class name of a UDT
    private String getUDTClassName( DatabaseMetaData dmd, String sqlTypeName )
        throws SQLException
    {
        String className = null;
        
        try {
            // special case for system defined types
            if ( sqlTypeName.charAt( 0 ) != '"' ) { return sqlTypeName; }

            String[] nameParts = IdUtil.parseMultiPartSQLIdentifier( sqlTypeName );

            String schemaName = nameParts[ 0 ];
            String unqualifiedName = nameParts[ 1 ];

            ResultSet rs = dmd.getUDTs( null, schemaName, unqualifiedName, new int[] { java.sql.Types.JAVA_OBJECT } );

            if ( rs.next() )
            {
                className = rs.getString( 4 );
            }
            rs.close();
        }
        catch (Exception e) { throw LoadError.unexpectedError( e ); }

        if ( className == null ) { className = "???"; }
        
        return className;
    }


	//return true if the given type is supported by import/export
	public  static final boolean importExportSupportedType(int type){

		return !(type == java.sql.Types.BIT ||
				 type == java.sql.Types.OTHER ||
				 type == JDBC40Translation.SQLXML ); 
	}



	public static String getTypeOption(String type , int length , int precision , int scale) {

			if ((type.equals("CHAR") ||
				 type.equals("BLOB") ||
				 type.equals("CLOB") ||
				 type.equals("VARCHAR")) && length != 0)
			{
				 return "(" + length + ")";
			}

			if (type.equals("FLOAT")  && precision != 0)
				return  "(" + precision + ")";

			//there are three format of decimal and numeric. Plain decimal, decimal(x)
			//and decimal(x,y). x is precision and y is scale.
			if (type.equals("DECIMAL") ||
				type.equals("NUMERIC")) 
			{
				if ( precision != 0 && scale == 0)
					return "(" + precision + ")";
				else if (precision != 0 && scale != 0)
					return "(" + precision + "," + scale + ")";
				else if(precision == 0 && scale!=0)
					return "(" + scale + ")";
			}

			if ((type.equals("DECIMAL") ||
				 type.equals("NUMERIC")) && scale != 0)
				return "(" + scale + ")";
        
			//no special type option
			return "";
	}

    /**
     * Get the column type names.
     */
    public String getColumnTypeNames() {
        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for (Object columnType : columnTypes) {
            if (!first)
                sb.append(", ");
            else
                first = false;
            sb.append(columnType);
        }
        return sb.toString();
    }

    /**
     * Get the column type names.
     */
    public String getImportAsColumns() {
        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for(int index = 0 ; index < columnTypes.size(); index++) {
            if (!first)
                sb.append(", ");
            else
                first = false;
            String name = (String) insertColumnNames.get(index);
            sb.append(IdUtil.normalToDelimited(name));
            sb.append(" ");
            sb.append(columnTypes.get(index));
        }
        return sb.toString();
    }

	// write a Serializable as a string
	public static String stringifyObject( Object udt ) throws Exception {
		DynamicByteArrayOutputStream dbaos = new DynamicByteArrayOutputStream();
		ObjectOutputStream oos = new ObjectOutputStream( dbaos );

		oos.writeObject( udt );

		byte[] buffer = dbaos.getByteArray();
		int length = dbaos.getUsed();

		return StringUtil.toHexString( buffer, 0, length );
	}


	/**
     * Get the class names of udt columns as a string.
     */
    public String getUDTClassNames() throws Exception {
        return stringifyObject( udtClassNames );
    }

	/* returns comma seperated column Names delimited by quotes for the insert
     * statement
	 * eg: "C1", "C2" , "C3" , "C4" 
	 */
	public String getInsertColumnNames()
	{
		StringBuilder sb = new StringBuilder();
		boolean first = true;
		for(int index = 0 ; index < noOfColumns; index++)
		{
			if(!first)
				sb.append(", ");
			else
				first = false;
            // Column names can be SQL reserved words, or they can contain
            // spaces and special characters, so it is necessary delimit them
            // for insert to work correctly.
            String name = (String) insertColumnNames.get(index);
            sb.append(IdUtil.normalToDelimited(name));
		}
	
		//there is no column info available
		if(first)
			return null;
		else
			return sb.toString();
	}

	//Return true if the given table exists in the database
	private boolean tableExists() throws SQLException
	{
		DatabaseMetaData dmd = conn.getMetaData();
		ResultSet rs = dmd.getTables(null, schemaName, tableName, null);
		boolean foundTable = false;
		if(rs.next())
		{
			//found the entry
			foundTable = true;
		}
		
		rs.close();
		return foundTable;
	}

    /*
     * Get the expected vti column type names. This information was 
     * passed earlier as a string to the vti. This routine extracts the 
     * information from the string.
     * @param columnTypeNamesString  import data column type information, encoded as string. 
     * @param noOfColumns     number of columns in the import file.
     * 
     * @see getColumnTypeNames()
     */
    public static String[] getExpectedColumnTypeNames
        ( String columnTypeNamesString, int noOfColumns )
        throws Exception
    {
        ArrayList list = (ArrayList) ImportAbstract.destringifyObject( columnTypeNamesString );

        String[] retval = new String[ list.size() ];

        list.toArray( retval );
        
        return retval;
    }

    /*
     * Get the expected classes bound to UDT columns. This information was 
     * passed earlier as a string to the vti. This routine extracts the 
     * information from the string.
     * @param stringVersion The result of calling toString() on the original HashMap<String><String>.
     * @return a HashMap<String><Class> mapping column names to their udt classes
     * 
     * @see initializeColumnInfo()
     */
    public static HashMap getExpectedUDTClasses( String stringVersion )
        throws Exception
    {
        // deserialize the original HashMap<String><String>
        HashMap stringMap = deserializeHashMap( stringVersion );

        if ( stringMap == null ) { return null; }
        
        HashMap retval = new HashMap();

		for (Object o : stringMap.entrySet()) {
			Map.Entry entry = (Map.Entry) o;
			String columnName = (String) entry.getKey();
			String className = (String) entry.getValue();

			Class classValue = Class.forName(className);

			retval.put(columnName, classValue);
		}

        return retval;
    }
    
    /*
     * Deserialize a HashMap produced by ExportAbstract.stringifyObject()
     */
    public static HashMap deserializeHashMap( String stringVersion )
        throws Exception
    {
        if ( stringVersion == null ) { return null; }

		return (HashMap) ImportAbstract.destringifyObject( stringVersion );
    }

    public ArrayList getImportColumns() {
    	return insertColumnNames;
	}
}





