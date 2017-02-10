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
 * All such Splice Machine modifications are Copyright 2012 - 2017 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.db.impl.drda;

import java.sql.Types;
import java.sql.SQLException;
import com.splicemachine.db.iapi.reference.DRDAConstants;



class SQLTypes {

  // define final statics for the fdoca type codes here!!!

  // hide the default constructor
  private SQLTypes() {}


  /**
   * Map DB2 SQL Type to JDBC Type
   * 
   * @param sqlType SQL Type to convert
   * @param length storage length of type
   * @param ccsid ccsid of type
   *
   * @return Corresponding JDBC Type 
   */

  static protected int mapDB2SqlTypeToJdbcType (int sqlType, long length, int ccsid)
  {
    switch (getNonNullableSqlType (sqlType)) { // mask the isNullable bit
    case DRDAConstants.DB2_SQLTYPE_SMALL:
      return java.sql.Types.SMALLINT;
    case DRDAConstants.DB2_SQLTYPE_INTEGER:
      return java.sql.Types.INTEGER;
    case DRDAConstants.DB2_SQLTYPE_BIGINT:
      return java.sql.Types.BIGINT;
    case DRDAConstants.DB2_SQLTYPE_FLOAT:
      if (length == 16)                  // can map to either NUMERIC or DECIMAL!!! @sxg
        return java.sql.Types.DECIMAL;
      else if (length == 8)              // can map to either DOUBLE or FLOAT!!! @sxg
        return java.sql.Types.DOUBLE;
      else if (length == 4)
        return java.sql.Types.REAL;
      else
        return 0;
        //throw new BugCheckException ("Encountered unexpected float length");
    case DRDAConstants.DB2_SQLTYPE_DECIMAL:            // can map to either NUMERIC or DECIMAL!!! @sxg
    case DRDAConstants.DB2_SQLTYPE_ZONED:              // can map to either NUMERIC or DECIMAL!!! @sxg
    case DRDAConstants.DB2_SQLTYPE_NUMERIC:            // can map to either NUMERIC or DECIMAL!!! @sxg
      return java.sql.Types.DECIMAL;
    case DRDAConstants.DB2_SQLTYPE_CHAR:    // mixed and single byte
      if (ccsid == 0xffff || ccsid == 0) // we think UW returns 0, and 390 returns 0xffff, doublecheck !!!
        return java.sql.Types.BINARY;
      else
        return java.sql.Types.CHAR;
    case DRDAConstants.DB2_SQLTYPE_CSTR:    // SBCS null terminated 
    case DRDAConstants.DB2_SQLTYPE_GRAPHIC: // fixed character DBCS
      return java.sql.Types.CHAR;
    // use ccsid to distinguish between BINARY and CHAR, VARBINARY and VARCHAR, LONG... !!! -j/p/s
    case DRDAConstants.DB2_SQLTYPE_VARGRAPH:  // variable character DBCS
    case DRDAConstants.DB2_SQLTYPE_VARCHAR:   // variable character SBCS/Mixed
      if (ccsid == 0xffff || ccsid == 0) // we think UW returns 0, and 390 returns 0xffff, doublecheck !!!
        return java.sql.Types.VARBINARY;
      else
        return java.sql.Types.VARCHAR;
    case DRDAConstants.DB2_SQLTYPE_LSTR:      // pascal string SBCS/Mixed
      return java.sql.Types.VARCHAR;
    case DRDAConstants.DB2_SQLTYPE_LONGRAPH:  // long varchar DBCS
    case DRDAConstants.DB2_SQLTYPE_LONG:      // long varchar SBCS/Mixed
      if (ccsid == 0xffff || ccsid == 0) // we think UW returns 0, and 390 returns 0xffff, doublecheck !!!
        return java.sql.Types.LONGVARBINARY;
      else
        return java.sql.Types.LONGVARCHAR;
    case DRDAConstants.DB2_SQLTYPE_DATE:
      return java.sql.Types.DATE;
    case DRDAConstants.DB2_SQLTYPE_TIME:
      return java.sql.Types.TIME;
    case DRDAConstants.DB2_SQLTYPE_TIMESTAMP:
      return java.sql.Types.TIMESTAMP;
    case DRDAConstants.DB2_SQLTYPE_CLOB:    // large object character SBCS/Mixed
    case DRDAConstants.DB2_SQLTYPE_DBCLOB:  // large object character DBCS
      return java.sql.Types.CLOB;
    case DRDAConstants.DB2_SQLTYPE_BLOB:    // large object bytes
		case DRDAConstants.DB2_SQLTYPE_BLOB_LOCATOR:
		case DRDAConstants.DB2_SQLTYPE_CLOB_LOCATOR:
		case DRDAConstants.DB2_SQLTYPE_DBCLOB_LOCATOR:
      return java.sql.Types.BLOB;
    default:
      //throw new BugCheckException ("Encountered unexpected type code");
      return 0;
    }
  }


	/**
	 * Map jdbc type to the DB2 DRDA SQL Types expected by jcc.
	 *@param jdbctype  - jdbc Type to convert
	 *@param nullable - whether the type is nullable
	 **/

	
 /**  Map JDBC Type to DB2 SqlType
  * @param jdbctype   JDBC Type from java.sql.Types
  * @param nullable   true if this is a nullable type
	 * @param appRequester - state variable for the connection
  * @param outlen     output parameter with type length
  *
  * @return Corresponding DB2 SQL Type (See DRDA Manual FD:OCA Meta 
  *          Data Summary, page 245)
  * 
  * @exception SQLException thrown for unrecognized SQLType
  */

    static protected int mapJdbcTypeToDB2SqlType (int jdbctype, boolean nullable, AppRequester appRequester,
											   int[] outlen)
	 throws SQLException
  {
	  int nullAddVal =0;

	  if (nullable) 
		  nullAddVal =1; 
	  
	  // Call FdocaConstants just to get the length
	  FdocaConstants.mapJdbcTypeToDrdaType(jdbctype,nullable,appRequester,outlen);

	  switch(jdbctype)
	  {
		  case Types.BOOLEAN:
              if ( appRequester.supportsBooleanValues() )
              {
                  return DRDAConstants.DB2_SQLTYPE_BOOLEAN + nullAddVal;
              }
              else
              {
                  return DRDAConstants.DB2_SQLTYPE_SMALL + nullAddVal;
              }
		  case java.sql.Types.BIT:
		  case java.sql.Types.TINYINT:
		  case java.sql.Types.SMALLINT:
			  return DRDAConstants.DB2_SQLTYPE_SMALL + nullAddVal;
		  case java.sql.Types.INTEGER:
			  return DRDAConstants.DB2_SQLTYPE_INTEGER + nullAddVal;
		  case java.sql.Types.BIGINT:
			  return DRDAConstants.DB2_SQLTYPE_BIGINT + nullAddVal;
		  case java.sql.Types.DOUBLE:
		  case java.sql.Types.REAL:
			  return DRDAConstants.DB2_SQLTYPE_FLOAT + nullAddVal;
		  case java.sql.Types.DECIMAL:
		  case java.sql.Types.NUMERIC:
			  return DRDAConstants.DB2_SQLTYPE_DECIMAL + nullAddVal;
		  case java.sql.Types.DATE:
			  return DRDAConstants.DB2_SQLTYPE_DATE + nullAddVal;
		  case java.sql.Types.TIME:
			  return DRDAConstants.DB2_SQLTYPE_TIME + nullAddVal;
		  case java.sql.Types.TIMESTAMP:
			  return DRDAConstants.DB2_SQLTYPE_TIMESTAMP + nullAddVal;
		  case java.sql.Types.CHAR:
			  return  DRDAConstants.DB2_SQLTYPE_CHAR + nullAddVal;    // null terminated SBCS/Mixed
		  case java.sql.Types.BINARY:
			  return DRDAConstants.DB2_SQLTYPE_CHAR + nullAddVal;

		  case java.sql.Types.VARCHAR:
		  case java.sql.Types.VARBINARY:
			  return  DRDAConstants.DB2_SQLTYPE_VARCHAR + nullAddVal;			  
		  case java.sql.Types.LONGVARBINARY:
			  return DRDAConstants.DB2_SQLTYPE_LONG + nullAddVal;
		  case java.sql.Types.JAVA_OBJECT:
              if ( appRequester.supportsUDTs() )
              {
                  return DRDAConstants.DB2_SQLTYPE_FAKE_UDT + nullAddVal;
              }
              else
              {
                  return DRDAConstants.DB2_SQLTYPE_LONG + nullAddVal;
              }
		  case java.sql.Types.BLOB:
			  return DRDAConstants.DB2_SQLTYPE_BLOB + nullAddVal;
		  case java.sql.Types.CLOB:
			  return DRDAConstants.DB2_SQLTYPE_CLOB + nullAddVal;
		  case java.sql.Types.LONGVARCHAR:
			  return DRDAConstants.DB2_SQLTYPE_LONG + nullAddVal;

          case java.sql.Types.REF:
              return DRDAConstants.DB2_SQLTYPE_ROWID + nullAddVal;
		  case java.sql.Types.ARRAY:
		  case java.sql.Types.DISTINCT:
		  case java.sql.Types.NULL:
		  case java.sql.Types.OTHER:
		  case java.sql.Types.STRUCT:
			  throw new SQLException("Jdbc type" + jdbctype + "not Supported yet");
			default:
				throw new SQLException ("unrecognized sql type: " + jdbctype);
			  //throw new BugCheckException ("Encountered unexpected type code");

	  }
  }

	/**
	 * Translate DB2 SQL Type to the non-nullable type.
	 * @param sqlType DB2 SQL Type
	 *
	 * @return The Non-Nullable DB2 SQL Type.
	 */
   protected  static int getNonNullableSqlType (int sqlType)
  {
    return sqlType & ~1;
  }


}
