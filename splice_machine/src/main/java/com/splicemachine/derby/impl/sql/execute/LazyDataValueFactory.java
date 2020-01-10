/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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

package com.splicemachine.derby.impl.sql.execute;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.StoredFormatIds;
import com.splicemachine.db.iapi.types.*;
import java.math.BigDecimal;

public class LazyDataValueFactory extends J2SEDataValueFactory{

        public NumberDataValue getDecimalDataValue(Long value, NumberDataValue previous) throws StandardException {
                if(previous != null && previous instanceof SQLDecimal){
                        previous.setValue(value);
                    }else{
                        previous = new SQLDecimal(BigDecimal.valueOf(value.longValue()));
                    }
        
                        return previous;
            }
    
                public NumberDataValue getDecimalDataValue(String value) throws StandardException {
                return new SQLDecimal(value);
            }
    
                public NumberDataValue getNullDecimal(NumberDataValue dataValue) {
                if(dataValue == null){
                        dataValue = new SQLDecimal();
                    }else{
                        dataValue.setToNull();
                    }
        
                        return dataValue;
            }
    
    public static DataValueDescriptor getLazyNull(int formatId) throws StandardException {
        switch (formatId) {
        /* Wrappers */
            case StoredFormatIds.SQL_BIT_ID: return new SQLBit();
            case StoredFormatIds.SQL_BOOLEAN_ID: return new SQLBoolean();
            case StoredFormatIds.SQL_CHAR_ID: return new SQLChar();
            case StoredFormatIds.SQL_DATE_ID: return new SQLDate();
            case StoredFormatIds.SQL_DOUBLE_ID: return new SQLDouble();
            case StoredFormatIds.SQL_DECIMAL_ID: return new SQLDecimal();
            case StoredFormatIds.SQL_INTEGER_ID: return new SQLInteger();
            case StoredFormatIds.SQL_LONGINT_ID: return new SQLLongint();
            case StoredFormatIds.SQL_REAL_ID: return new SQLReal();
            case StoredFormatIds.SQL_REF_ID: return new SQLRef();
            case StoredFormatIds.SQL_SMALLINT_ID: return new SQLSmallint();
            case StoredFormatIds.SQL_TIME_ID: return new SQLTime();
            case StoredFormatIds.SQL_TIMESTAMP_ID: return new SQLTimestamp();
            case StoredFormatIds.SQL_TINYINT_ID: return new SQLTinyint();
            case StoredFormatIds.SQL_VARCHAR_ID: return new SQLVarchar();
            case StoredFormatIds.SQL_LONGVARCHAR_ID: return new SQLLongvarchar();
            case StoredFormatIds.SQL_VARBIT_ID: return new SQLVarbit();
            case StoredFormatIds.SQL_LONGVARBIT_ID: return new SQLLongVarbit();
            case StoredFormatIds.SQL_USERTYPE_ID_V3: return new UserType();
            case StoredFormatIds.SQL_BLOB_ID: return new SQLBlob();
            case StoredFormatIds.SQL_CLOB_ID: return new SQLClob();
            case StoredFormatIds.XML_ID: return new XML();
            case StoredFormatIds.ACCESS_HEAP_ROW_LOCATION_V1_ID: return new HBaseRowLocation();
            default:
                throw new RuntimeException("No Data Descriptor for type=" + formatId);
        }
    }
}
