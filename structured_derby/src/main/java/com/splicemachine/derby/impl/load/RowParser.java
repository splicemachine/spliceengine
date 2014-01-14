package com.splicemachine.derby.impl.load;

import com.splicemachine.derby.utils.ErrorState;
import com.splicemachine.derby.utils.Exceptions;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.types.*;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author Scott Fines
 * Created on: 9/26/13
 */
public class RowParser {
    private final ExecRow template;

    private DateFormat timestampFormat;
    private DateFormat dateFormat;
    private DateFormat timeFormat;

    private final String timestampFormatStr;
    private final String dateFormatStr;
    private final String timeFormatStr;

    public RowParser(ExecRow template,
                     String dateFormat,
                     String timeFormat,
                     String timestampFormat) {
        this.template = template;
        if(dateFormat==null)
            dateFormat = "yyyy-MM-dd";
        this.dateFormatStr = dateFormat;
        if(timeFormat==null)
            timeFormat = "HH:mm:ss";
        this.timeFormatStr = timeFormat;
        if(timestampFormat ==null)
            timestampFormat = "yyyy-MM-dd hh:mm:ss"; //iso format

        this.timestampFormatStr = timestampFormat;
    }

    public ExecRow process(String[] line, ColumnContext[] columnContexts) throws StandardException {
        template.resetRowArray();

        int pos=0;
        for(ColumnContext context:columnContexts){
            String value = pos>=line.length ||line[pos]==null||line[pos].length()==0?null: line[pos];
            setColumn(context, value);
            pos++;
        }

        return template;
    }

    private void setColumn(ColumnContext columnContext, String elem) throws StandardException {
        if(elem==null||elem.length()==0)
            elem=null;
        DataValueDescriptor column = template.getColumn(columnContext.getColumnNumber() + 1);
        if(elem==null){
        	elem = columnContext.getColumnDefault();
        	column.setValue(elem);
            columnContext.validate(column);
            return;
        }
        switch(column.getTypeFormatId()){
            case StoredFormatIds.SQL_BOOLEAN_ID: //return new SQLBoolean();
            case StoredFormatIds.SQL_TINYINT_ID: //return new SQLTinyint();
            case StoredFormatIds.SQL_SMALLINT_ID: //return new SQLSmallint();
            case StoredFormatIds.SQL_INTEGER_ID: //return new SQLInteger();
            case StoredFormatIds.SQL_LONGINT_ID: //return new SQLLongint();
            case StoredFormatIds.SQL_REAL_ID: //return new SQLReal();
            case StoredFormatIds.SQL_DOUBLE_ID: //return new SQLDouble();
            case StoredFormatIds.SQL_DECIMAL_ID:
                //treat empty strings as null
                elem = elem.trim();
                if(elem.length()==0) {
                	elem = columnContext.getColumnDefault();
                }
            case StoredFormatIds.SQL_VARCHAR_ID: //return new SQLVarchar();
            case StoredFormatIds.SQL_LONGVARCHAR_ID: //return new SQLLongvarchar();
            case StoredFormatIds.SQL_CLOB_ID: //return new SQLClob();
            case StoredFormatIds.XML_ID: //return new XML();
            case StoredFormatIds.SQL_CHAR_ID: //return new SQLChar();
            case StoredFormatIds.SQL_VARBIT_ID: //return new SQLVarbit();
            case StoredFormatIds.SQL_LONGVARBIT_ID: //return new SQLLongVarbit();
            case StoredFormatIds.SQL_BLOB_ID: //return new SQLBlob();
            case StoredFormatIds.ACCESS_HEAP_ROW_LOCATION_V1_ID:
            case StoredFormatIds.SQL_BIT_ID: //return new SQLBit();
                column.setValue(elem);
                break;
            case StoredFormatIds.SQL_DATE_ID: //return new SQLDate();
            case StoredFormatIds.SQL_TIME_ID: //return new SQLTime();
            case StoredFormatIds.SQL_TIMESTAMP_ID: //return new SQLTimestamp();
                elem = elem.trim();
                if(elem.length()<=0){
                    column.setToNull();
                    break;
                }

                DateFormat format = getDateFormat(column);
                try{
                    Date value = format.parse(elem);
                    column.setValue(new Timestamp(value.getTime()));
                }catch (ParseException p){
                    throw ErrorState.LANG_DATE_SYNTAX_EXCEPTION.newException();
                }
                break;
            default:
                throw new IllegalStateException("Unable to parse column type "+ column.getTypeName());
        }
        columnContext.validate(column);
    }

    private DateFormat getDateFormat(DataValueDescriptor dvd) throws StandardException {
        DateFormat format;
        if(dvd instanceof SQLTimestamp){
            if(timestampFormat==null){
                timestampFormat = new SimpleDateFormat(timestampFormatStr);
            }
            format = timestampFormat;
        }else if(dvd instanceof SQLDate){
            if(dateFormat==null){
                dateFormat = new SimpleDateFormat(dateFormatStr);
            }
            format = dateFormat;
        }else if(dvd instanceof SQLTime){
            if(timeFormat==null){
                timeFormat = new SimpleDateFormat(timeFormatStr);
            }
            format = timeFormat;
        }else{
            throw Exceptions.parseException(new IllegalStateException("Unable to determine date format for type " + dvd.getClass()));
        }
        return format;
    }
}
