package com.splicemachine.derby.impl.load;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.impl.sql.execute.operations.Sequence;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.derby.utils.ErrorState;
import com.splicemachine.derby.utils.Exceptions;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.SQLDate;
import org.apache.derby.iapi.types.SQLTime;
import org.apache.derby.iapi.types.SQLTimestamp;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;

/**
 * @author Scott Fines
 * Created on: 9/26/13
 */
public class RowParser {
    private final ExecRow template;

    private SimpleDateFormat timestampFormat;
    private SimpleDateFormat dateFormat;
    private SimpleDateFormat timeFormat;
    private final String timestampFormatStr;
    private final String dateFormatStr;
    private final String timeFormatStr;
    private final HashMap<String,String> columnTimestampFormats;
    private final ImportContext importContext;
    private Sequence[] sequences;

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
        
        this.timestampFormatStr = timestampFormat;   
        columnTimestampFormats = new HashMap<String,String>();
        if(timestampFormat != null && timestampFormat.contains("@")) {
        	String[] tmp = timestampFormat.split("\\|");
        	for (int i = 0; i < tmp.length; i++) {
        		int indexOfAt = tmp[i].indexOf("@");
        		columnTimestampFormats.put(tmp[i].substring(indexOfAt + 1).trim(), tmp[i].substring(0, indexOfAt).trim());
        	}
        }
        this.importContext = null;
            
    }
    
    public RowParser(ExecRow template,
            String dateFormat,
            String timeFormat,
            String timestampFormat,
            ImportContext importContext) {
    	this.template = template;
    	if(dateFormat==null)
    		dateFormat = "yyyy-MM-dd";
    	this.dateFormatStr = dateFormat;
    	if(timeFormat==null)
    		timeFormat = "HH:mm:ss";
    	this.timeFormatStr = timeFormat;

    	this.timestampFormatStr = timestampFormat;   
    	columnTimestampFormats = new HashMap<String,String>();
    	if(timestampFormat != null && timestampFormat.contains("@")) {
    		String[] tmp = timestampFormat.split("\\|");
    		for (int i = 0; i < tmp.length; i++) {
    			int indexOfAt = tmp[i].indexOf("@");
    			columnTimestampFormats.put(tmp[i].substring(indexOfAt + 1).trim(), tmp[i].substring(0, indexOfAt).trim());
    		}
    	}
				this.importContext = importContext;
				ColumnContext[] columnInformation = importContext.getColumnInformation();
				this.sequences = new Sequence[columnInformation.length];
				for(int i=0;i< columnInformation.length;i++){
						ColumnContext cc = columnInformation[i];
					if(columnInformation[i].isAutoIncrement()){
							sequences[i] = new Sequence(SpliceAccessManager.getHTable(SpliceConstants.SEQUENCE_TABLE_NAME_BYTES),
											50*cc.getAutoIncrementIncrement(),
											cc.getSequenceRowLocation(),
											cc.getAutoIncrementStart(),
											cc.getAutoIncrementIncrement());
					}
				}
		}

    public ExecRow process(String[] line, ColumnContext[] columnContexts) throws StandardException {
        template.resetRowArray();
        
        int pos=0;
        for(ColumnContext context:columnContexts){
            String value = pos>=line.length ||line[pos]==null||line[pos].length()==0?null: line[pos];
            if (timestampFormatStr != null && timestampFormatStr.contains("@") && context.getColumnType() == 93 && context.getColumnNumber() == pos) {
            	String tmpstr = columnTimestampFormats.get(String.valueOf(context.getColumnNumber()+1));
            	if (tmpstr.equals("null") || tmpstr == null)
            	    context.setFormatStr(null);
            	else
            		context.setFormatStr(tmpstr);
            }
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
						if(columnContext.isAutoIncrement())
								column.setValue(sequences[columnContext.getColumnNumber()].getNext());
						else{
								elem = columnContext.getColumnDefault();
								column.setValue(elem);
						}
						columnContext.validate(column);
						return;
				}else if(columnContext.isAutoIncrement()){
						throw ErrorState.LANG_AI_CANNOT_MODIFY_AI.newException(columnContext.getColumnName());
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
                //default formats supported, can be expanded on demand later
                String timestampFormatStr2 = "yyyy-MM-dd hh:mm:ss";
                
                if(column instanceof SQLTimestamp){
                	if (elem.matches("^\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}[-,+]\\d{2}")) {
                		timestampFormatStr2 = "yyyy-MM-dd HH:mm:ssZ";      		
                    }
                	if (elem.matches("^\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}.\\d{2}[-,+]\\d{2}")) {
                		timestampFormatStr2 = "yyyy-MM-dd HH:mm:ss.SSZ";
                    }
                    if (elem.matches("^\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}.\\d{3}[-,+]\\d{2}")) {
                    	timestampFormatStr2 = "yyyy-MM-dd HH:mm:ss.SSSZ";
                    }
                }
                SimpleDateFormat format = getDateFormat(columnContext, column, timestampFormatStr2);
                try{                   	
                	if(format.toPattern().endsWith("Z") || format.toPattern().endsWith("X"))
                		//if not append 00, cannot parse correctly
                		elem = elem + "00";
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

    private SimpleDateFormat getDateFormat(ColumnContext columnContext, DataValueDescriptor dvd, String tsfm) throws StandardException {
        SimpleDateFormat format;
        if(dvd instanceof SQLTimestamp){
        	if(timestampFormatStr == null) {
        		timestampFormat = new SimpleDateFormat(tsfm);
        	} else {
        		if(columnContext.isFormatStrSet()) {
        			if(columnContext.getFormatStr() != null)
        			    timestampFormat = new SimpleDateFormat(columnContext.getFormatStr());
        			else 
        				timestampFormat = new SimpleDateFormat(tsfm);
        		} else {
                    timestampFormat = new SimpleDateFormat(timestampFormatStr);
        		}
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