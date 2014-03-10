package com.splicemachine.derby.impl.load;

import com.google.common.base.Joiner;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.impl.sql.execute.operations.Sequence;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.derby.utils.ErrorState;
import com.splicemachine.derby.utils.Exceptions;
import com.splicemachine.hbase.writer.WriteResult;
import org.apache.commons.lang.time.FastDateFormat;
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
import java.util.GregorianCalendar;
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
    private String timestampFormatStr;
    private String dateFormatStr;
    private String timeFormatStr;
    private final HashMap<String,String> columnTimestampFormats;
		private Sequence[] sequences;
		private GregorianCalendar calendar;

		private final ImportErrorReporter errorReporter;

		public RowParser(ExecRow template,
                     String dateFormat,
                     String timeFormat,
                     String timestampFormat,
										 ImportErrorReporter errorReporter) {
				this.template = template;
				this.errorReporter = errorReporter;
				String ctxDateFormat = dateFormat;
				if(ctxDateFormat ==null)
						ctxDateFormat = "yyyy-MM-dd";
				this.dateFormatStr = ctxDateFormat;
				String ctxTimeFormat = timeFormat;
				if(ctxTimeFormat ==null)
						ctxTimeFormat = "HH:mm:ss";
				this.timeFormatStr = ctxTimeFormat;
				String ctxTimestampFormat = timestampFormat;
				if(ctxTimestampFormat ==null)
						ctxTimestampFormat = "yyy-MM-dd HH:mm:ss";
				this.timestampFormatStr = ctxTimestampFormat;
        columnTimestampFormats = new HashMap<String,String>();
        if(timestampFormat != null && timestampFormat.contains("@")) {
        	String[] tmp = timestampFormat.split("\\|");
						for (String aTmp : tmp) {
								int indexOfAt = aTmp.indexOf("@");
								columnTimestampFormats.put(aTmp.substring(indexOfAt + 1).trim(), aTmp.substring(0, indexOfAt).trim());
						}
        }
		}


    public RowParser(ExecRow template,
            ImportContext importContext,
						ImportErrorReporter errorReporter) {
				this(template,importContext.getDateFormat(),importContext.getTimeFormat(),importContext.getTimestampFormat(),errorReporter);

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
						try{
								setColumn(context, value,line);
						}catch(StandardException se){
								if(!errorReporter.reportError(join(line),WriteResult.failed(se.getMessage()))){
										if(errorReporter == FailAlwaysReporter.INSTANCE)
												throw se; //don't swallow the exception if we aren't recording errors
										else
												throw ErrorState.LANG_IMPORT_TOO_MANY_BAD_RECORDS.newException();
								}else{
										//skip line
										return null;
								}
						}
            pos++;
        }

        return template;
    }

		private void setColumn(ColumnContext columnContext, String elem,String[] row) throws StandardException {
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
				elem = elem.trim();
				if(elem.length()<=0){
						//if it's a date, treat "" as null by setting to null
						//otherwise, get it's default value
						switch(column.getTypeFormatId()){
								case StoredFormatIds.SQL_DATE_ID: //return new SQLDate();
								case StoredFormatIds.SQL_TIME_ID: //return new SQLTime();
								case StoredFormatIds.SQL_TIMESTAMP_ID: //return new SQLTimestamp();
										column.setToNull();
										return;
						}
						if(elem.length()==0) {
								elem = columnContext.getColumnDefault();
						}
				}
				switch(column.getTypeFormatId()){
						case StoredFormatIds.SQL_TINYINT_ID: //return new SQLTinyint();
						case StoredFormatIds.SQL_SMALLINT_ID: //return new SQLSmallint();
						case StoredFormatIds.SQL_INTEGER_ID: //return new SQLInteger();
								if(elem==null){
										column.setToNull();
										break;
								}
								try{
										int value = Integer.parseInt(elem);
										column.setValue(value);
										break;
								}catch(NumberFormatException nfe){
										throw ErrorState.LANG_FORMAT_EXCEPTION.newException(column.getTypeName());
								}
						case StoredFormatIds.SQL_BOOLEAN_ID: //return new SQLBoolean();
						case StoredFormatIds.SQL_LONGINT_ID: //return new SQLLongint();
						case StoredFormatIds.SQL_REAL_ID: //return new SQLReal();
						case StoredFormatIds.SQL_DOUBLE_ID: //return new SQLDouble();
						case StoredFormatIds.SQL_DECIMAL_ID:
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
								SimpleDateFormat format = getDateFormat(column, elem);
								try{
										if(format.toPattern().endsWith("Z") || format.toPattern().endsWith("X")){
												//if not append 00, cannot parse correctly
												elem = elem + "00";
										}
										Date value = format.parse(elem);
										if(calendar==null)
												calendar = new GregorianCalendar();

										column.setValue(new Timestamp(value.getTime()),calendar);
                }catch (ParseException p){
                    throw ErrorState.LANG_DATE_SYNTAX_EXCEPTION.newException();
                }
                break;
            default:
                throw new IllegalStateException("Unable to parse column type "+ column.getTypeName());
        }
        columnContext.validate(column);
    }

		/*Convenience method to re-join a failed row*/
		private Joiner joiner = null;
		private String join(String[] row) {
				if(joiner==null)
						joiner = Joiner.on(",");
				return joiner.join(row);
		}

		private SimpleDateFormat getDateFormat(DataValueDescriptor dvd, String elem) throws StandardException {
				SimpleDateFormat format;
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
						//this represents a programmer error, don't try and log this
						throw Exceptions.parseException(new IllegalStateException("Unable to determine date format for type " + dvd.getClass()));
				}

				return format;
		}
}