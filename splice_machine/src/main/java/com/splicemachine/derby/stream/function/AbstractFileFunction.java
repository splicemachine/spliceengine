/*
 * Copyright (c) 2012 - 2019 Splice Machine, Inc.
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

package com.splicemachine.derby.stream.function;

import com.splicemachine.EngineDriver;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.ArrayUtil;
import com.splicemachine.db.iapi.services.io.StoredFormatIds;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.DateTimeDataValue;
import com.splicemachine.db.iapi.types.SQLTimestamp;
import com.splicemachine.db.shared.common.reference.SQLState;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.InsertOperation;
import com.splicemachine.derby.impl.sql.execute.operations.VTIOperation;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.stream.output.WriteReadUtils;
import com.splicemachine.derby.stream.utils.BooleanList;
import com.splicemachine.derby.utils.SpliceDateTimeFormatter;
import com.splicemachine.derby.utils.SpliceDateFunctions;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.supercsv.prefs.CsvPreference;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.List;

import static com.splicemachine.derby.utils.SpliceDateFunctions.TO_DATE;
import static com.splicemachine.derby.utils.SpliceDateTimeFormatter.*;

/**
 *
 * Created by jleach on 10/30/15.
 */
@SuppressWarnings("WeakerAccess") //weaker access isn't allowed because we have to be serializable
public abstract class AbstractFileFunction<I> extends SpliceFlatMapFunction<SpliceOperation, I, ExecRow> {
    CsvPreference preference = null;
    private static final char DEFAULT_COLUMN_DELIMITTER = ",".charAt(0);
    private static final char DEFAULT_STRIP_STRING = "\"".charAt(0);
    private String characterDelimiter;
    private String columnDelimiter;
    protected ExecRow execRow;
    private String timeFormat;
    private String dateFormat;
    private SpliceDateTimeFormatter dateFormatter;
    private SpliceDateTimeFormatter timestampFormatter;
    private SpliceDateTimeFormatter timeFormatter;
    private String timestampFormat;
    private int[] columnIndex;

    private transient Calendar calendar = new GregorianCalendar();

    @SuppressWarnings("WeakerAccess") //weaker access isn't allowed because we have to be serializable
    public AbstractFileFunction() { }

    private void setupFormatters() {
        // Create a date formatter, time formatter and timestamp formatter
        // that can be reused, to save memory, instead of
        // constructing a new one every to TO_DATE , TO_TIME or TO_TIMESTAMP is called.

        this.dateFormatter      = (dateFormat == null ||
                                   dateFormat.equals(defaultDateFormatString)) ?
        DEFAULT_DATE_FORMATTER :
                                  new SpliceDateTimeFormatter(dateFormat,
                                       SpliceDateTimeFormatter.FormatterType.DATE);
        this.timestampFormatter = (timestampFormat == null ||
                                   timestampFormat.equals(defaultTimestampFormatString)) ?
        DEFAULT_TIMESTAMP_FORMATTER :
                                  new SpliceDateTimeFormatter(timestampFormat,
                                       SpliceDateTimeFormatter.FormatterType.TIMESTAMP);
        this.timeFormatter      = (timeFormat == null ||
                                   timeFormat.equals(defaultTimeFormatString)) ?
        DEFAULT_TIME_FORMATTER :
                                  new SpliceDateTimeFormatter(timeFormat,
                                       SpliceDateTimeFormatter.FormatterType.TIME);
    }

    @SuppressWarnings("unchecked")
    @SuppressFBWarnings(value = "EI_EXPOSE_REP2",justification = "Intentional")
    AbstractFileFunction(String characterDelimiter, String columnDelimiter, ExecRow execRow, int[] columnIndex, String timeFormat,
                         String dateFormat, String timestampFormat, OperationContext operationContext) {
        super(operationContext);
        this.characterDelimiter = characterDelimiter;
        this.columnDelimiter = columnDelimiter;
        this.execRow = execRow;
        this.timeFormat = timeFormat;
        this.dateFormat = dateFormat;
        this.timestampFormat = timestampFormat;
        this.columnIndex = columnIndex;

        setupFormatters();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeBoolean(characterDelimiter!=null);
        if (characterDelimiter!=null)
            out.writeUTF(characterDelimiter);
        out.writeBoolean(columnDelimiter!=null);
        if (columnDelimiter!=null)
            out.writeUTF(columnDelimiter);
        writeNullableUTF(out, timeFormat);
        writeNullableUTF(out, dateFormat);
        writeNullableUTF(out,timestampFormat);
        try {
            ArrayUtil.writeIntArray(out, WriteReadUtils.getExecRowTypeFormatIds(execRow));
        } catch (StandardException se) {
            throw new IOException(se);
        }
        out.writeBoolean(columnIndex!=null);
        if (columnIndex!=null)
            ArrayUtil.writeIntArray(out,columnIndex);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        if (in.readBoolean())
            characterDelimiter = in.readUTF();
        if (in.readBoolean())
            columnDelimiter = in.readUTF();
        if (in.readBoolean())
            timeFormat = in.readUTF();
        if (in.readBoolean())
            dateFormat = in.readUTF();
        if (in.readBoolean())
            timestampFormat = in.readUTF();
        setupFormatters();
        execRow =WriteReadUtils.getExecRowFromTypeFormatIds(ArrayUtil.readIntArray(in));
        if (in.readBoolean())
            columnIndex = ArrayUtil.readIntArray(in);
    }

    private void writeNullableUTF(ObjectOutput out, String source) throws IOException {
        out.writeBoolean(source!=null);
        if (source!=null)
            out.writeUTF(source);
    }

    @SuppressFBWarnings(value = "REC_CATCH_EXCEPTION",justification = "Intentional")
    public ExecRow call(List<String> values,BooleanList quotedColumns) throws Exception {
        return getRow(values, quotedColumns, operationContext, execRow, calendar, timeFormat,
        dateFormat, timestampFormat, this.dateFormatter, this.timestampFormatter, this.timeFormatter);
    }


    public static ExecRow getRow(List<String> values,BooleanList quotedColumns,
                                 OperationContext operationContext, ExecRow execRow,
                                 Calendar calendar, String timeFormat,
                                 String dateTimeFormat, String timestampFormat,
                                 SpliceDateTimeFormatter dateFormatter,
                                 SpliceDateTimeFormatter timestampFormatter,
                                 SpliceDateTimeFormatter timeFormatter)  throws Exception {
        int columnID = 0;
        String columnValue = null;
        int numofColumnsinTable = 0;
        int numofColumnsinFile = 0;
        boolean columnnumbermistmatch = false;
        boolean convertTimestamps = false;

        if (operationContext != null)
            operationContext.recordRead();

        try {
            ExecRow returnRow = execRow.getClone();
            if (values == null) {
                columnnumbermistmatch = true;
                throw StandardException.newException(SQLState.COLUMN_NUMBER_MISMATCH, execRow.nColumns(), 0);
            }
            else if (values.size() < returnRow.nColumns()) {
                columnnumbermistmatch = true;
                throw StandardException.newException(SQLState.COLUMN_NUMBER_MISMATCH, returnRow.nColumns(), values.size());
            }

            DataTypeDescriptor[] dataTypeDescriptors = null;
            if (operationContext != null && operationContext.getOperation() instanceof VTIOperation) {
                VTIOperation op = (VTIOperation) operationContext.getOperation();
                dataTypeDescriptors = op.getResultColumnTypes();

                if (op.isConvertTimestampsEnabled() &&
                    op.getActivation().getResultSet() != null &&
                    op.getActivation().getResultSet() instanceof InsertOperation) {

                    InsertOperation insOp = (InsertOperation)op.getActivation().getResultSet();
                    String tableVersion = insOp.getTableVersion();
                    if (tableVersion.equals("2.0"))
                        convertTimestamps = true;
                }
            }

            numofColumnsinTable = returnRow.nColumns();
            numofColumnsinFile = values.size();
            for (int i = 1; i <= returnRow.nColumns(); i++) {
                DataValueDescriptor dvd = returnRow.getColumn(i);
                columnID = i;
                int type = dvd.getTypeFormatId();

                String value = values.get(i - 1);
                if (shouldBeNull(value,quotedColumns.valueAt(i-1)))
                    value = null;
                columnValue = value;
                switch(type){
                    case StoredFormatIds.SQL_TIME_ID:
                        if (timeFormat == null || value==null){
                            ((DateTimeDataValue)dvd).setValue(value,calendar);
                        }else
                            dvd.setValue(SpliceDateFunctions.TO_TIME(value, timeFormat, timeFormatter), calendar);
                        break;
                    case StoredFormatIds.SQL_DATE_ID:
                        if (dateTimeFormat == null || value == null)
                            ((DateTimeDataValue)dvd).setValue(value,calendar);
                        else
                            dvd.setValue(TO_DATE(value, dateTimeFormat, dateFormatter),calendar);
                        break;
                    case StoredFormatIds.SQL_TIMESTAMP_ID:
                        if (timestampFormat == null || value==null)
                            ((DateTimeDataValue)dvd).setValue(value,calendar);
                        else {
                            Timestamp ts = SpliceDateFunctions.TO_TIMESTAMP(value, timestampFormat, timestampFormatter);
                            if (convertTimestamps)
                                ts = SQLTimestamp.convertTimeStamp(ts);
                            dvd.setValue(ts, calendar);
                        }
                        break;
                    case StoredFormatIds.SQL_CHAR_ID:
                    case StoredFormatIds.SQL_VARCHAR_ID:
                    case StoredFormatIds.SQL_CLOB_ID:
                        dvd.setValue(value);
                        //normalize the char type
                        if(dataTypeDescriptors != null && !dvd.isNull()){
                            dvd.normalize(dataTypeDescriptors[i-1], dvd);
                        }
                        break;
                    default:
                        dvd.setValue(value);
                }
            }
            return returnRow;
        } catch (Exception e) {
            if (operationContext != null && operationContext.isPermissive()) {
                String extendedMessage;
                if (columnnumbermistmatch)
                    extendedMessage = " row Data: " + values;
                else
                    extendedMessage = " [Columns in Table: " + numofColumnsinTable + "] [Columns in File: " + numofColumnsinFile + "] [Bad Column ID: " + columnID + "] "+ "[Bad Column Value: " + columnValue + "]" + " row Data: " + values;
                operationContext.recordBadRecord(e.getLocalizedMessage() + extendedMessage, e);
                return null;
            }
            throw e; // Not Permissive of errors
        }
    }

    void checkPreference() {
        if (preference==null){
            SConfiguration config =EngineDriver.driver().getConfiguration();
            int maxQuotedLines = config.getImportMaxQuotedColumnLines();
            preference=new CsvPreference.Builder(
                    characterDelimiter!=null && !characterDelimiter.isEmpty() ?characterDelimiter.charAt(0):DEFAULT_STRIP_STRING,
                    columnDelimiter!=null && !columnDelimiter.isEmpty() ?columnDelimiter.charAt(0):DEFAULT_COLUMN_DELIMITTER,
                    "\n").maxLinesPerRow(maxQuotedLines).build();
        }
    }

    /* ****************************************************************************************************************/
    /*private helper methods*/
    @SuppressWarnings("SimplifiableIfStatement") //the logic is clearer this way, without a performance penalty
    private static boolean shouldBeNull(String value,boolean wasQuoted){
        if(value==null) return true;
        else if(wasQuoted) return false;
        else return value.isEmpty() || value.equalsIgnoreCase("null");
    }



}
