package com.splicemachine.derby.stream.function;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.List;

import com.splicemachine.db.iapi.types.DateTimeDataValue;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.supercsv.prefs.CsvPreference;

import com.splicemachine.EngineDriver;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.ArrayUtil;
import com.splicemachine.db.iapi.services.io.StoredFormatIds;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.shared.common.reference.SQLState;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.stream.output.WriteReadUtils;
import com.splicemachine.derby.utils.SpliceDateFunctions;

/**
 * Created by jleach on 10/30/15.
 */
public abstract class AbstractFileFunction<I> extends SpliceFlatMapFunction<SpliceOperation, I, LocatedRow> {
    CsvPreference preference = null;
    private static final char DEFAULT_COLUMN_DELIMITTER = ",".charAt(0);
    private static final char DEFAULT_STRIP_STRING = "\"".charAt(0);
    private String characterDelimiter;
    private String columnDelimiter;
    protected ExecRow execRow;
    private String timeFormat;
    private String dateTimeFormat;
    private String timestampFormat;
    private int[] columnIndex;

    private transient Calendar calendar;

    public AbstractFileFunction() {

    }

    @SuppressFBWarnings(value = "EI_EXPOSE_REP2",justification = "Intentional")
    public AbstractFileFunction(String characterDelimiter, String columnDelimiter, ExecRow execRow, int[] columnIndex, String timeFormat,
                        String dateTimeFormat, String timestampFormat, OperationContext operationContext) {
        super(operationContext);
        this.characterDelimiter = characterDelimiter;
        this.columnDelimiter = columnDelimiter;
        this.execRow = execRow;
        this.timeFormat = timeFormat;
        this.dateTimeFormat = dateTimeFormat;
        this.timestampFormat = timestampFormat;
        this.columnIndex = columnIndex;
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
        writeNullableUTF(out, dateTimeFormat);
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
            dateTimeFormat = in.readUTF();
        if (in.readBoolean())
            timestampFormat = in.readUTF();
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
    public LocatedRow call(List<String> values) throws Exception {
        operationContext.recordRead();
        try {
            ExecRow returnRow = execRow.getClone();
            for (int i = 1; i <= returnRow.nColumns(); i++) {
                DataValueDescriptor dvd = returnRow.getColumn(i);
                int type = dvd.getTypeFormatId();
                if (values.size() <= i-1) {
                    throw StandardException.newException(SQLState.COLUMN_NUMBER_MISMATCH, execRow.nColumns(), values.size());
                }
                String value = values.get(i - 1);
                if (value != null && (value.equals("null") || value.equals("NULL") || value.isEmpty()))
                    value = null;
                if (type == StoredFormatIds.SQL_TIME_ID) {
                    if(calendar==null)
                        calendar = new GregorianCalendar();
                    if (timeFormat == null || value==null){
                        ((DateTimeDataValue)dvd).setValue(value,calendar);
                    }else
                        dvd.setValue(SpliceDateFunctions.TO_TIME(value, timeFormat),calendar);
                } else if (type == StoredFormatIds.SQL_TIMESTAMP_ID) {
                    if(calendar==null)
                        calendar = new GregorianCalendar();
                    if (timestampFormat == null || value==null)
                        ((DateTimeDataValue)dvd).setValue(value,calendar);
                    else
                        dvd.setValue(SpliceDateFunctions.TO_TIMESTAMP(value, timestampFormat),calendar);
                } else if (type == StoredFormatIds.SQL_DATE_ID) {
                    if(calendar==null)
                        calendar = new GregorianCalendar();
                    if (dateTimeFormat == null || value == null)
                        ((DateTimeDataValue)dvd).setValue(value,calendar);
                    else
                        dvd.setValue(SpliceDateFunctions.TO_DATE(value, dateTimeFormat),calendar);
                } else {
                    dvd.setValue(value);
                }
            }
            return new LocatedRow(returnRow);
        } catch (Exception e) {
            if (operationContext.isPermissive()) {
               operationContext.recordBadRecord(e.getLocalizedMessage() + values, e);
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
                    characterDelimiter!=null && characterDelimiter.length()>0?characterDelimiter.charAt(0):DEFAULT_STRIP_STRING,
                    columnDelimiter!=null && columnDelimiter.length()>0?columnDelimiter.charAt(0):DEFAULT_COLUMN_DELIMITTER,
                    "\n",
                    maxQuotedLines).useNullForEmptyColumns(false).build();
        }
    }




}
