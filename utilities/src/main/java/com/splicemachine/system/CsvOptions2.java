package com.splicemachine.system;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * these options are mainly used for SPLITKEYS, but also for SpliceFileVTI, controlling the options of e.g.
 * SYSCS_UTIL.IMPORT_DATA .
 * It differs from CsvOptions as it also has characterDelimiter, timestamp, date and time format, while
 * CsvOptions also has escapeCharacter.
 * We should combine CsvOptions and CsvOptions2, but should avoid introducing problems because we set a value and it's not used.
 */
public class CsvOptions2 {
    public String columnDelimiter = null;    // character used to separate columns in the CSV file (default = ,)
    public String characterDelimiter = null; // character used to delimit strings in the CSV file ( default = ")
    public String timestampFormat = null;
    public String dateFormat = null;
    public String timeFormat = null;

    public CsvOptions2() {}
    public CsvOptions2(ObjectInput in) throws IOException {
        readExternal(in);
    }
    public CsvOptions2(String columnDelimiter,
                       String characterDelimiter,
                       String timestampFormat,
                       String dateFormat,
                       String timeFormat)
    {
        this.columnDelimiter = columnDelimiter;
        this.characterDelimiter = characterDelimiter;
        this.timestampFormat = timestampFormat;
        this.dateFormat = dateFormat;
        this.timeFormat = timeFormat;
    }
    private void writeEx(ObjectOutput out, String s) throws IOException
    {
        out.writeBoolean(s != null);
        if ( s != null )
            out.writeUTF(s);
    }

    private String readExString(ObjectInput in) throws IOException {
        return in.readBoolean() ? in.readUTF() : null;
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        writeEx(out, columnDelimiter);
        writeEx(out, characterDelimiter);
        writeEx(out, timestampFormat);
        writeEx(out, dateFormat);
        writeEx(out, timeFormat);

        // todo: others
    }
    public void readExternal(ObjectInput in) throws IOException {
        columnDelimiter = readExString(in);
        characterDelimiter = readExString(in);
        timestampFormat = readExString(in);
        dateFormat = readExString(in);
        timeFormat = readExString(in);
    }
}
