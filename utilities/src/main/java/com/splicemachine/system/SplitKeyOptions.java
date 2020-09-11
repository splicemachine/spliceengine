package com.splicemachine.system;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public class SplitKeyOptions {
    public String columnDelimiter = null;    // character used to separate columns in the CSV file (default = ,)
    public String characterDelimiter = null; // character used to delimit strings in the CSV file ( default = ")
    public String timestampFormat = null;
    public String dateFormat = null;
    public String timeFormat = null;
    public SplitKeyOptions() {}
    public SplitKeyOptions(ObjectInput in) throws IOException {
        readExternal(in);
    }
    public SplitKeyOptions(String columnDelimiter,
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
