package com.splicemachine.system;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public class CsvOptions {
    public String delimited = null;
    public String escaped = null; // escaped by
    public String lines = null; // delimited by
    public String columnDelimiter = null;
    public String characterDelimiter = null;
    public String timestampFormat = null;
    public String dateFormat = null;
    public String timeFormat = null;
    public CsvOptions() {}
    public CsvOptions(ObjectInput in) throws IOException {
        readExternal(in);
    }

    public CsvOptions(String delimited,
                      String escaped,
                      String lines,
                      String columnDelimiter,
                      String characterDelimiter)
    {
        this.delimited = delimited;
        this.escaped = escaped;
        this.lines = lines;
        this.columnDelimiter = columnDelimiter;
        this.characterDelimiter = characterDelimiter;
        this.timestampFormat = null;
        this.dateFormat = null;
        this.timeFormat = null;
    }
    public CsvOptions(String delimited,
                      String escaped,
                      String lines,
                      String columnDelimiter,
                      String characterDelimiter,
                      String timestampFormat,
                      String dateFormat,
                      String timeFormat)
    {
        this.delimited = delimited;
        this.escaped = escaped;
        this.lines = lines;
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
        writeEx(out, delimited);
        writeEx(out, escaped);
        writeEx(out, lines);
        // todo: others
    }
    public void readExternal(ObjectInput in) throws IOException {
        delimited   = readExString(in);
        escaped     = readExString(in);
        lines       = readExString(in);
    }

    public String toCreateTableString()
    {
        if( delimited == null && escaped == null && lines == null ) return "";
        else {
            String str = "";
            if (delimited != null)
                str = str + "FIELDS TERMINATED BY '" + delimited + "' ";
            if (escaped != null) {
                if( delimited == null ) // no ESCAPED by without FIELDS TERMINATED BY
                    str = str + "FIELDS TERMINATED BY ',' ";
                str = str + "ESCAPED BY '" + escaped + "' ";
            }
            if (lines != null)
                str = str + "LINES TERMINATED BY '" + lines + "' ";
            return "ROW FORMAT DELIMITED " + str;
        }
    }
}
