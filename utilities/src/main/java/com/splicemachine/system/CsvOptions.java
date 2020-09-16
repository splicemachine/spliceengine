package com.splicemachine.system;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;

public class CsvOptions implements Serializable {
    public String columnDelimiter = null; // character used to separate columns in the CSV file (default = ,)
    public String escapeCharacter = null; // character used to escape string separators, e.g. "hello \"world\"!". default \
    public String lineTerminator = null;  // LINES DELIMITED BY
    public CsvOptions() {}
    public CsvOptions(ObjectInput in) throws IOException {
        readExternal(in);
    }
    public CsvOptions(String columnDelimiter,
                      String escapeCharacter,
                      String lineTerminator)
    {
        this.columnDelimiter = columnDelimiter;
        this.escapeCharacter = escapeCharacter;
        this.lineTerminator = lineTerminator;
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
        writeEx(out, escapeCharacter);
        writeEx(out, lineTerminator);
    }
    public void readExternal(ObjectInput in) throws IOException {
        columnDelimiter = readExString(in);
        escapeCharacter = readExString(in);
        lineTerminator = readExString(in);
    }
}
