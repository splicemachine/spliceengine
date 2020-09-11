package com.splicemachine.system;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public class CsvOptions {
    public String delimited = null;
    public String escaped = null; // escaped by
    public String lines = null; // delimited by
    public CsvOptions() {}
    public CsvOptions(ObjectInput in) throws IOException {
        readExternal(in);
    }
    public CsvOptions(String delimited,
                      String escaped,
                      String lines)
    {
        this.delimited = delimited;
        this.escaped = escaped;
        this.lines = lines;
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
    }
    public void readExternal(ObjectInput in) throws IOException {
        delimited   = readExString(in);
        escaped     = readExString(in);
        lines       = readExString(in);
    }
}
