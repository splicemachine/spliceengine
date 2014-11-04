package com.splicemachine.derby.impl.sql.catalog;

import org.apache.derby.impl.sql.catalog.DD_Version;
import org.apache.derby.impl.sql.catalog.DataDictionaryImpl;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Created by jyuan on 10/13/14.
 */
public class Splice_DD_Version extends DD_Version {

    private int patchVerionNumer;

    public Splice_DD_Version() {

    }

    public Splice_DD_Version (DataDictionaryImpl bootingDictionary, int major, int minor, int patch) {
        this.bootingDictionary = bootingDictionary;
        this.majorVersionNumber = major;
        this.minorVersionNumber = minor;
        this.patchVerionNumer = patch;
    }

    public int getMajorVersionNumber() {
        return majorVersionNumber;
    }

    public int getMinorVersionNumber() {
        return minorVersionNumber;
    }

    public int getPatchVersionNumber() {
        return patchVerionNumer;
    }

    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append(majorVersionNumber).append(".").append(minorVersionNumber).append(".").append(patchVerionNumer);
        return sb.toString();
    }

    @Override
    public void readExternal( ObjectInput in ) throws IOException {
        super.readExternal(in);
        patchVerionNumer = in.readInt();
    }

    @Override
    public void writeExternal( ObjectOutput out ) throws IOException
    {
        super.writeExternal(out);
        out.writeInt(patchVerionNumer);
    }

    public long toLong() {
        return majorVersionNumber * 1000000 + minorVersionNumber * 1000 + patchVerionNumer;
    }

}
