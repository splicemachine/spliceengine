/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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

package com.splicemachine.procedures.external;

import com.splicemachine.concurrent.Clock;
import com.splicemachine.db.impl.sql.execute.ColumnInfo;
import com.splicemachine.derby.iapi.sql.olap.DistributedJob;
import com.splicemachine.derby.iapi.sql.olap.OlapStatus;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.Callable;

/**
 * Created by jfilali on 11/14/16.
 * Job specification for the creation of a empty external table if none is available when one
 * create an external table. This is used to make sure we always have an external table file
 * and avoid to have a case where the user query a table that doesn't have a file attached to it.
 */
public class DistributedCreateExternalTableJob extends DistributedJob implements Externalizable {
    private String delimited;
    private String escaped;
    private String lines;
    private String storedAs;
    private String location;
    private String compression;

    private String jobGroup;
    private ColumnInfo[] columnInfo;
    int[] partitionBy;



    public DistributedCreateExternalTableJob() {
    }

    public DistributedCreateExternalTableJob(String delimited,
                                             String escaped,
                                             String lines,
                                             String storedAs,
                                             String location,
                                             String compression,
                                             int[] partitionBy,
                                             String jobGroup,
                                             ColumnInfo[] columnInfo) {
        this.delimited = delimited;
        this.escaped = escaped;
        this.lines = lines;
        this.storedAs = storedAs;
        this.compression = compression;
        this.location = location;
        this.partitionBy = partitionBy;
        this.jobGroup = jobGroup;
        this.columnInfo =  columnInfo;
    }

    @Override
    public Callable<Void> toCallable(OlapStatus jobStatus, Clock clock, long clientTimeoutCheckIntervalMs) {
        return new CreateExternalTableJob(this, jobStatus);
    }

    @Override
    public String getName() {
        return null;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        int length = partitionBy.length;
        out.writeInt(length);
        for(int i =0;i<length;i++){
            out.writeInt(partitionBy[i]);
        }

        out.writeBoolean(delimited!=null);
        if (delimited!=null)
            out.writeUTF(delimited);

        out.writeBoolean(escaped!=null);
        if (escaped!=null)
            out.writeUTF(escaped);

        out.writeBoolean(lines!=null);
        if (lines!=null)
            out.writeUTF(lines);

        out.writeBoolean(storedAs!=null);
        if (storedAs!=null)
            out.writeUTF(storedAs);

        out.writeBoolean(location!=null);
        if (location!=null)
            out.writeUTF(location);

        out.writeBoolean(compression!=null);
        if (compression!=null)
            out.writeUTF(compression);

        out.writeUTF(jobGroup);
        out.writeInt(columnInfo.length);
        for(int i = 0; i < columnInfo.length; ++i) {
            out.writeObject(columnInfo[i]);
        }
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        int length = in.readInt();
        partitionBy = new int[length];
        for(int i =0;i<length;i++){
            partitionBy[i]=in.readInt();
        }

        delimited   = in.readBoolean()?in.readUTF():null;
        escaped     = in.readBoolean()?in.readUTF():null;
        lines       = in.readBoolean()?in.readUTF():null;
        storedAs    = in.readBoolean()?in.readUTF():null;
        location    = in.readBoolean()?in.readUTF():null;
        compression    = in.readBoolean()?in.readUTF():null;
        jobGroup    = in.readUTF();
        int n = in.readInt();
        columnInfo = new ColumnInfo[n];
        for (int i = 0; i < n; ++i) {
            columnInfo[i] = (ColumnInfo)in.readObject();
        }
    }

    public String getDelimited() {
        return delimited;
    }

    public String getEscaped() {
        return escaped;
    }

    public String getLines() {
        return lines;
    }

    public String getStoredAs() {
        return storedAs;
    }

    public String getLocation() {
        return location;
    }

    public String getCompression() {
        return compression;
    }

    public String getJobGroup() {
        return jobGroup;
    }

    public ColumnInfo[] getColumnInfo() {
        return columnInfo;
    }

    public int[] getPartitionBy() {
        return partitionBy;
    }
}
