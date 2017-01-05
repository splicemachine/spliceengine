package com.splicemachine.procedures.external;

import com.splicemachine.concurrent.Clock;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
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

    private String jobGroup;
    private ExecRow execRow;
    int[] partitionBy;



    public DistributedCreateExternalTableJob() {
    }

    public DistributedCreateExternalTableJob(String delimited,
                                             String escaped,
                                             String lines,
                                             String storedAs,
                                             String location,
                                             int[] partitionBy,
                                             String jobGroup,
                                             ExecRow execRow) {
        this.delimited = delimited;
        this.escaped = escaped;
        this.lines = lines;
        this.storedAs = storedAs;
        this.location = location;
        this.partitionBy = partitionBy;
        this.jobGroup = jobGroup;
        this.execRow =  execRow;
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

        out.writeUTF(jobGroup);
        out.writeObject(execRow);
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
        jobGroup    = in.readUTF();
        execRow     = (ExecRow)in.readObject();

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

    public String getJobGroup() {
        return jobGroup;
    }

    public ExecRow getExecRow() {
        return execRow;
    }

    public int[] getPartitionBy() {
        return partitionBy;
    }
}
