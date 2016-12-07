package com.splicemachine.procedures.external;

import com.splicemachine.concurrent.Clock;
import com.splicemachine.derby.iapi.sql.olap.DistributedJob;
import com.splicemachine.derby.iapi.sql.olap.OlapStatus;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.Callable;

/**
 * Created by jfilali on 11/14/16.
 * When a user make a modification in a external file outside of Splice, there is no way for use
 * to detect and reload the schema automatically. This Job is meant to be use with a procedure.
 *
 */
public class DistributedRefreshExternalTableSchemaJob extends DistributedJob implements Externalizable {

     private String jobGroup;
     private String location;




    public DistributedRefreshExternalTableSchemaJob() {
    }

    public DistributedRefreshExternalTableSchemaJob(String jobGroup, String location) {
        this.jobGroup = jobGroup;
        this.location = location;

    }

    @Override
    public Callable<Void> toCallable(OlapStatus jobStatus, Clock clock, long clientTimeoutCheckIntervalMs) {
        return new RefreshTableSchemaTableJob(this, jobStatus);
    }

    @Override
    public String getName() {
        return null;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeUTF(jobGroup);
        out.writeUTF(location);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
       jobGroup = in.readUTF();
       location = in.readUTF();
    }

    public String getJobGroup() {
        return jobGroup;
    }

    public String getLocation() {
        return location;
    }
}
