package com.splicemachine.olap;

import com.splicemachine.concurrent.Clock;
import com.splicemachine.derby.iapi.sql.olap.DistributedJob;
import com.splicemachine.derby.iapi.sql.olap.OlapStatus;
import com.splicemachine.derby.stream.compaction.SparkCompactionFunction;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.SerializationUtils;

import java.io.Serializable;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * Dumb data holder for a CompactionRequest
 * @author Scott Fines
 *         Date: 4/1/16
 */
public class DistributedCompaction extends DistributedJob{
    private static final long serialVersionUID = 1l;

    SparkCompactionFunction compactionFunction;

    List<String> files;

    String jobDetails;
    String jobGroup;
    String jobDescription;
    String poolName;
    String scope;
    String regionLocation;
    int id;

    /*Used for Serialization*/
    @SuppressWarnings("unused")
    public DistributedCompaction(){ }

    public DistributedCompaction(SparkCompactionFunction compactionFunction,
                                 List<String> files,
                                 String jobDetails,
                                 String jobGroup,
                                 String jobDescription,
                                 String poolName,
                                 String scope,
                                 String regionLocation,
                                 int id){
        this.compactionFunction=compactionFunction;
        this.files=files;
        this.jobDetails=jobDetails;
        this.jobGroup=jobGroup;
        this.jobDescription=jobDescription;
        this.poolName=poolName;
        this.scope=scope;
        this.regionLocation=regionLocation;
        this.id = id;
    }

    public String base64EncodedFileList(){
        assert files instanceof Serializable: "Non-serializable list specified!";
        return Base64.encodeBase64String(SerializationUtils.serialize((Serializable)files));
    }

    @Override
    public boolean equals(Object o){
        if(this==o) return true;
        if(!(o instanceof DistributedCompaction)) return false;

        DistributedCompaction that=(DistributedCompaction)o;

        return jobGroup.equals(that.jobGroup);

    }

    @Override
    public String getName(){
        return jobGroup;
    }

    @Override
    public int hashCode(){
        return jobGroup.hashCode();
    }

    @Override
    public Callable<Void> toCallable(OlapStatus jobStatus,Clock clock,long clientTimeoutCheckIntervalMs){
        return new CompactionJob(this,jobStatus,clock,clientTimeoutCheckIntervalMs,id);
    }

}
