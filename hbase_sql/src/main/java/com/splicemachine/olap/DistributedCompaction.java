/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

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
    long maxWait;

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
                                 long maxWait){
        this.compactionFunction=compactionFunction;
        this.files=files;
        this.jobDetails=jobDetails;
        this.jobGroup=jobGroup;
        this.jobDescription=jobDescription;
        this.poolName=poolName;
        this.scope=scope;
        this.regionLocation=regionLocation;
        this.maxWait = maxWait;
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
        return new CompactionJob(this,jobStatus,clock,clientTimeoutCheckIntervalMs);
    }

}
