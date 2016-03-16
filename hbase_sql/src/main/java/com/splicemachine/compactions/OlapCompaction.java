package com.splicemachine.compactions;

import com.splicemachine.EngineDriver;
import com.splicemachine.derby.iapi.sql.olap.OlapResult;
import com.splicemachine.derby.impl.SpliceSpark;
import com.splicemachine.derby.stream.compaction.SparkCompactionFunction;
import com.splicemachine.derby.stream.iapi.DistributedDataSetProcessor;
import com.splicemachine.derby.stream.spark.SparkFlatMapFunction;
import com.splicemachine.olap.AbstractOlapCallable;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;

/**
 * Created by dgomezferro on 3/16/16.
 */
public class OlapCompaction extends AbstractOlapCallable<CompactionResult> {
    private static final Logger LOG = Logger.getLogger(OlapCompaction.class);

    private SparkCompactionFunction compactionFunction;

    private List<String> files;

    private String jobDetails;
    private String jobGroup;
    private String jobDescription;
    private String poolName;
    private String scope;

    public OlapCompaction() {
    }

    public OlapCompaction(SparkCompactionFunction compactionFunction, List<String> files,
                          String jobDetails, String jobGroup, String jobDescription, String poolName, String scope) {
        this.compactionFunction = compactionFunction;
        this.files = files;
        this.jobDetails = jobDetails;
        this.jobGroup = jobGroup;
        this.jobDescription = jobDescription;
        this.poolName = poolName;
        this.scope = scope;
    }

    @Override
    public CompactionResult call() throws Exception {
        initializeJob(jobGroup, jobDescription, poolName);

        SpliceSpark.pushScope(scope + ": Parallelize");
        JavaRDD rdd1 = SpliceSpark.getContext().parallelize(files, 1);
        rdd1.setName("Distribute Compaction Load");
        SpliceSpark.popScope();

        SpliceSpark.pushScope(scope + ": Compact files");
        JavaRDD rdd2 = rdd1.mapPartitions(new SparkFlatMapFunction<>(compactionFunction));
        rdd2.setName(jobDetails);
        SpliceSpark.popScope();

        SpliceSpark.pushScope("Compaction");
        List<String> sPaths = rdd2.collect();
        SpliceSpark.popScope();

        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG, "Paths Returned: %s", sPaths);
        return new CompactionResult(sPaths);
    }


    protected void initializeJob(String jobGroup, String jobDescription, String poolName) {
        DistributedDataSetProcessor dsp = EngineDriver.driver().processorFactory().distributedProcessor();
        dsp.setJobGroup(jobGroup, jobDescription);
        dsp.setSchedulerPool(poolName);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeObject(compactionFunction);
        out.writeObject(files);
        out.writeUTF(jobDetails);
        out.writeUTF(jobGroup);
        out.writeUTF(jobDescription);
        out.writeUTF(poolName);
        out.writeUTF(scope);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        compactionFunction = (SparkCompactionFunction) in.readObject();
        files = (List<String>) in.readObject();
        jobDetails = in.readUTF();
        jobGroup = in.readUTF();
        jobDescription = in.readUTF();
        poolName = in.readUTF();
        scope = in.readUTF();
    }
}
