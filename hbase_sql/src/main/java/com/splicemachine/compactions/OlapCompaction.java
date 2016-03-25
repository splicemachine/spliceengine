package com.splicemachine.compactions;

import com.splicemachine.EngineDriver;
import com.splicemachine.access.HConfiguration;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.derby.impl.SpliceSpark;
import com.splicemachine.derby.stream.compaction.SparkCompactionFunction;
import com.splicemachine.derby.stream.iapi.DistributedDataSetProcessor;
import com.splicemachine.derby.stream.spark.SparkFlatMapFunction;
import com.splicemachine.mrio.MRConstants;
import com.splicemachine.olap.AbstractOlapCallable;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.ParallelCollectionRDD;
import scala.collection.Map;
import scala.reflect.ClassTag;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
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
    private String regionLocation;

    public OlapCompaction() {
    }

    public OlapCompaction(SparkCompactionFunction compactionFunction, List<String> files, String regionLocaiton,
                          String jobDetails, String jobGroup, String jobDescription, String poolName, String scope) {
        this.compactionFunction = compactionFunction;
        this.files = files;
        this.regionLocation = regionLocaiton;
        this.jobDetails = jobDetails;
        this.jobGroup = jobGroup;
        this.jobDescription = jobDescription;
        this.poolName = poolName;
        this.scope = scope;
    }

    @Override
    public CompactionResult call() throws Exception {
        initializeJob(jobGroup, jobDescription, poolName);
        Configuration conf = new Configuration(HConfiguration.unwrapDelegate());
        if (LOG.isTraceEnabled()) {
            LOG.trace("regionLocation = " + regionLocation);
        }
        conf.set(MRConstants.REGION_LOCATION, regionLocation);
        conf.set(MRConstants.COMPACTION_FILES,getCompactionFilesBase64String());

        SpliceSpark.pushScope(scope + ": Parallelize");
        //JavaRDD rdd1 = SpliceSpark.getContext().parallelize(files, 1);
        //ParallelCollectionRDD rdd1 = getCompactionRDD();

        JavaPairRDD rdd1 = SpliceSpark.getContext().newAPIHadoopRDD(conf, CompactionInputFormat.class, Integer.class, Iterator.class);
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

    private ParallelCollectionRDD getCompactionRDD() {

        List<String> locationList = new ArrayList<>(1);
        locationList.add(regionLocation);
        scala.collection.mutable.Buffer locationBuffer = scala.collection.JavaConversions.asScalaBuffer(locationList);
        scala.collection.Seq<String> locationSeq = locationBuffer.toSeq();
        java.util.Map<Object, scala.collection.Seq<String>> locationMap = new HashMap<>();
        locationMap.put(new Integer(1), locationSeq);
        scala.collection.Map locations = scala.collection.JavaConversions.asScalaMap(locationMap);

        scala.collection.mutable.Buffer fileBuffer = scala.collection.JavaConversions.asScalaBuffer(files);
        scala.collection.Seq<String> fileSeq = fileBuffer.toSeq();

        ClassTag<String> tag = scala.reflect.ClassTag$.MODULE$.apply(String.class);
        ParallelCollectionRDD rdd1 = new ParallelCollectionRDD<String>(SpliceSpark.getContext().sc(), fileSeq, 1, locations, tag);

        return rdd1;
    }

    private String getCompactionFilesBase64String() throws IOException, StandardException {
        return Base64.encodeBase64String(SerializationUtils.serialize((ArrayList<String>)files));
    }
}
