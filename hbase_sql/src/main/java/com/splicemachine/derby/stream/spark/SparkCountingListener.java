package com.splicemachine.derby.stream.spark;

import com.splicemachine.derby.impl.SpliceSpark;
import com.splicemachine.derby.stream.iapi.OperationContext;
import org.apache.spark.SparkContext;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerJobStart;
import org.apache.spark.scheduler.SparkListenerTaskEnd;
import org.apache.spark.scheduler.StageInfo;
import scala.collection.JavaConverters;

import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

class SparkCountingListener extends SparkListener implements AutoCloseable
{
    OperationContext context;
    SparkContext sc;
    String uuid;
    Set<Integer> stageIdsToWatch;

    public SparkCountingListener(OperationContext context) {
        this( context, UUID.randomUUID().toString() );
    }
    public SparkCountingListener(OperationContext context, String uuid) {
        sc = SpliceSpark.getSession().sparkContext();
        sc.getLocalProperties().setProperty("operation-uuid", uuid);
        sc.addSparkListener(this);
        this.context = context;
        this.uuid = uuid;
    }

    public void onJobStart(SparkListenerJobStart jobStart) {
        if ( !jobStart.properties().getProperty("operation-uuid", "").equals(uuid) ) {
            return;
        }

        List<StageInfo> stageInfos = JavaConverters.seqAsJavaListConverter(jobStart.stageInfos()).asJava();
        stageIdsToWatch = stageInfos.stream().map(s -> s.stageId()).collect(Collectors.toSet());
    }

    @Override
    public void onTaskEnd(SparkListenerTaskEnd taskEnd) {
        if( stageIdsToWatch != null && stageIdsToWatch.contains( taskEnd.stageId() ))
            context.recordPipelineWrites( taskEnd.taskMetrics().outputMetrics().recordsWritten() );
    }

    @Override
    public void close() {
        sc.removeSparkListener(this);
    }
}
