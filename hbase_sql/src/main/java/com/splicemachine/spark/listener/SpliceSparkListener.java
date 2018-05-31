package com.splicemachine.spark.listener;

import com.splicemachine.derby.impl.SpliceSpark;
import com.splicemachine.derby.stream.spark.SparkOperationContext;
import org.apache.spark.Accumulable;
import org.apache.spark.SparkContext;
import org.apache.spark.scheduler.*;
import org.apache.spark.util.LongAccumulator;
import org.apache.zookeeper.txn.Txn;
import scala.*;
import scala.collection.*;
import scala.collection.Iterable;
import scala.collection.Traversable;
import scala.collection.generic.CanBuildFrom;
import scala.collection.immutable.*;
import scala.collection.immutable.LinearSeq;
import scala.collection.immutable.Seq;

import java.util.ArrayList;

/**
 *
 * Interface for listening to the spark bus...
 *
 */
public class SpliceSparkListener implements SparkListenerInterface {
    private SparkContext sc;
    public SpliceSparkListener(SparkContext sc) {
        this.sc = sc;
    }


    @Override
    public void onStageCompleted(SparkListenerStageCompleted stageCompleted) {

    }

    @Override
    public void onApplicationStart(SparkListenerApplicationStart applicationStart) {

    }

    @Override
    public void onJobStart(SparkListenerJobStart jobStart) {
        System.out.println("onJobStartProperties?" + jobStart.properties());
    }

    @Override
    public void onExecutorBlacklisted(SparkListenerExecutorBlacklisted executorBlacklisted) {

    }

    @Override
    public void onBlockManagerRemoved(SparkListenerBlockManagerRemoved blockManagerRemoved) {

    }

    @Override
    public void onExecutorRemoved(SparkListenerExecutorRemoved executorRemoved) {

    }

    @Override
    public void onUnpersistRDD(SparkListenerUnpersistRDD unpersistRDD) {

    }

    @Override
    public void onTaskGettingResult(SparkListenerTaskGettingResult taskGettingResult) {

    }

    @Override
    public void onExecutorUnblacklisted(SparkListenerExecutorUnblacklisted executorUnblacklisted) {

    }

    @Override
    public void onNodeUnblacklisted(SparkListenerNodeUnblacklisted nodeUnblacklisted) {

    }

    @Override
    public void onNodeBlacklisted(SparkListenerNodeBlacklisted nodeBlacklisted) {

    }

    @Override
    public void onExecutorAdded(SparkListenerExecutorAdded executorAdded) {

    }

    @Override
    public void onBlockUpdated(SparkListenerBlockUpdated blockUpdated) {

    }

    @Override
    public void onEnvironmentUpdate(SparkListenerEnvironmentUpdate environmentUpdate) {

    }

    @Override
    public void onOtherEvent(SparkListenerEvent event) {

    }

    @Override
    public void onTaskEnd(SparkListenerTaskEnd taskEnd) {
        System.out.println("task End -_ >> " + taskEnd.taskInfo().accumulables());
    }

    @Override
    public void onBlockManagerAdded(SparkListenerBlockManagerAdded blockManagerAdded) {

    }

    @Override
    public void onApplicationEnd(SparkListenerApplicationEnd applicationEnd) {

    }

    @Override
    public void onTaskStart(SparkListenerTaskStart taskStart) {
        ArrayList accumulableInfoList = new ArrayList();
        TxnAccumulator txnAccumulator = new TxnAccumulator("TXNIDFOOEY");
        sc.register(txnAccumulator,"txn");
        accumulableInfoList.add(txnAccumulator.toInfo(Option.apply(txnAccumulator),Option.apply(txnAccumulator)));
        taskStart.taskInfo().setAccumulables(
                scala.collection.JavaConversions.asScalaBuffer(accumulableInfoList).toSeq());
    }

    @Override
    public void onExecutorMetricsUpdate(SparkListenerExecutorMetricsUpdate executorMetricsUpdate) {

    }

    @Override
    public void onStageSubmitted(SparkListenerStageSubmitted stageSubmitted) {

    }

    @Override
    public void onJobEnd(SparkListenerJobEnd jobEnd) {

    }
}
