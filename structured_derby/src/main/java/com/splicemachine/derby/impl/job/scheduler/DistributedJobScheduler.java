package com.splicemachine.derby.impl.job.scheduler;

import com.splicemachine.derby.impl.job.coprocessor.CoprocessorJob;
import com.splicemachine.derby.impl.job.coprocessor.CoprocessorTaskScheduler;
import com.splicemachine.derby.impl.job.coprocessor.RegionTask;
import com.splicemachine.job.JobFuture;
import com.splicemachine.job.JobScheduler;
import com.splicemachine.job.JobSchedulerManagement;
import com.splicemachine.utils.SpliceZooKeeperManager;
import com.splicemachine.utils.ZkUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * JobScheduler which uses ZooKeeper and HBase coprocessors to submit and manage tasks.
 *
 * //TODO -sf- more documentation here
 * @author Scott Fines
 * Created on: 5/9/13
 */
public class DistributedJobScheduler implements JobScheduler<CoprocessorJob>{
    private static final int DEFAULT_MAX_RESUBMISSIONS = 20;
		private static final Logger LOG = Logger.getLogger(DistributedJobScheduler.class);
		protected final SpliceZooKeeperManager zkManager;
    private final int maxResubmissionAttempts;

    private final JobMetrics jobMetrics = new StatementMetrics();

    public DistributedJobScheduler(SpliceZooKeeperManager zkManager, Configuration configuration) {
        this.zkManager = zkManager;

        maxResubmissionAttempts = configuration.getInt("splice.tasks.maxResubmissions",DEFAULT_MAX_RESUBMISSIONS);
    }

    @Override
    public JobFuture submit(CoprocessorJob job) throws ExecutionException {
        jobMetrics.totalSubmittedJobs.incrementAndGet();
        try{
            String jobPath = createJobNode(job);
            return submitTasks(job,jobPath);
        } catch (InterruptedException e) {
            throw new ExecutionException(e);
        } catch (KeeperException e) {
            throw new ExecutionException(e);
        } catch (Exception e) {
            throw new ExecutionException(e);
        }
    }

    @Override
    public JobSchedulerManagement getJobMetrics() {
        return jobMetrics;
    }

		@Override
		public long[] getActiveOperations() throws ExecutionException {
				/*
				 *Look at ZooKeeper for the job list
				 */
				try {
						List<String> children = zkManager.getRecoverableZooKeeper().getChildren(CoprocessorTaskScheduler.getJobPath(), false);
						long[] jobs = new long[children.size()];
						int i=0;
						for(String child:children){
								try{
										jobs[i] = Long.parseLong(child);
								}catch(NumberFormatException nfe){
										jobs[i] = -1;
									if(LOG.isDebugEnabled()){
										LOG.debug("job "+ child+" ignored, because it is not an operation job");
									}
								}
								i++;
						}
						return jobs;
				} catch (ZooKeeperConnectionException e) {
						throw new ExecutionException(e);
				} catch (InterruptedException e) {
						throw new ExecutionException(e);
				} catch (KeeperException e) {
						throw new ExecutionException(e);
				}
		}

		/********************************************************************************************/
    /*Private helper methods*/

    private JobFuture submitTasks(CoprocessorJob job,String jobPath) throws ExecutionException{
        JobControl control = new JobControl(job,jobPath,zkManager,maxResubmissionAttempts, jobMetrics);
        Map<? extends RegionTask, Pair<byte[], byte[]>> tasks;
        try {
            tasks = job.getTasks();
        } catch (Exception e) {
            throw new ExecutionException("Unable to get tasks for submission",e);
        }

        jobMetrics.numRunningJobs.incrementAndGet();
        HTableInterface table = job.getTable();
        for(Map.Entry<? extends RegionTask,Pair<byte[],byte[]>> taskEntry:tasks.entrySet()){
            control.submit(taskEntry.getKey(),taskEntry.getValue(),table,0);
        }
        return control;
    }

    private String createJobNode(CoprocessorJob job) throws KeeperException, InterruptedException {
        String jobId = job.getJobId();
        jobId = jobId.replaceAll("/","_");
        String path = CoprocessorTaskScheduler.getJobPath()+"/"+jobId;
        ZkUtils.recursiveSafeCreate(path, new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

        return path;
    }
}
