package com.splicemachine.derby.utils;

import com.splicemachine.derby.impl.job.JobInfo;
import com.splicemachine.derby.management.OperationInfo;
import com.splicemachine.derby.management.StatementInfo;
import com.splicemachine.derby.stats.TaskStats;
import com.splicemachine.job.Status;
import com.splicemachine.job.TaskFuture;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.impl.ActiveWriteTxn;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;

/**
 * @author Scott Fines
 *         Date: 4/26/16
 */
public class StatementAdminTest{

    @Test
    public void testBuildOperationDetailsProperly() throws Exception{
        StatementInfo si = new StatementInfo("show tables","SPLICE",new ActiveWriteTxn(49168L,49168L),1,1158908576745672706L);
        List<OperationInfo> info = new ArrayList<>();
        info.add(new OperationInfo(-3434763043171274750L,si.getStatementUuid(),"Union",null,false,-4587684547778125822L));
        info.add(new OperationInfo(-1128920033957580798L,si.getStatementUuid(),"Normalize",null,false,-2281841538564427774L));
        info.add(new OperationInfo(-2281841538564427774L,si.getStatementUuid(),"Union",null,false,-3434763043171274750L));
        info.add(new OperationInfo(-8046449061598666750L,si.getStatementUuid(),"ProjectRestrict",null,true,4635687489076649986L));
        info.add(new OperationInfo(-4587684547778125822L,si.getStatementUuid(),"Union",null,false,-5740606052384972798L));
        info.add(new OperationInfo(-5740606052384972798L,si.getStatementUuid(),"ProjectRestrict",null,true,3482765984469803010L));
        info.add(new OperationInfo(-5740606052384972798L,si.getStatementUuid(),"ProjectRestrict",null,true,3482765984469803010L));
        info.add(new OperationInfo(-6893527556991819774L,si.getStatementUuid(),"TableScan",null,false,-8046449061598666750L));
        info.add(new OperationInfo(-9199370566205513726L,si.getStatementUuid(),"TableScan",null,true,5788608993683496962L));
        OperationInfo sort=new OperationInfo(1176922975256109058L,si.getStatementUuid(),"Sort",null,false,6932523299035611138L);
        info.add(sort);

        info.add(new OperationInfo(6941530498290343938L,si.getStatementUuid(),"ProjectRestrict",null,false,5788608993683496962L));
        info.add(new OperationInfo(8094452002897190914L,si.getStatementUuid(),"TableScan",null,false,6941530498290343938L));
        info.add(new OperationInfo(2329844479862956034L,si.getStatementUuid(),"ProjectRestrict",null,false,1176922975256109058L));
        OperationInfo sortMergeJoin=new OperationInfo(3482765984469803010L,si.getStatementUuid(),"SortMergeJoin",null,false,2329844479862956034L);
        info.add(sortMergeJoin);
        info.add(new OperationInfo(4635687489076649986L,si.getStatementUuid(),"BroadcastJoin",null,false,3482765984469803010L));
        info.add(new OperationInfo(5788608993683496962L,si.getStatementUuid(),"NestedLoopJoin",null,false,4635687489076649986L));
        info.add(new OperationInfo(5987072139788290L,si.getStatementUuid(),"Row",null,false,-1128920033957580798L));
        info.add(new OperationInfo(1158908576746635266L,si.getStatementUuid(),"Row",null,true,-2281841538564427774L));
        info.add(new OperationInfo(2311830081353482242L,si.getStatementUuid(),"Normalize",null,true,-3434763043171274750L));
        info.add(new OperationInfo(3464751585960329218L,si.getStatementUuid(),"Row",null,false,2311830081353482242L));
        info.add(new OperationInfo(4617673090567176194L,si.getStatementUuid(),"Normalize",null,true,-4587684547778125822L));
        info.add(new OperationInfo(5770594595174023170L,si.getStatementUuid(),"Row",null,false,4617673090567176194L));
        info.add(new OperationInfo(6932523299035611138L,si.getStatementUuid(),"ScrollInsensitive",null,false,-1L));

        si.setOperationInfo(info);
        JobInfo jobInfo=new JobInfo(Long.toString(sort.getOperationUuid()),1,1461687533896L);
        jobInfo.taskRunning(Bytes.toBytes(1l));
        si.addRunningJob(sort.getOperationUuid(),jobInfo);

        jobInfo = new JobInfo(Long.toString(sortMergeJoin.getOperationUuid()),2,1461687533896L);
        jobInfo.taskRunning(Bytes.toBytes(2L));
        jobInfo.taskRunning(Bytes.toBytes(3L));

        si.addRunningJob(sortMergeJoin.getOperationUuid(),jobInfo);
        jobInfo.success(new TaskFuture(){
            @Override
            public Status getStatus() throws ExecutionException{
                return Status.COMPLETED;
            }

            @Override public void complete() throws ExecutionException, CancellationException, InterruptedException{ }
            @Override public double getEstimatedCost(){ return 0; }

            @Override
            public byte[] getTaskId(){
                return Bytes.toBytes(2L);
            }

            @Override public TaskStats getTaskStats(){ return null; }
        });

        jobInfo.success(new TaskFuture(){
            @Override
            public Status getStatus() throws ExecutionException{
                return Status.COMPLETED;
            }

            @Override public void complete() throws ExecutionException, CancellationException, InterruptedException{ }
            @Override public double getEstimatedCost(){ return 0; }

            @Override
            public byte[] getTaskId(){
                return Bytes.toBytes(3L);
            }

            @Override public TaskStats getTaskStats(){ return null; }
        });
        si.completeJob(jobInfo);

        StatementAdmin.OperationTreeNode operationTreeNode=StatementAdmin.buildOperationTree(si);

        System.out.println(operationTreeNode.buildTreeString());
    }
}