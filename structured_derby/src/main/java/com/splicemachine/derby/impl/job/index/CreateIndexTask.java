package com.splicemachine.derby.impl.job.index;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.splicemachine.constants.SIConstants;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.ddl.DDLChange;
import com.splicemachine.derby.ddl.TentativeIndexDesc;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.hbase.SpliceIndexEndpoint;
import com.splicemachine.derby.impl.job.ZkTask;
import com.splicemachine.derby.impl.job.operation.OperationJob;
import com.splicemachine.derby.impl.sql.execute.LocalWriteContextFactory;
import com.splicemachine.derby.impl.sql.execute.index.IndexTransformer;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.derby.utils.marshall.RowMarshaller;
import com.splicemachine.encoding.MultiFieldEncoder;
import com.splicemachine.hbase.writer.CallBuffer;
import com.splicemachine.hbase.writer.KVPair;
import com.splicemachine.storage.EntryPredicateFilter;
import com.splicemachine.storage.Predicate;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.utils.SpliceZooKeeperManager;
import org.apache.derby.iapi.services.io.ArrayUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.MultiVersionConsistencyControl;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * @author Scott Fines
 * Created on: 4/5/13
 */
public class CreateIndexTask extends ZkTask {
    private static final long serialVersionUID = 5l;

    private DDLChange ddlChange;

    public CreateIndexTask() { }

    public CreateIndexTask(String jobId,
                           DDLChange ddlChange) {
        super(jobId, OperationJob.operationTaskPriority,ddlChange.getTransactionId(),false);
        this.ddlChange = ddlChange;
    }

    @Override
    public void prepareTask(RegionCoprocessorEnvironment rce, SpliceZooKeeperManager zooKeeper) throws ExecutionException {
        super.prepareTask(rce, zooKeeper);
    }

    @Override
    protected String getTaskType() {
        return "createIndexTask";
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeObject(ddlChange);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        ddlChange = (DDLChange) in.readObject();
    }

    @Override
    public boolean invalidateOnClose() {
        return true;
    }
    @Override
    public void doExecute() throws ExecutionException, InterruptedException {
        try{
            //add index to table watcher
            TentativeIndexDesc tentativeIndexDesc = ddlChange.getTentativeIndexDesc();
            LocalWriteContextFactory contextFactory = SpliceIndexEndpoint.getContextFactory(tentativeIndexDesc.getBaseConglomerateNumber());
            contextFactory.addIndex(ddlChange);
        } catch (Exception e) {
            throw new ExecutionException(Throwables.getRootCause(e));
        }
    }

}
