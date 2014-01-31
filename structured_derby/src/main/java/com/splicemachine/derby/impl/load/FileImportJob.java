package com.splicemachine.derby.impl.load;

import com.google.common.io.Closeables;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.constants.bytes.BytesUtil;
import com.splicemachine.derby.impl.job.coprocessor.RegionTask;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.job.Task;

import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.*;

/**
 * @author Scott Fines
 *         Created on: 4/5/13
 */
public class FileImportJob extends ImportJob{

		private static final Logger LOG = Logger.getLogger(HdfsImport.class);

		protected FileImportJob(HTableInterface table, ImportContext context, long statementId, long operationId) {
				super(table, context, statementId, operationId);
		}

		@Override
    public Map<? extends RegionTask, Pair<byte[], byte[]>> getTasks() throws Exception {
        Path filePath = context.getFilePath();
        FileSystem fs = FileSystem.get(SpliceUtils.config);
        if(!fs.exists(filePath))
            throw new IOException("File "+ filePath+" does not exist");

        ImportReader reader = new FileImportReader();
        ImportTask task = new ImportTask(getJobId(), context,reader,
                SpliceConstants.importTaskPriority, context.getTransactionId(),statementId,operationId);
				Pair<byte[], byte[]> taskBoundary = getTaskBoundary();
				Map<ImportTask, Pair<byte[], byte[]>> importTaskPairMap = Collections.singletonMap(task, taskBoundary);

				SpliceLogUtils.info(LOG,"Importing file %s with boundaries [%s,%s)",filePath,Bytes.toStringBinary(taskBoundary.getFirst()),Bytes.toStringBinary(taskBoundary.getSecond()));
				return importTaskPairMap;
    }

    @Override
    public <T extends Task> Pair<T, Pair<byte[], byte[]>> resubmitTask(T originalTask, byte[] taskStartKey, byte[] taskEndKey) throws IOException {
        return Pair.newPair(originalTask,getTaskBoundary());
    }

    private Pair<byte[],byte[]> getTaskBoundary() throws IOException{
        byte[] tableBytes = Bytes.toBytes(context.getTableName());
        HBaseAdmin admin = null;
        List<HRegionInfo> regions = null;
        try {
        	admin = new HBaseAdmin(SpliceUtils.config);
        	regions = admin.getTableRegions(tableBytes);
        } finally {
        	Closeables.close(admin, false);
        }

        HRegionInfo regionToSubmit = null;
				if(regions!=null&&regions.size()>0) {
						Random random = new Random(); // Assign random regions for submission (spray)
						regionToSubmit = regions.get(random.nextInt(regions.size()));
				}

        byte[] start = regionToSubmit!=null?regionToSubmit.getStartKey(): HConstants.EMPTY_START_ROW;
				if(start==null || start.length==0){
						byte[] end = regionToSubmit!=null? regionToSubmit.getEndKey(): HConstants.EMPTY_END_ROW;
						if(end!=null){
								start = Arrays.copyOf(end,end.length);
								BytesUtil.unsignedDecrement(start,start.length-1);
						}
				}
        return Pair.newPair(start,start);
    }
}
