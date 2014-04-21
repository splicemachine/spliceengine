package com.splicemachine.derby.impl.load;

import com.google.common.collect.Maps;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.constants.bytes.BytesUtil;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.impl.job.coprocessor.RegionTask;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.hbase.HBaseRegionCache;
import com.splicemachine.job.Task;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Logger;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * @author Scott Fines
 *         Created on: 4/5/13
 */
public class FileImportJob extends ImportJob{

		private static final Logger LOG = Logger.getLogger(HdfsImport.class);
		private final List<Path> files;

		protected FileImportJob(HTableInterface table,
														ImportContext context,
														long statementId,
														List<Path> files,
														long operationId) {
				super(table, context, statementId, operationId);
				this.files = files;
		}

		@Override
    public Map<? extends RegionTask, Pair<byte[], byte[]>> getTasks() throws Exception {
        FileSystem fs = FileSystem.get(SpliceUtils.config);
				for(Path filePath:files){
						if(!fs.exists(filePath))
								throw new FileNotFoundException("File "+ filePath+" does not exist");
				}

        ImportReader reader = new FileImportReader();
				Map<ImportTask,Pair<byte[],byte[]>> tasks = Maps.newHashMap();
				for(Path filePath:files){
						ImportContext ctx = context.getCopy();
						ctx.setFilePath(filePath);
						ImportTask importTask = new ImportTask(jobId, ctx, reader, SpliceConstants.importTaskPriority, context.getTransactionId(), statementId, operationId);
						tasks.put(importTask,getTaskBoundary(ctx));
				}

				return tasks;
    }

    @Override
    public <T extends Task> Pair<T, Pair<byte[], byte[]>> resubmitTask(T originalTask, byte[] taskStartKey, byte[] taskEndKey) throws IOException {
				ImportContext ctx = ((ImportTask) originalTask).getContext();
				Pair<T, Pair<byte[], byte[]>> retPair = Pair.newPair(originalTask, getTaskBoundary(ctx));
				//invalidate the region cache so that it forces a check in the event of region splits
				HBaseRegionCache.getInstance().invalidate(TableName.valueOf(ctx.getTableName()));
				return retPair;
    }

    private Pair<byte[],byte[]> getTaskBoundary(ImportContext ctx) throws IOException{
        TableName tableBytes = SpliceDriver.driver().getTempTable().getTempTableName();
				List<HRegionInfo> regions;
				HBaseAdmin admin = new HBaseAdmin(SpliceConstants.config);
				try{
						regions = admin.getTableRegions(tableBytes);
				}finally{
						admin.close();
				}

				HRegionInfo regionToSubmit = null;
				if(regions!=null&&regions.size()>0) {
						Random random = new Random(); // Assign random regions for submission (spray)
						regionToSubmit = regions.get(random.nextInt(regions.size()));
				}

        byte[] start = regionToSubmit!=null?regionToSubmit.getStartKey(): HConstants.EMPTY_START_ROW;
				if(start==null || start.length==0){
						if(LOG.isDebugEnabled())
								LOG.debug("Chose first region in table. Attempting to select the end of the region");
						byte[] end = regionToSubmit!=null? regionToSubmit.getEndKey(): HConstants.EMPTY_END_ROW;
						if(end!=null && end.length>0){
								if(LOG.isDebugEnabled())
										LOG.debug("End of the region is not empty, so we can use that value");
								start = Arrays.copyOf(end,end.length);
								BytesUtil.unsignedDecrement(start,start.length-1);
						}
				}
				if(LOG.isDebugEnabled()) {
						String s = Bytes.toStringBinary(start);
						SpliceLogUtils.debug(LOG,"For file %s, Choosing region boundary of [%s,%s)",ctx.getFilePath(), s,s);
				}
        return Pair.newPair(start,start);
    }
}
