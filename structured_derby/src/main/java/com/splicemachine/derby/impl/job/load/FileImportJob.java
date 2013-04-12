package com.splicemachine.derby.impl.job.load;

import com.splicemachine.constants.bytes.BytesUtil;
import com.splicemachine.derby.impl.job.coprocessor.RegionTask;
import com.splicemachine.derby.utils.SpliceUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * @author Scott Fines
 *         Created on: 4/5/13
 */
public class FileImportJob extends ImportJob{

    protected FileImportJob(HTableInterface table, ImportContext context) {
        super(table, context);
    }

    @Override
    public Map<? extends RegionTask, Pair<byte[], byte[]>> getTasks() throws Exception {
        Path filePath = context.getFilePath();
        FileSystem fs = FileSystem.get(SpliceUtils.config);
        if(!fs.exists(filePath))
            throw new IOException("File "+ filePath+" does not exist");

        byte[] tableBytes = Bytes.toBytes(context.getTableName());

        HBaseAdmin admin = new HBaseAdmin(SpliceUtils.config);
        List<HRegionInfo> regions = admin.getTableRegions(tableBytes);
        HRegionInfo regionToSubmit = null;
        if(regions!=null&&regions.size()>0)
            regionToSubmit = regions.get(0);

        byte[] start = regionToSubmit!=null?regionToSubmit.getStartKey(): new byte[]{};
        byte[] end = regionToSubmit!=null?regionToSubmit.getEndKey(): new byte[]{};
        if(end.length>0){
            byte[] endRow = new byte[end.length];
            System.arraycopy(end,0,endRow,0,end.length);
            BytesUtil.decrementAtIndex(endRow,endRow.length-1);
            end = endRow;
        }

        return Collections.singletonMap(new FileImportTask(getJobId(),context,ImportJob.importTaskPriority),Pair.newPair(start,end));
    }
}
