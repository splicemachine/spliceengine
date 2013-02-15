package com.splicemachine.derby.impl.load;

import com.splicemachine.constants.bytes.BytesUtil;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.derby.utils.SpliceUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

/**
 *  Sequentially imports a CSV file into HDFS.
 *
 *  This is a relatively inefficient means to execute a bulk-load, but it is useful when the
 * file is compressed using some non-splittable compression scheme, such as gzip.
 *
 * @author Scott Fines
 * Created: 1/30/13 2:47 PM
 */
public class SequentialImporter implements Importer {

	protected final HBaseAdmin admin;
	protected final ImportContext context;

	public SequentialImporter(HBaseAdmin admin, ImportContext context) {
		this.admin = admin;
		this.context = context;
	}

	@Override
	public long importData() throws IOException {
		Path filePath = context.getFilePath();
		FileSystem fs = FileSystem.get(SpliceUtils.config);
		if(!fs.exists(filePath))
			throw new IOException("File "+filePath+" does not exist!");
		/*
		 * Imports data sequentially, by just picking a RegionServer at random, and submitting a coprocessor
		 * exec to the ImportCoprocessor to execute.
		 */
		byte[] tableBytes = Bytes.toBytes(Long.toString(context.getTableId()));

		List<HRegionInfo> infos = admin.getTableRegions(tableBytes);
		HRegionInfo regionInfo = null;
		if(infos!=null&&infos.size()>0)
			regionInfo = infos.get(0);


		HTableInterface table = SpliceAccessManager.getHTable(tableBytes);

		final AtomicLong rowsImported = new AtomicLong(0l);
		byte[] start = regionInfo!=null?regionInfo.getStartKey():new byte[]{};
		byte[] end = regionInfo!=null?regionInfo.getEndKey(): new byte[]{};
		if(end.length>0)
			BytesUtil.decrementAtIndex(end,end.length-1);
		final CountDownLatch latch = new CountDownLatch(1);
		try {
			table.coprocessorExec(SpliceImportProtocol.class,start,end,new Batch.Call<SpliceImportProtocol, Long>() {
				@Override
				public Long call(SpliceImportProtocol instance) throws IOException {
					return instance.importFile(context);
				}
			},new Batch.Callback<Long>() {
						@Override
						public void update(byte[] region, byte[] row, Long result) {
							rowsImported.addAndGet(result);
							latch.countDown();
						}
					});
		} catch (Throwable throwable) {
			throw new IOException(throwable);
		}
		try{
			latch.await();
		} catch (InterruptedException e) {
			throw new IOException(e);
		}
		return rowsImported.get();
	}
}
