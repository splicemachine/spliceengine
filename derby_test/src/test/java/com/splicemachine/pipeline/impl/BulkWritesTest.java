package com.splicemachine.pipeline.impl;

import java.io.IOException;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.pipeline.utils.PipelineUtils;
import com.splicemachine.si.impl.ActiveWriteTxn;

public class BulkWritesTest {
	@Test
	public void bulkWritesSerDeTest() throws IOException {
		BulkWrites bws1 = new BulkWrites();
		BulkWrites bws2 = new BulkWrites();
		BulkWrite bw1 = new BulkWrite(new ActiveWriteTxn(1l,1l),Bytes.toBytes("john leach"),"chicken");
		BulkWrite bw2 = new BulkWrite(new ActiveWriteTxn(1l,1l),Bytes.toBytes("john leach"),"chicken");
		bw1.addWrite(new KVPair(Bytes.toBytes("yo"),Bytes.toBytes("mtv")));
		bw2.addWrite(new KVPair(Bytes.toBytes("yo"),Bytes.toBytes("mtv")));
		bws1.addBulkWrite(bw1);
		bws2.addBulkWrite(bw2);		
		Assert.assertEquals(bws1,PipelineUtils.fromCompressedBytes(PipelineUtils.toCompressedBytes(bws2),BulkWrites.class));
	}
		
}
