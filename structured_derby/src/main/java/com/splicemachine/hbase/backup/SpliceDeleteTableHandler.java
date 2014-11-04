package com.splicemachine.hbase.backup;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.handler.DeleteTableHandler;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import com.splicemachine.utils.SpliceLogUtils;

public class SpliceDeleteTableHandler extends DeleteTableHandler {
	private static final Logger LOG = Logger.getLogger(SpliceDeleteTableHandler.class);
	protected CountDownLatch latch;
	public SpliceDeleteTableHandler(byte[] tableName, Server server,
			MasterServices masterServices, CountDownLatch latch) throws IOException {
		super(tableName, server, masterServices);
		this.latch = latch;
	}

    @Override
	public void process() {
		super.process();
		latch.countDown();
	}
}
