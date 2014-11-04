package com.splicemachine.hbase.backup;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.TableNotEnabledException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.catalog.CatalogTracker;
import org.apache.hadoop.hbase.master.AssignmentManager;
import org.apache.hadoop.hbase.master.handler.DisableTableHandler;
import org.apache.log4j.Logger;

public class SpliceDisableTableHandler extends DisableTableHandler {
	private static final Logger LOG = Logger.getLogger(SpliceDisableTableHandler.class);
	protected CountDownLatch latch;
    public SpliceDisableTableHandler(Server server, byte[] tableName,
			CatalogTracker catalogTracker, AssignmentManager assignmentManager,
			boolean skipTableStateCheck, CountDownLatch latch) throws TableNotFoundException,
			TableNotEnabledException, IOException {
		super(server, tableName, catalogTracker, assignmentManager, skipTableStateCheck);
		this.latch = latch;
	}
	@Override
	public void process() {
		super.process();
		latch.countDown();
	}
    

}
