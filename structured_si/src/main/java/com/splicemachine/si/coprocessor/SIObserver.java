package com.splicemachine.si.coprocessor;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.log4j.Logger;

import com.splicemachine.utils.SpliceLogUtils;

public class SIObserver extends BaseRegionObserver {
	private static Logger LOG = Logger.getLogger(SIObserver.class);

	@Override
	public void start(CoprocessorEnvironment e) throws IOException {
		SpliceLogUtils.trace(LOG, "starting %s",SIObserver.class);
		super.start(e);
	}

	@Override
	public void stop(CoprocessorEnvironment e) throws IOException {
		SpliceLogUtils.trace(LOG, "stopping %s",SIObserver.class);
		super.stop(e);
	}

	@Override
	public void preGet(ObserverContext<RegionCoprocessorEnvironment> e, Get get, List<KeyValue> results) throws IOException {
		SpliceLogUtils.trace(LOG, "preGet %s", get);
		super.preGet(e, get, results);
	}

	@Override
	public void prePut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, boolean writeToWAL) throws IOException {
		SpliceLogUtils.trace(LOG, "prePut %s", put);
		// write-write conflict detection
		super.prePut(e, put, edit, writeToWAL);
	}

	@Override
	public void preDelete(ObserverContext<RegionCoprocessorEnvironment> e, Delete delete, WALEdit edit, boolean writeToWAL) throws IOException {
		SpliceLogUtils.trace(LOG, "preDelete %s", delete);
		// Write - Write Conflict Detection
		// tombstone ?
		super.preDelete(e, delete, edit, writeToWAL);
	}

}
