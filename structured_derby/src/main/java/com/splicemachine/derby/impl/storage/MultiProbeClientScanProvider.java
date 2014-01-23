package com.splicemachine.derby.impl.storage;

import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.derby.utils.Exceptions;
import com.splicemachine.derby.utils.marshall.PairDecoder;
import com.splicemachine.utils.SpliceLogUtils;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.Activation;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;

/**
 *
 *
 *
 */
public class MultiProbeClientScanProvider extends AbstractMultiScanProvider {
    private static final Logger LOG = Logger.getLogger(MultiProbeClientScanProvider.class);
    private final byte[] tableName;
    private HTableInterface htable;
    private final List<Scan> scans;
    private ResultScanner scanner;

	public MultiProbeClientScanProvider(String type,byte[] tableName,
																			 List<Scan> scans,
																			 PairDecoder decoder,
																			 SpliceRuntimeContext spliceRuntimeContext) {
		super(decoder, type, spliceRuntimeContext);
		SpliceLogUtils.trace(LOG, "instantiated");
		this.tableName = tableName;
		this.scans = scans;
	}

	@Override
    public Result getResult() throws StandardException {
		try {
			return scanner.next();
		} catch (IOException e) {
            SpliceLogUtils.logAndThrow(LOG,"Unable to getResult",Exceptions.parseException(e));
            return null;//won't happen
		}
	}

	@Override
	public void open() {
		SpliceLogUtils.trace(LOG, "open");
		if(htable==null)
			htable = SpliceAccessManager.getHTable(tableName);
		try {
            scanner = ProbeDistributedScanner.create(htable, scans);
		} catch (IOException e) {
			SpliceLogUtils.logAndThrowRuntime(LOG,"unable to open table "+ Bytes.toString(tableName),e);
		}
	}

	@Override
	public void close() {
        super.close();
		SpliceLogUtils.trace(LOG, "closed after calling hasNext %d times",called);
		if(scanner!=null)scanner.close();
		if(htable!=null)
			try {
				htable.close();
			} catch (IOException e) {
				SpliceLogUtils.logAndThrowRuntime(LOG,"unable to close htable for "+ Bytes.toString(tableName),e);
			}
	}

	@Override
	public List<Scan> getScans() throws StandardException {
            return scans;
	}

	@Override
	public byte[] getTableName() {
		return tableName;
	}
	@Override
	public SpliceRuntimeContext getSpliceRuntimeContext() {
		return spliceRuntimeContext;
	}

}
