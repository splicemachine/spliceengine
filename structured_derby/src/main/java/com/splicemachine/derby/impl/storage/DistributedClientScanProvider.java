package com.splicemachine.derby.impl.storage;

import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.impl.sql.execute.operations.DistributedScanner;
import com.splicemachine.derby.impl.sql.execute.operations.RowKeyDistributorByHashPrefix;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.derby.utils.Exceptions;
import com.splicemachine.derby.utils.marshall.BucketHasher;
import com.splicemachine.derby.utils.marshall.PairDecoder;
import com.splicemachine.derby.utils.marshall.RowDecoder;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.derby.iapi.error.StandardException;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * RowProvider which uses an HBase client ResultScanner to
 * pull rows in serially.
 *
 * @author Scott Fines
 * Date Created: 1/17/13:1:23 PM
 */
public class DistributedClientScanProvider extends AbstractMultiScanProvider {
    private static final Logger LOG = Logger.getLogger(DistributedClientScanProvider.class);
    private final byte[] tableName;
    private HTableInterface htable;
    private final Scan scan;
    private final RowKeyDistributorByHashPrefix keyDistributor;

    private ResultScanner scanner;


	public DistributedClientScanProvider(String type,
																			 byte[] tableName,
																			 Scan scan,
																			 PairDecoder decoder,
																			 SpliceRuntimeContext spliceRuntimeContext) {
		super(decoder, type, spliceRuntimeContext);
		SpliceLogUtils.trace(LOG, "instantiated");
		this.tableName = tableName;
		this.scan = scan;
		this.keyDistributor = new RowKeyDistributorByHashPrefix(BucketHasher.getHasher(SpliceDriver.driver().getTempTable().getCurrentSpread()));
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
            scanner = DistributedScanner.create(htable, scan, keyDistributor);
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
	    try {
            return Arrays.asList(keyDistributor.getDistributedScans(scan));
        } catch (IOException e) {
            throw Exceptions.parseException(e);
        }
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
