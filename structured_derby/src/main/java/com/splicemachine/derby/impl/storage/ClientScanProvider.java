package com.splicemachine.derby.impl.storage;

import com.splicemachine.derby.error.SpliceStandardLogUtils;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.utils.SpliceLogUtils;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * RowProvider which uses an HBase client ResultScanner to
 * pull rows in serially.
 *
 * @author Scott Fines
 * Date Created: 1/17/13:1:23 PM
 */
public class ClientScanProvider extends AbstractScanProvider {
    private static final Logger LOG = Logger.getLogger(ClientScanProvider.class);
    private final byte[] tableName;
    private HTableInterface htable;
    private final Scan scan;

    private ResultScanner scanner;

	public ClientScanProvider( byte[] tableName, Scan scan,ExecRow rowTemplate,FormatableBitSet fbt) {
		super(rowTemplate, fbt);
		SpliceLogUtils.trace(LOG, "instantiated");
		this.tableName = tableName;
		this.scan = scan;
	}

	@Override
	protected Result getResult() throws StandardException {
		try {
			return scanner.next();
		} catch (IOException e) {
			throw SpliceStandardLogUtils.logAndReturnStandardException(LOG, "getResult Error", e);
		}
	}

	@Override
	public void open() {
		SpliceLogUtils.trace(LOG, "open");
		if(htable==null)
			htable = SpliceAccessManager.getHTable(tableName);
		try {
			scanner = htable.getScanner(scan);
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
	public Scan toScan() {
		return scan;
	}

	@Override
	public byte[] getTableName() {
		return tableName;
	}
}
