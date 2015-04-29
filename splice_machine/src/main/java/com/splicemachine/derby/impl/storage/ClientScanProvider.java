package com.splicemachine.derby.impl.storage;

import com.splicemachine.async.AsyncHbase;
import com.splicemachine.async.AsyncScanner;
import com.splicemachine.async.QueueingAsyncScanner;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.derby.utils.marshall.PairDecoder;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
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
		private AsyncScanner scanner;
		private boolean opened= false;

		public ClientScanProvider(String type,
															byte[] tableName, Scan scan,
															PairDecoder decoder,
                              SpliceRuntimeContext spliceRuntimeContext) {
				super(decoder, type, spliceRuntimeContext);
				SpliceLogUtils.trace(LOG, "instantiated");
				this.tableName = tableName;
				this.scan = scan;
		}

		@Override
		public Result getResult() throws StandardException, IOException {
				if(!opened) {
						scanner.open();
						opened =true;
				}
				return scanner.next();
		}

		@Override
		public void open() {
				SpliceLogUtils.trace(LOG, "open");
				if(htable==null)
						htable = SpliceAccessManager.getHTable(tableName);
//				try {
//						scanner = new SequentialAsyncScanner(DerbyAsyncScannerUtils.convertScanner(scan,tableName,SimpleAsyncScanner.HBASE_CLIENT),spliceRuntimeContext);
//           scanner = new SimpleAsyncScanner(DerbyAsyncScannerUtils.convertScanner(scan,tableName,SimpleAsyncScanner.HBASE_CLIENT),spliceRuntimeContext);
						scanner = new QueueingAsyncScanner(DerbyAsyncScannerUtils.convertScanner(scan,tableName, AsyncHbase.HBASE_CLIENT),spliceRuntimeContext);
//						scanner = new MeasuredResultScanner(htable, scan, htable.getScanner(scan),spliceRuntimeContext);
//				} catch (IOException e) {
//						SpliceLogUtils.logAndThrowRuntime(LOG,"unable to open table "+ Bytes.toString(tableName),e);
//				}
		}

		@Override
		public void close() throws StandardException {
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
