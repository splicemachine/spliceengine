package com.splicemachine.mrio.api;

import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import org.apache.hadoop.hbase.client.Scan;
import com.splicemachine.derby.impl.sql.execute.operations.scanner.SIFilterFactory;
import com.splicemachine.derby.impl.sql.execute.operations.scanner.SITableScanner;
import com.splicemachine.hbase.MeasuredRegionScanner;
import com.splicemachine.metrics.Metrics;
import com.splicemachine.si.api.TransactionalRegion;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.si.impl.SIFactoryImpl;

public class SMSITableScanner extends SITableScanner {

	public SMSITableScanner(MeasuredRegionScanner scanner,
			TransactionalRegion region, ExecRow template,
			Scan scan, int[] rowDecodingMap,
			TxnView txn, int[] keyColumnEncodingOrder,
			boolean[] keyColumnSortOrder, int[] keyColumnTypes,
			int[] keyDecodingMap, FormatableBitSet accessedPks,
			String indexName, String tableVersion, SIFilterFactory filterFactory) {
		super(SIFactoryImpl.dataLib, scanner, region, template, Metrics.noOpMetricFactory(), scan, rowDecodingMap,
				txn, keyColumnEncodingOrder, keyColumnSortOrder, keyColumnTypes,
				keyDecodingMap, accessedPks, indexName, tableVersion, filterFactory);
	}

}
