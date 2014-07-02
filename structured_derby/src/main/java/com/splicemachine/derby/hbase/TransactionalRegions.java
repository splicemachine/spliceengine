package com.splicemachine.derby.hbase;

import com.splicemachine.si.api.HTransactorFactory;
import com.splicemachine.si.api.TransactionalRegion;
import com.splicemachine.si.api.TxnRegion;
import org.apache.hadoop.hbase.regionserver.HRegion;

/**
 * @author Scott Fines
 * Date: 7/2/14
 */
public class TransactionalRegions {
		private TransactionalRegions(){} //can't make a utility class

		public static TransactionalRegion get(HRegion region){
				return new TxnRegion(region,
								HTransactorFactory.getRollForward(region),
								HTransactorFactory.getReadResolver(region),
								HTransactorFactory.getTxnSupplier(),
								HTransactorFactory.getDataStore(),
								HTransactorFactory.getDataLib(),
								HTransactorFactory.getTransactor());
		}
}
