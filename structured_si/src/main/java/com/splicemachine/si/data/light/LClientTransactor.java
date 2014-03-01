package com.splicemachine.si.data.light;

import com.splicemachine.si.api.TransactionManager;
import com.splicemachine.si.data.api.AbstractClientTransactor;
import com.splicemachine.si.data.api.SDataLib;
import com.splicemachine.si.impl.DataStore;
import com.splicemachine.si.impl.TransactionId;
import org.apache.hadoop.hbase.client.OperationWithAttributes;
import java.io.IOException;

/**
 * @author Scott Fines
 * Date: 2/13/14
 */
@SuppressWarnings("unchecked")
public class LClientTransactor<Put extends OperationWithAttributes,Get extends OperationWithAttributes,
Scan extends OperationWithAttributes,Mutation extends OperationWithAttributes> extends AbstractClientTransactor<Put,Get,Scan,Mutation> {

		public LClientTransactor(DataStore dataStore,
								TransactionManager control,
								SDataLib dataLib) {
			super(dataStore,control,dataLib);
		}

}
