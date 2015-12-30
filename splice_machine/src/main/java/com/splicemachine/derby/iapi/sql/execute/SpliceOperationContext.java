package com.splicemachine.derby.iapi.sql.execute;

import com.splicemachine.derby.impl.store.access.SpliceTransactionManager;
import com.splicemachine.si.api.TransactionalRegion;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.impl.sql.GenericStorablePreparedStatement;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.log4j.Logger;
import java.io.IOException;


/**
 * Represents the context of a SpliceOperation stack.
 *
 * This is primarily intended to ease the initialization interface by providing a single
 * wrapper object, instead of 400 different individual elements.
 *
 * @author Scott Fines
 * Created: 1/18/13 9:18 AM
 */
public class SpliceOperationContext {
    static final Logger LOG = Logger.getLogger(SpliceOperationContext.class);
    private final GenericStorablePreparedStatement preparedStatement;
    private final HRegion region;
    private final Activation activation;
    private final Scan scan;
    private LanguageConnectionContext lcc;
	private TxnView txn;
    private TransactionalRegion txnRegion;

    public SpliceOperationContext(HRegion region,
                                  TransactionalRegion txnRegion,
                                  Scan scan,
                                  Activation activation,
                                  GenericStorablePreparedStatement preparedStatement,
                                  LanguageConnectionContext lcc,
								  TxnView txn){
        this.region= region;
        this.scan = scan;
        this.activation = activation;
        this.preparedStatement = preparedStatement;
        this.lcc = lcc;
	    this.txn = txn;
        this.txnRegion = txnRegion;
    }

    public HRegion getRegion(){
        return region;
    }

    public LanguageConnectionContext getLanguageConnectionContext() {
        if(activation!=null){
            lcc = activation.getLanguageConnectionContext();
         }
    	return lcc;
    }

    public void close() throws IOException, StandardException {
        try{
            closeDerby();
        }finally{
        }
    }

    private void closeDerby() throws StandardException {
        if(activation!=null)
            activation.close();
    }

    public GenericStorablePreparedStatement getPreparedStatement() {
        return preparedStatement;
    }

    public Activation getActivation() {
        return activation;
    }

		public TxnView getTxn() { return txn; }

		public static SpliceOperationContext newContext(Activation a){
				return newContext(a,null);
		}

    public static SpliceOperationContext newContext(Activation a,TxnView txn){
				if(txn==null){
						TransactionController te = a.getLanguageConnectionContext().getTransactionExecute();
						txn = ((SpliceTransactionManager) te).getRawTransaction().getActiveStateTxn();
				}
        return new SpliceOperationContext(null,null,null,
                a,
                (GenericStorablePreparedStatement)a.getPreparedStatement(),
                null,txn);
    }

    public Scan getScan() {
        return scan;
    }


    public TransactionalRegion getTransactionalRegion() {
        return txnRegion;
    }
}
