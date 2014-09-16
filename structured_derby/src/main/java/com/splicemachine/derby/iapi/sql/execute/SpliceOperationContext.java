package com.splicemachine.derby.iapi.sql.execute;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.hbase.SpliceOperationRegionScanner;
import com.splicemachine.derby.impl.store.access.SpliceTransaction;
import com.splicemachine.derby.impl.store.access.SpliceTransactionManager;
import com.splicemachine.hbase.BufferedRegionScanner;
import com.splicemachine.hbase.MeasuredRegionScanner;
import com.splicemachine.hbase.ReadAheadRegionScanner;
import com.splicemachine.si.api.TransactionalRegion;
import com.splicemachine.si.api.Txn;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.impl.sql.GenericStorablePreparedStatement;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
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
    private MeasuredRegionScanner scanner;
    private LanguageConnectionContext lcc;
    private boolean isSink;
    private SpliceOperation topOperation;
    private SpliceOperationRegionScanner spliceRegionScanner;
    private boolean cacheBlocks = true;
    private SpliceRuntimeContext spliceRuntimeContext;

		private Txn txn;
    private TransactionalRegion txnRegion;

    public SpliceOperationContext(HRegion region,
                                  TransactionalRegion txnRegion,
                                  Scan scan,
                                  Activation activation,
                                  GenericStorablePreparedStatement preparedStatement,
                                  LanguageConnectionContext lcc,boolean isSink,SpliceOperation topOperation,
                                  SpliceRuntimeContext spliceRuntimeContext,
																	Txn txn){
        this.region= region;
        this.scan = scan;
        this.activation = activation;
        this.preparedStatement = preparedStatement;
        this.lcc = lcc;
        this.isSink = isSink;
        this.topOperation = topOperation;
        this.spliceRuntimeContext = spliceRuntimeContext;
				this.txn = txn;
        this.txnRegion = txnRegion;
    }


		public SpliceOperationContext(RegionScanner scanner,
                                  HRegion region,
                                  TransactionalRegion txnRegion,
                                  Scan scan,
                                  Activation activation,
                                  GenericStorablePreparedStatement preparedStatement,
                                  LanguageConnectionContext lcc,
                                  boolean isSink,SpliceOperation topOperation,
                                  SpliceRuntimeContext spliceRuntimeContext,
																	Txn txn){
        this.activation = activation;
        this.preparedStatement = preparedStatement;

				if(SpliceConstants.useReadAheadScanner)
						this.scanner = new ReadAheadRegionScanner(region, scan.getCaching(), scanner,spliceRuntimeContext);
				else
						this.scanner = new BufferedRegionScanner(region, scanner, scan, scan.getCaching(),spliceRuntimeContext);

        this.region=region;
        this.txnRegion = txnRegion;
        this.scan = scan;
        this.lcc = lcc;
        this.isSink=isSink;
        this.topOperation = topOperation;
        this.spliceRuntimeContext = spliceRuntimeContext;
				this.txn = txn;
    }


    public void setSpliceRegionScanner(SpliceOperationRegionScanner sors){
        this.spliceRegionScanner = sors;
    }

    public HRegion getRegion(){
        return region;
    }

    public boolean isSink() {
        return isSink;
    }

    /**
     * Indicate whether passed operation is currently sinking rows
     */
    public boolean isOpSinking(SinkingOperation op){
        return isSink && topOperation == op;
    }

    public MeasuredRegionScanner getScanner() throws IOException {
        return getScanner(cacheBlocks);
    }

		public SpliceRuntimeContext getRuntimeContext() { return spliceRuntimeContext; }

		public MeasuredRegionScanner getScanner(boolean enableBlockCache) throws IOException{
        if(scanner==null){
            if(region==null)return null;

            Scan scan = new Scan(this.scan);
            scan.setCacheBlocks(enableBlockCache);
						RegionScanner baseScanner = region.getCoprocessorHost().preScannerOpen(scan);
            if (baseScanner == null) {
                baseScanner = region.getScanner(scan);
            }
            int caching = scan.getCaching();
            if(caching<0)
                caching=SpliceConstants.DEFAULT_CACHE_SIZE;

						if(SpliceConstants.useReadAheadScanner)
								scanner = new ReadAheadRegionScanner(region, caching, baseScanner,spliceRuntimeContext);
						else
								scanner = new BufferedRegionScanner(region, baseScanner, scan, caching, spliceRuntimeContext);
        }
        return scanner;
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
            if(scanner!=null)
                scanner.close();
        }
    }

    private void closeDerby() throws StandardException {
        try{
            if(activation!=null)
                activation.close();
        }finally{
//            LanguageConnectionContext languageConnectionContext = getLanguageConnectionContext();
//            if(languageConnectionContext.get)
//            languageConnectionContext.popStatementContext(languageConnectionContext.getStatementContext(), null);
        }
    }

    public GenericStorablePreparedStatement getPreparedStatement() {
        return preparedStatement;
    }

    public Activation getActivation() {
        return activation;
    }

		public Txn getTxn() { return txn; }

		public static SpliceOperationContext newContext(Activation a){
				return newContext(a,null);
		}

    public static SpliceOperationContext newContext(Activation a,Txn txn){
				if(txn==null){
						TransactionController te = a.getLanguageConnectionContext().getTransactionExecute();
						txn = ((SpliceTransaction)((SpliceTransactionManager) te).getRawTransaction()).getTxn();
				}
        return new SpliceOperationContext(null,null,null,
                a,
                (GenericStorablePreparedStatement)a.getPreparedStatement(),
                null,false,null, new SpliceRuntimeContext(),txn);
    }

    public SpliceOperation getTopOperation() {
        return topOperation;
    }

    public Scan getScan() {
        return scan;
    }

    public SpliceOperationRegionScanner getSpliceRegionScanner(){
        return spliceRegionScanner;
    }

    public void setCacheBlocks(boolean cacheBlocks) {
        this.cacheBlocks = cacheBlocks;
    }

    public TransactionalRegion getTransactionalRegion() {
        return txnRegion;
    }
}
