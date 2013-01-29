package com.splicemachine.derby.iapi.sql.execute;

import java.io.IOException;

import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.impl.sql.GenericStorablePreparedStatement;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionScanner;

import com.splicemachine.hbase.txn.coprocessor.region.TransactionalRegionObserver;
import com.splicemachine.hbase.txn.coprocessor.region.TxnUtils;

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
    private final LanguageConnectionContext lcc;
    private final GenericStorablePreparedStatement preparedStatement;
    private final HRegion region;
    private final Activation activation;
    private final Scan scan;
    private RegionScanner scanner;

    public SpliceOperationContext(HRegion region,
                                  Scan scan,
                                  Activation activation,
                                  GenericStorablePreparedStatement preparedStatement,
                                  LanguageConnectionContext lcc){
        this.lcc = lcc;
        this.region= region;
        this.scan = scan;
        this.activation = activation;
        this.preparedStatement = preparedStatement;
    }

    public SpliceOperationContext(RegionScanner scanner,
                                  HRegion region,
                                  Scan scan,
                                  Activation activation,
                                  GenericStorablePreparedStatement preparedStatement,
                                  LanguageConnectionContext lcc){
        this.lcc = lcc;
        this.activation = activation;
        this.preparedStatement = preparedStatement;
        this.scanner = scanner;
        this.region=region;
        this.scan = scan;
    }

    public HRegion getRegion(){
        return region;
    }

    public RegionScanner getScanner() throws IOException {
        if(scanner==null){
            if(region==null)return null;
            
            //Bug 193: manually trigger  transactional observer's preScannerOpen to handle transaction cases. 
            //I am not using the TransactionalRegionObserver class implicitly since this is the highest priority
            //coprocessor in our transactional system thus its preScannerOpen is called by default - jz
            if (TxnUtils.getTransactionID(scan) != null) {
            	scanner = region.getCoprocessorHost().preScannerOpen(scan);
							if(scanner==null){
								/*
								 * the Coprocessor host may bypass calling preScannerOpen, in which case we need
								 * to open the scanner ourselves and run it through postScannerOpen to make sure it gets
								 * pushed through the coprocessor framework correctly
								 */
								scanner = region.getScanner(scan);
//								scanner = region.getCoprocessorHost().postScannerOpen(scan,scanner);
							}
						}else
            	scanner = region.getScanner(scan);
        }
        return scanner;
    }

    public LanguageConnectionContext getLanguageConnectionContext() {
        return lcc;
    }

    public GenericStorablePreparedStatement getPreparedStatement() {
        return preparedStatement;
    }

    public Activation getActivation() {
        return activation;
    }

    public static SpliceOperationContext newContext(Activation a){
        return new SpliceOperationContext(null,null,
                a,
                (GenericStorablePreparedStatement)a.getPreparedStatement(),
                a.getLanguageConnectionContext());
    }

}
