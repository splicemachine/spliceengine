package com.splicemachine.derby.iapi.sql.execute;

import com.splicemachine.derby.error.SpliceStandardException;
import com.splicemachine.derby.hbase.SpliceOperationRegionScanner;
import com.splicemachine.hbase.BufferedRegionScanner;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.impl.sql.GenericStorablePreparedStatement;
import org.apache.hadoop.hbase.DoNotRetryIOException;
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
    private RegionScanner scanner;
    private LanguageConnectionContext lcc;
    private boolean isSink;
    private SpliceOperation topOperation;
    private SpliceOperationRegionScanner spliceRegionScanner;

    public SpliceOperationContext(HRegion region,
                                  Scan scan,
                                  Activation activation,
                                  GenericStorablePreparedStatement preparedStatement,
                                  LanguageConnectionContext lcc,boolean isSink,SpliceOperation topOperation){
        this.region= region;
        this.scan = scan;
        this.activation = activation;
        this.preparedStatement = preparedStatement;
        this.lcc = lcc;
        this.isSink = isSink;
        this.topOperation = topOperation;
    }

    public SpliceOperationContext(RegionScanner scanner,
                                  HRegion region,
                                  Scan scan,
                                  Activation activation,
                                  GenericStorablePreparedStatement preparedStatement,
                                  LanguageConnectionContext lcc,
                                  boolean isSink,SpliceOperation topOperation ){
        this.activation = activation;
        this.preparedStatement = preparedStatement;
        this.scanner = new BufferedRegionScanner(region,scanner, scan.getCaching());
        this.region=region;
        this.scan = scan;
        this.lcc = lcc;
        this.isSink=isSink;
        this.topOperation = topOperation;
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

    public RegionScanner getScanner() throws IOException {
        if(scanner==null){
            if(region==null)return null;

            Scan scan = new Scan(this.scan);
            scanner = region.getCoprocessorHost().preScannerOpen(scan);
            if (scanner == null) {
                scanner = region.getScanner(scan);
            }
            scanner = new BufferedRegionScanner(region,scanner,scan.getCaching());
        }
        return scanner;
    }

    public LanguageConnectionContext getLanguageConnectionContext() {
        if(activation!=null){
            lcc = activation.getLanguageConnectionContext();
         }
    	return lcc;
    }

    public void close(boolean commit) throws IOException {
        try{
        if(activation!=null) try {
            activation.close();
        } catch (StandardException e) {
            throw new DoNotRetryIOException(new SpliceStandardException(e).getTextMessage());
        }
        }finally{
                getLanguageConnectionContext().popStatementContext(getLanguageConnectionContext().getStatementContext(), null);
        }
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
                null,false,null);
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
}
